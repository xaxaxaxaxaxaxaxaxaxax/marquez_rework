/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.OpenLineageDao.DEFAULT_NAMESPACE_OWNER;

import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.Field;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.mappers.ExtendedRunRowMapper;
import marquez.db.mappers.JobRowMapper;
import marquez.db.mappers.RunMapper;
import marquez.db.mappers.RunRowMapper;
import marquez.db.models.DatasetRow;
import marquez.db.models.ExtendedRunRow;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunRow;
import marquez.service.models.Dataset;
import marquez.service.models.JobMeta;
import marquez.service.models.LineageEvent.SchemaField;
import marquez.service.models.Run;
import marquez.service.models.RunMeta;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterRowMapper(ExtendedRunRowMapper.class)
@RegisterRowMapper(RunRowMapper.class)
@RegisterRowMapper(RunMapper.class)
@RegisterRowMapper(JobRowMapper.class)
public interface RunDao extends BaseDao {
  int RUN_INPUT_MAPPING_CHUNK_SIZE = 1000;

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM runs WHERE uuid = :rowUuid)")
  boolean exists(UUID rowUuid);

  @SqlUpdate(
      "UPDATE runs "
          + "SET updated_at = :transitionedAt, "
          + "    current_run_state = :currentRunState, "
          + "    transitioned_at = :transitionedAt "
          + "WHERE uuid = :rowUuid")
  void updateRunState(UUID rowUuid, Instant transitionedAt, RunState currentRunState);

  @SqlUpdate(
      "UPDATE runs "
          + "SET updated_at = :transitionedAt, "
          + "    start_run_state_uuid = :startRunStateUuid,"
          + "    started_at = :transitionedAt "
          + "WHERE uuid = :rowUuid")
  void updateStartState(UUID rowUuid, Instant transitionedAt, UUID startRunStateUuid);

  @SqlUpdate(
      "UPDATE runs "
          + "SET updated_at = :transitionedAt, "
          + "    end_run_state_uuid = :endRunStateUuid, "
          + "    ended_at = :transitionedAt "
          + "WHERE uuid = :rowUuid")
  void updateEndState(UUID rowUuid, Instant transitionedAt, UUID endRunStateUuid);

  String FIND_RUN_SELECT_SQL =
      """
      SELECT r.*, ra.args, f.facets,
      jv.version AS job_version,
      ri.input_versions, ro.output_versions, df.dataset_facets
      """;

  String FIND_RUN_CORE_ENRICHMENT_SQL =
      """
      LEFT OUTER JOIN run_args AS ra ON ra.uuid = r.run_args_uuid
      LEFT OUTER JOIN LATERAL (
          SELECT JSON_AGG(json_build_object('namespace', dv.namespace_name,
              'name', dv.dataset_name,
              'version', dv.version,
              'dataset_version_uuid', dv.uuid)) AS input_versions
          FROM runs_input_mapping im
          INNER JOIN dataset_versions dv on im.dataset_version_uuid = dv.uuid
          WHERE im.run_uuid = r.uuid
      ) ri ON TRUE
      LEFT OUTER JOIN LATERAL (
          SELECT JSON_AGG(json_build_object('namespace', dv.namespace_name,
              'name', dv.dataset_name,
              'version', dv.version,
              'dataset_version_uuid', dv.uuid
              )) AS output_versions
          FROM dataset_versions dv
          WHERE dv.run_uuid = r.uuid
      ) ro ON TRUE
      """;

  String FIND_RUN_ENRICHMENT_SQL =
      """
      LEFT OUTER JOIN LATERAL (
          SELECT JSON_AGG(rf.facet ORDER BY rf.lineage_event_time ASC) AS facets
          FROM run_facets_view rf
          WHERE rf.run_uuid = r.uuid
      ) AS f ON TRUE
      """
          + FIND_RUN_CORE_ENRICHMENT_SQL
          + """
      LEFT OUTER JOIN job_versions jv ON jv.uuid=r.job_version_uuid
      LEFT OUTER JOIN LATERAL (
          SELECT JSON_AGG(json_build_object(
                  'dataset_version_uuid', df.dataset_version_uuid,
                  'name', df.name,
                  'type', df.type,
                  'facet', df.facet
              ) ORDER BY df.created_at ASC) as dataset_facets
          FROM dataset_facets_view df
          WHERE df.run_uuid = r.uuid
            AND (df.type ILIKE 'output' OR df.type ILIKE 'input')
      ) AS df ON TRUE
      """;

  String BASE_FIND_RUN_SQL =
      FIND_RUN_SELECT_SQL + " FROM runs_view AS r\n" + FIND_RUN_ENRICHMENT_SQL;

  String BASE_FIND_EXTENDED_RUN_SQL =
      """
      SELECT r.*, ra.args, ri.input_versions, ro.output_versions
      FROM runs_view AS r
      """
          + FIND_RUN_CORE_ENRICHMENT_SQL;

  @SqlQuery(BASE_FIND_RUN_SQL + "WHERE r.uuid = :runUuid")
  Optional<Run> findRunByUuid(UUID runUuid);

  @SqlQuery(BASE_FIND_EXTENDED_RUN_SQL + "WHERE r.uuid = :runUuid")
  Optional<ExtendedRunRow> findRunByUuidAsExtendedRow(UUID runUuid);

  @SqlQuery(
      BASE_FIND_RUN_SQL + "WHERE r.uuid = ANY(CAST(:runUuids AS uuid[])) " + "ORDER BY r.uuid")
  List<Run> findRunsByUuidsQuery(@Bind("runUuids") UUID[] runUuids);

  default List<Run> findRunsByUuids(Collection<UUID> runUuids) {
    return runUuids.isEmpty() ? List.of() : findRunsByUuidsQuery(runUuids.toArray(UUID[]::new));
  }

  @SqlQuery("SELECT * FROM runs r WHERE r.uuid = :runUuid")
  Optional<RunRow> findRunByUuidAsRow(UUID runUuid);

  @SqlQuery(
      """
  SELECT j.* FROM jobs_view j
  INNER JOIN runs_view r  ON r.job_uuid=j.uuid
  WHERE r.uuid=:uuid
""")
  Optional<JobRow> findJobRowByRunUuid(UUID uuid);

  @SqlQuery(
      """
          WITH filtered_jobs AS (
            SELECT
                jv.uuid,
                jv.namespace_name,
                jv.name
            FROM jobs_view jv
            WHERE jv.namespace_name=:namespace AND (jv.name=:jobName OR :jobName = ANY(jv.aliases))
          ),
          selected_runs AS MATERIALIZED (
              SELECT r.*
              FROM runs_view r
              INNER JOIN filtered_jobs fj ON r.job_uuid = fj.uuid
              ORDER BY r.started_at DESC NULLS LAST, r.uuid DESC
              LIMIT :limit OFFSET :offset
          )
      """
          + FIND_RUN_SELECT_SQL
          + " FROM selected_runs AS r\n"
          + FIND_RUN_ENRICHMENT_SQL
          + " ORDER BY r.started_at DESC NULLS LAST, r.uuid DESC")
  List<Run> findAll(String namespace, String jobName, int limit, int offset);

  @SqlQuery(
      "INSERT INTO runs ( "
          + "uuid, "
          + "parent_run_uuid, "
          + "external_id, "
          + "created_at, "
          + "updated_at, "
          + "job_uuid, "
          + "job_version_uuid, "
          + "run_args_uuid, "
          + "nominal_start_time, "
          + "nominal_end_time,"
          + "current_run_state, "
          + "transitioned_at, "
          + "namespace_name, "
          + "job_name, "
          + "location "
          + ") VALUES ( "
          + ":runUuid, "
          + ":parentRunUuid, "
          + ":externalId, "
          + ":now, "
          + ":now, "
          + ":jobUuid,"
          + ":jobVersionUuid, "
          + ":runArgsUuid, "
          + ":nominalStartTime, "
          + ":nominalEndTime, "
          + ":runStateType,"
          + ":runStateTime, "
          + ":namespaceName, "
          + ":jobName, "
          + ":location "
          + ") ON CONFLICT(uuid) DO "
          + "UPDATE SET "
          + "external_id = EXCLUDED.external_id, "
          + "updated_at = EXCLUDED.updated_at, "
          + "current_run_state = EXCLUDED.current_run_state, "
          + "transitioned_at = EXCLUDED.transitioned_at, "
          + "nominal_start_time = COALESCE(EXCLUDED.nominal_start_time, runs.nominal_start_time), "
          + "nominal_end_time = COALESCE(EXCLUDED.nominal_end_time, runs.nominal_end_time), "
          + "location = EXCLUDED.location "
          + "RETURNING *")
  RunRow upsert(
      UUID runUuid,
      UUID parentRunUuid,
      String externalId,
      Instant now,
      UUID jobUuid,
      UUID jobVersionUuid,
      UUID runArgsUuid,
      Instant nominalStartTime,
      Instant nominalEndTime,
      RunState runStateType,
      Instant runStateTime,
      String namespaceName,
      String jobName,
      String location);

  @SqlQuery(
      "INSERT INTO runs ( "
          + "uuid, "
          + "parent_run_uuid, "
          + "external_id, "
          + "created_at, "
          + "updated_at, "
          + "job_uuid, "
          + "job_version_uuid, "
          + "run_args_uuid, "
          + "nominal_start_time, "
          + "nominal_end_time, "
          + "namespace_name, "
          + "job_name, "
          + "location "
          + ") VALUES ( "
          + ":runUuid, "
          + ":parentRunUuid, "
          + ":externalId, "
          + ":now, "
          + ":now, "
          + ":jobUuid, "
          + ":jobVersionUuid, "
          + ":runArgsUuid, "
          + ":nominalStartTime, "
          + ":nominalEndTime, "
          + ":namespaceName, "
          + ":jobName, "
          + ":location "
          + ") ON CONFLICT(uuid) DO "
          + "UPDATE SET "
          + "external_id = EXCLUDED.external_id, "
          + "updated_at = EXCLUDED.updated_at, "
          + "nominal_start_time = COALESCE(EXCLUDED.nominal_start_time, runs.nominal_start_time), "
          + "nominal_end_time = COALESCE(EXCLUDED.nominal_end_time, runs.nominal_end_time), "
          + "location = EXCLUDED.location "
          + "RETURNING *")
  RunRow upsert(
      UUID runUuid,
      UUID parentRunUuid,
      String externalId,
      Instant now,
      UUID jobUuid,
      UUID jobVersionUuid,
      UUID runArgsUuid,
      Instant nominalStartTime,
      Instant nominalEndTime,
      String namespaceName,
      String jobName,
      String location);

  default RunRow upsert(RunUpsert runUpsert) {
    if (runUpsert.runStateType == null) {
      return upsert(
          runUpsert.runUuid(),
          runUpsert.parentRunUuid(),
          runUpsert.externalId(),
          runUpsert.now(),
          runUpsert.jobUuid(),
          runUpsert.jobVersionUuid(),
          runUpsert.runArgsUuid(),
          runUpsert.nominalStartTime(),
          runUpsert.nominalEndTime(),
          runUpsert.namespaceName(),
          runUpsert.jobName(),
          runUpsert.location());
    } else {
      return upsert(
          runUpsert.runUuid(),
          runUpsert.parentRunUuid(),
          runUpsert.externalId(),
          runUpsert.now(),
          runUpsert.jobUuid(),
          runUpsert.jobVersionUuid(),
          runUpsert.runArgsUuid(),
          runUpsert.nominalStartTime(),
          runUpsert.nominalEndTime(),
          runUpsert.runStateType(),
          runUpsert.runStateTime(),
          runUpsert.namespaceName(),
          runUpsert.jobName(),
          runUpsert.location());
    }
  }

  @SqlUpdate(
      """
      INSERT INTO runs_input_mapping (run_uuid, dataset_version_uuid)
      SELECT :runUuid, mappings.dataset_version_uuid
      FROM unnest(CAST(:datasetVersionUuids AS uuid[])) mappings(dataset_version_uuid)
      ON CONFLICT (run_uuid, dataset_version_uuid) DO NOTHING
      """)
  void insertInputMappingsChunk(
      UUID runUuid, @Bind("datasetVersionUuids") UUID[] datasetVersionUuids);

  /**
   * Associates input dataset versions with a run using bounded, set-based inserts. All chunks are
   * committed atomically, including when this method is called outside a surrounding transaction.
   */
  @Transaction
  default void updateInputMappings(UUID runUuid, Iterable<UUID> datasetVersionUuids) {
    Objects.requireNonNull(runUuid, "runUuid");
    Iterator<UUID> iterator =
        Objects.requireNonNull(datasetVersionUuids, "datasetVersionUuids").iterator();
    if (!iterator.hasNext()) {
      return;
    }

    Set<UUID> seenDatasetVersionUuids = new HashSet<>();
    List<UUID> chunk = new ArrayList<>(RUN_INPUT_MAPPING_CHUNK_SIZE);

    while (iterator.hasNext()) {
      UUID datasetVersionUuid =
          Objects.requireNonNull(iterator.next(), "datasetVersionUuids contains null");
      if (seenDatasetVersionUuids.add(datasetVersionUuid)) {
        chunk.add(datasetVersionUuid);
      }
      if (chunk.size() == RUN_INPUT_MAPPING_CHUNK_SIZE) {
        insertInputMappingsChunk(runUuid, chunk.toArray(UUID[]::new));
        chunk.clear();
      }
    }

    if (!chunk.isEmpty()) {
      insertInputMappingsChunk(runUuid, chunk.toArray(UUID[]::new));
    }
  }

  /** Used to associate one input dataset version with a run. */
  default void updateInputMapping(UUID runUuid, UUID datasetVersionUuid) {
    updateInputMappings(runUuid, List.of(datasetVersionUuid));
  }

  @Transaction
  default void notifyJobChange(UUID runUuid, JobRow jobRow, JobMeta jobMeta) {
    upsertRun(runUuid, jobRow.getName(), jobRow.getNamespaceName());

    updateInputDatasetMapping(jobMeta.getInputs(), runUuid);

    upsertOutputDatasetsFor(runUuid, jobMeta.getOutputs());
  }

  default void upsertOutputDatasetsFor(UUID runUuid, ImmutableSet<DatasetId> runOutputIds) {
    DatasetVersionDao datasetVersionDao = createDatasetVersionDao();
    DatasetDao datasetDao = createDatasetDao();
    OpenLineageDao openLineageDao = createOpenLineageDao();

    if (runOutputIds != null) {
      for (DatasetId runOutputId : runOutputIds) {
        Optional<DatasetRow> dsRow =
            datasetDao.findDatasetAsRow(
                runOutputId.getNamespace().getValue(), runOutputId.getName().getValue());
        Optional<Dataset> ds =
            datasetDao.findDatasetByName(
                runOutputId.getNamespace().getValue(), runOutputId.getName().getValue());
        ds.ifPresent(
            d -> {
              UUID version =
                  Utils.newDatasetVersionFor(
                          d.getNamespace().getValue(),
                          d.getSourceName().getValue(),
                          d.getPhysicalName().getValue(),
                          d.getName().getValue(),
                          null,
                          toSchemaFields(d.getFields()),
                          runUuid)
                      .getValue();
              datasetVersionDao.upsert(
                  UUID.randomUUID(),
                  Instant.now(),
                  dsRow.get().getUuid(),
                  version,
                  // this path does not upsert dataset_fields, therefore no schema version created
                  null,
                  runUuid,
                  datasetVersionDao.toPgObjectFields(d.getFields()),
                  d.getNamespace().getValue(),
                  d.getName().getValue(),
                  null);
            });
      }
    }
  }

  default List<SchemaField> toSchemaFields(List<Field> fields) {
    if (fields == null) {
      return null;
    }
    return fields.stream()
        .map(
            f ->
                SchemaField.builder()
                    .name(f.getName().getValue())
                    .type(f.getType())
                    .description(f.getDescription().orElse(null))
                    .build())
        .collect(Collectors.toList());
  }

  default void updateInputDatasetMapping(Set<DatasetId> inputs, UUID runUuid) {
    if (inputs == null) {
      return;
    }
    DatasetDao datasetDao = createDatasetDao();
    List<UUID> datasetVersionUuids = new ArrayList<>(inputs.size());

    for (DatasetId datasetId : inputs) {
      Optional<Dataset> dataset =
          datasetDao.findDatasetByName(
              datasetId.getNamespace().getValue(), datasetId.getName().getValue());
      if (dataset.isPresent() && dataset.get().getCurrentVersion().isPresent()) {
        datasetVersionUuids.add(dataset.get().getCurrentVersion().get());
      }
    }
    updateInputMappings(runUuid, datasetVersionUuids);
  }

  @SqlUpdate(
      "UPDATE runs SET job_name = :jobName, "
          + "namespace_name = :namespaceName "
          + "WHERE uuid = :runUuid")
  void upsertRun(UUID runUuid, @NonNull String jobName, @NonNull String namespaceName);

  /** Insert from run creates a run but does not associate any datasets. */
  @Transaction
  default RunRow upsertRunMeta(
      NamespaceName namespaceName, JobRow jobRow, RunMeta runMeta, RunState currentState) {
    Instant now = Instant.now();

    NamespaceRow namespaceRow =
        createNamespaceDao()
            .upsertNamespaceRow(
                UUID.randomUUID(), now, namespaceName.getValue(), DEFAULT_NAMESPACE_OWNER);

    RunArgsRow runArgsRow =
        createRunArgsDao()
            .upsertRunArgs(
                UUID.randomUUID(),
                now,
                Utils.toJson(runMeta.getArgs()),
                Utils.checksumFor(runMeta.getArgs()));

    UUID uuid = runMeta.getId().map(RunId::getValue).orElse(UUID.randomUUID());

    RunRow runRow =
        upsert(
            uuid,
            null,
            null,
            now,
            jobRow.getUuid(),
            null,
            runArgsRow.getUuid(),
            runMeta.getNominalStartTime().orElse(null),
            runMeta.getNominalEndTime().orElse(null),
            currentState,
            now,
            namespaceRow.getName(),
            jobRow.getName(),
            jobRow.getLocation());

    updateInputDatasetMapping(jobRow.getInputs(), uuid);

    createRunStateDao().updateRunStateFor(uuid, currentState, now);

    return runRow;
  }

  @SqlUpdate(
      "UPDATE runs SET job_version_uuid = :jobVersionUuid "
          + "WHERE uuid = :runUuid AND job_version_uuid IS DISTINCT FROM :jobVersionUuid")
  void updateJobVersion(UUID runUuid, UUID jobVersionUuid);

  @SqlQuery(
      """
      WITH selected_runs AS MATERIALIZED (
          SELECT r.*
          FROM runs_view r
          WHERE r.uuid IN (
            SELECT j.current_run_uuid FROM jobs_view j
            WHERE j.namespace_name=:namespace
              AND (j.name=:jobName OR :jobName = ANY(j.aliases))
          )
          ORDER BY r.transitioned_at DESC, r.started_at DESC, r.uuid DESC
          LIMIT :limit OFFSET :offset
      )
      """
          + FIND_RUN_SELECT_SQL
          + " FROM selected_runs AS r\n"
          + FIND_RUN_ENRICHMENT_SQL
          + " ORDER BY r.transitioned_at DESC, r.started_at DESC, r.uuid DESC")
  List<Run> findCurrentRunByJob(String namespace, String jobName, int limit, int offset);

  @SqlQuery(
      """
      WITH filtered_jobs AS MATERIALIZED (
          SELECT jv.uuid
          FROM jobs_view jv
          WHERE jv.namespace_name=:namespace
            AND (jv.name=:jobName OR :jobName = ANY(jv.aliases))
      ),
      selected_runs AS MATERIALIZED (
          SELECT r.*
          FROM runs_view r
          WHERE r.job_uuid IN (
            SELECT j.uuid FROM jobs j
            WHERE j.uuid IN (SELECT uuid FROM filtered_jobs)
               OR j.symlink_target_uuid IN (SELECT uuid FROM filtered_jobs)
          )
          ORDER BY r.transitioned_at DESC, r.started_at DESC, r.uuid DESC
          LIMIT :limit OFFSET :offset
      )
      """
          + FIND_RUN_SELECT_SQL
          + " FROM selected_runs AS r\n"
          + FIND_RUN_ENRICHMENT_SQL
          + " ORDER BY r.transitioned_at DESC, r.started_at DESC, r.uuid DESC")
  List<Run> findByLatestJob(String namespace, String jobName, int limit, int offset);

  @Builder
  record RunUpsert(
      UUID runUuid,
      UUID parentRunUuid,
      String externalId,
      Instant now,
      UUID jobUuid,
      UUID jobVersionUuid,
      UUID runArgsUuid,
      Instant nominalStartTime,
      Instant nominalEndTime,
      RunState runStateType,
      Instant runStateTime,
      String namespaceName,
      String jobName,
      String location) {}
}
