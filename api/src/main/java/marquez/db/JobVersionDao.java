/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.Columns.stringOrThrow;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.Value;
import marquez.api.models.JobVersion;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunState;
import marquez.common.models.Version;
import marquez.db.mappers.ExtendedJobVersionRowMapper;
import marquez.db.mappers.JobDataMapper;
import marquez.db.mappers.JobVersionMapper;
import marquez.db.mappers.RunIoRowMapper;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.ExtendedJobVersionRow;
import marquez.db.models.JobRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunIoRow;
import marquez.db.models.RunIoSnapshot;
import marquez.db.models.UpdateLineageRow.DatasetRecord;
import marquez.service.models.Run;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

/** The DAO for {@code JobVersion}. */
@RegisterRowMapper(ExtendedJobVersionRowMapper.class)
@RegisterRowMapper(JobVersionMapper.class)
@RegisterRowMapper(JobDataMapper.class)
@RegisterRowMapper(RunIoRowMapper.class)
public interface JobVersionDao extends BaseDao {
  int JOB_VERSION_IO_BATCH_SIZE = 1000;

  /** An {@code enum} used to determine the input / output dataset type for a given job version. */
  enum IoType {
    INPUT,
    OUTPUT
  }

  /**
   * Returns JobVersion fields, along with Run-related fields, prefixed with "run_". Input and
   * Output datasets are constructed as JSON strings that can be deserialized into DatasetIds.
   */
  String BASE_SELECT_ON_JOB_VERSIONS =
      """
      WITH job_version_io AS (
          SELECT io.job_version_uuid,
                 JSON_AGG(json_build_object('namespace', ds.namespace_name,
                                            'name', ds.name))
                 FILTER (WHERE io.io_type = 'INPUT') AS input_datasets,
                 JSON_AGG(json_build_object('namespace', ds.namespace_name,
                                            'name', ds.name))
                 FILTER (WHERE io.io_type = 'OUTPUT') AS output_datasets
          FROM job_versions_io_mapping io
          INNER JOIN job_versions jv ON jv.uuid = io.job_version_uuid
          INNER JOIN datasets_view ds ON ds.uuid = io.dataset_uuid
          INNER JOIN jobs_view j ON j.uuid=jv.job_uuid
          WHERE j.namespace_name = :namespaceName
            AND j.name = :jobName
          GROUP BY io.job_version_uuid
      ), relevant_job_versions AS (
          SELECT jv.uuid, jv.created_at, jv.updated_at, jv.job_uuid, jv.version,\s
          jv.location, jv.latest_run_uuid, j.namespace_uuid,\s
          j.namespace_name, j.name AS job_name
          FROM job_versions jv
          INNER JOIN jobs_view j ON j.uuid=jv.job_uuid
          WHERE j.name = :jobName AND j.namespace_name=:namespaceName
          ORDER BY jv.created_at DESC
      )
      SELECT jv.*,
             dsio.input_datasets,
             dsio.output_datasets,
             r.uuid               AS run_uuid,
             r.created_at         AS run_created_at,
             r.updated_at         AS run_updated_at,
             r.nominal_start_time AS run_nominal_start_time,
             r.nominal_end_time   AS run_nominal_end_time,
             r.current_run_state  AS run_current_run_state,
             r.started_at         AS run_started_at,
             r.ended_at           AS run_ended_at,
             r.namespace_name     AS run_namespace_name,
             r.job_name           AS run_job_name,
             jv.version           AS run_job_version,
             r.location           AS run_location,
             ra.args              AS run_args,
             f.facets             AS run_facets,
             ri.input_versions    AS run_input_versions,
             ro.output_versions   AS run_output_versions
      FROM relevant_job_versions AS jv
      LEFT JOIN job_version_io dsio ON dsio.job_version_uuid = jv.uuid
      LEFT OUTER JOIN runs r ON r.uuid = jv.latest_run_uuid
      LEFT JOIN LATERAL (
          SELECT jf.run_uuid, JSON_AGG(jf.facet ORDER BY jf.lineage_event_time ASC) AS facets
          FROM job_facets_view AS jf
          WHERE jf.run_uuid=jv.latest_run_uuid AND jf.job_uuid = jv.job_uuid
          GROUP BY jf.run_uuid
      ) AS f ON r.uuid = f.run_uuid
      LEFT OUTER JOIN run_args AS ra ON ra.uuid = r.run_args_uuid
      LEFT JOIN LATERAL (
          SELECT im.run_uuid,
                 JSON_AGG(json_build_object('namespace', dv.namespace_name,
                                            'name', dv.dataset_name,
                                            'version', dv.version)) AS input_versions
          FROM runs_input_mapping im
          INNER JOIN dataset_versions dv on im.dataset_version_uuid = dv.uuid
          WHERE im.run_uuid=jv.latest_run_uuid
          GROUP BY im.run_uuid
      ) ri ON ri.run_uuid = r.uuid
      LEFT OUTER JOIN (
          SELECT run_uuid,
                 JSON_AGG(json_build_object('namespace', namespace_name,
                                            'name', dataset_name,
                                            'version', version)) AS output_versions
          FROM dataset_versions
          GROUP BY run_uuid
      ) ro ON ro.run_uuid = r.uuid
      """;

  @SqlQuery(BASE_SELECT_ON_JOB_VERSIONS + "WHERE jv.version = :jobVersionUuid")
  Optional<JobVersion> findJobVersion(String namespaceName, String jobName, UUID jobVersionUuid);

  @SqlQuery(BASE_SELECT_ON_JOB_VERSIONS + "LIMIT :limit OFFSET :offset")
  List<JobVersion> findAllJobVersions(String namespaceName, String jobName, int limit, int offset);

  /**
   * Used to upsert a {@link JobVersionRow} object; on version conflict, the job version object is
   * returned with the {@code updated_at} column set to the last modified timestamp.
   *
   * @param jobVersionUuid The unique ID of the job version.
   * @param now The last modified timestamp of the job version.
   * @param jobUuid The unique ID of the job associated with the version.
   * @param jobLocation The source code location for the job.
   * @param version The version of the job; for internal use only.
   * @param jobName The name of the job.
   * @param namespaceUuid The unique ID of the namespace associated with the job version.
   * @param namespaceName The namespace associated with the job version.
   * @return The {@link ExtendedJobVersionRow} object inserted into the {@code job_versions} table.
   */
  // TODO: A JobVersionRow object should be immutable; replace with JobVersionDao.insertJobVersion()
  @SqlQuery(
      """
    INSERT INTO job_versions (
      uuid,
      created_at,
      updated_at,
      job_uuid,
      location,
      version,
      job_name,
      namespace_uuid,
      namespace_name
    ) VALUES (
      :jobVersionUuid,
      :now,
      :now,
      :jobUuid,
      :jobLocation,
      :version,
      :jobName,
      :namespaceUuid,
      :namespaceName)
    ON CONFLICT(version) DO
    UPDATE SET updated_at = EXCLUDED.updated_at
    RETURNING *
  """)
  ExtendedJobVersionRow upsertJobVersion(
      UUID jobVersionUuid,
      Instant now,
      UUID jobUuid,
      String jobLocation,
      UUID version,
      String jobName,
      UUID namespaceUuid,
      String namespaceName);

  @SqlUpdate(
      """
    INSERT INTO job_versions_io_mapping (
      job_version_uuid, dataset_uuid, io_type, job_uuid, job_symlink_target_uuid, is_current_job_version, made_current_at)
    SELECT :jobVersionUuid, requested.dataset_uuid, :ioType, :jobUuid,
      :symlinkTargetJobUuid, TRUE, NOW()
    FROM unnest(CAST(:datasetUuids AS uuid[])) requested(dataset_uuid)
    ORDER BY requested.dataset_uuid
    ON CONFLICT (job_version_uuid, dataset_uuid, io_type, job_uuid) DO UPDATE
    SET is_current_job_version = TRUE
    WHERE job_versions_io_mapping.is_current_job_version IS DISTINCT FROM TRUE
  """)
  void upsertCurrentInputOrOutputDatasetsChunk(
      @Bind("jobVersionUuid") UUID jobVersionUuid,
      @Bind("datasetUuids") UUID[] datasetUuids,
      @Bind("jobUuid") UUID jobUuid,
      @Bind("symlinkTargetJobUuid") UUID symlinkTargetJobUuid,
      @Bind("ioType") IoType ioType);

  @SqlUpdate(
      """
    WITH requested AS (
      SELECT 0 AS io_order, input.dataset_uuid, 'INPUT' AS io_type
      FROM unnest(CAST(:inputDatasetUuids AS uuid[])) input(dataset_uuid)
      UNION ALL
      SELECT 1 AS io_order, output.dataset_uuid, 'OUTPUT' AS io_type
      FROM unnest(CAST(:outputDatasetUuids AS uuid[])) output(dataset_uuid)
    )
    INSERT INTO job_versions_io_mapping (
      job_version_uuid, dataset_uuid, io_type, job_uuid, job_symlink_target_uuid, is_current_job_version, made_current_at)
    SELECT :jobVersionUuid, requested.dataset_uuid, requested.io_type, :jobUuid,
      :symlinkTargetJobUuid, TRUE, NOW()
    FROM requested
    ORDER BY requested.io_order, requested.dataset_uuid
    ON CONFLICT (job_version_uuid, dataset_uuid, io_type, job_uuid) DO UPDATE
    SET is_current_job_version = TRUE
    WHERE job_versions_io_mapping.is_current_job_version IS DISTINCT FROM TRUE
  """)
  void upsertCurrentInputAndOutputDatasetsChunk(
      @Bind("jobVersionUuid") UUID jobVersionUuid,
      @Bind("inputDatasetUuids") UUID[] inputDatasetUuids,
      @Bind("outputDatasetUuids") UUID[] outputDatasetUuids,
      @Bind("jobUuid") UUID jobUuid,
      @Bind("symlinkTargetJobUuid") UUID symlinkTargetJobUuid);

  /** Compatibility entry point retaining the pre-batching public DAO contract. */
  @Transaction
  default void upsertCurrentInputOrOutputDatasetsFor(
      UUID jobVersionUuid,
      Iterable<UUID> datasetUuids,
      UUID jobUuid,
      UUID symlinkTargetJobUuid,
      IoType ioType) {
    upsertCurrentInputOrOutputDatasetsInCurrentTransaction(
        jobVersionUuid,
        ImmutableSortedSet.copyOf(Objects.requireNonNull(datasetUuids, "datasetUuids")),
        jobUuid,
        symlinkTargetJobUuid,
        ioType);
  }

  /** Used to upsert a single input or output dataset to a given job version. */
  default void upsertCurrentInputOrOutputDatasetFor(
      UUID jobVersionUuid,
      UUID datasetUuid,
      UUID jobUuid,
      UUID symlinkTargetJobUuid,
      IoType ioType) {
    upsertCurrentInputOrOutputDatasetsFor(
        jobVersionUuid, List.of(datasetUuid), jobUuid, symlinkTargetJobUuid, ioType);
  }

  @SqlUpdate(
      """
    UPDATE job_versions_io_mapping
    SET is_current_job_version = FALSE
    WHERE (job_uuid = :jobUuid OR job_symlink_target_uuid = :jobUuid)
    AND job_version_uuid != :jobVersionUuid
    AND io_type = :ioType
    AND is_current_job_version = TRUE;
  """)
  void markInputOrOutputDatasetAsPreviousFor(UUID jobVersionUuid, UUID jobUuid, IoType ioType);

  @SqlUpdate(
      """
    UPDATE job_versions_io_mapping
    SET is_current_job_version = FALSE
    WHERE (job_uuid = :jobUuid OR job_symlink_target_uuid = :jobUuid)
    AND job_version_uuid != :jobVersionUuid
    AND io_type IN ('INPUT', 'OUTPUT')
    AND is_current_job_version = TRUE;
  """)
  void markInputAndOutputDatasetsAsPreviousFor(UUID jobVersionUuid, UUID jobUuid);

  @SqlUpdate(
      """
    UPDATE job_versions_io_mapping
    SET is_current_job_version = FALSE
    WHERE (job_uuid = :jobUuid OR job_symlink_target_uuid = :jobUuid)
    AND io_type = :ioType
    AND is_current_job_version = TRUE;
  """)
  void markInputOrOutputDatasetAsPreviousFor(UUID jobUuid, IoType ioType);

  @SqlUpdate(
      """
    UPDATE job_versions_io_mapping
    SET is_current_job_version = FALSE
    WHERE (job_uuid = :jobUuid OR job_symlink_target_uuid = :jobUuid)
    AND io_type IN ('INPUT', 'OUTPUT')
    AND is_current_job_version = TRUE;
  """)
  void markInputAndOutputDatasetsAsPreviousFor(UUID jobUuid);

  /**
   * Used to link an input dataset to a given job version.
   *
   * @param inputDatasetUuid The unique ID of the input dataset.
   * @param jobUuid The unique ID of the job.
   */
  default void upsertInputDatasetFor(
      UUID jobVersionUuid, UUID inputDatasetUuid, UUID jobUuid, UUID symlinkTargetJobUuid) {
    upsertInputDatasetsFor(
        jobVersionUuid, List.of(inputDatasetUuid), jobUuid, symlinkTargetJobUuid);
  }

  /** Used to link input datasets to a given job version. */
  @Transaction
  default void upsertInputDatasetsFor(
      UUID jobVersionUuid, List<UUID> inputDatasetUuids, UUID jobUuid, UUID symlinkTargetJobUuid) {
    flushCurrentJobVersionIoInCurrentTransaction(
        CurrentJobVersionIoWrite.of(
            jobVersionUuid, jobUuid, symlinkTargetJobUuid, inputDatasetUuids, List.of()));
  }

  /**
   * Used to link an output dataset to a given job version.
   *
   * @param outputDatasetUuid The unique ID of the output dataset.
   * @param jobUuid The unique ID of the job.
   */
  default void upsertOutputDatasetFor(
      UUID jobVersionUuid, UUID outputDatasetUuid, UUID jobUuid, UUID symlinkTargetJobUuid) {
    upsertOutputDatasetsFor(
        jobVersionUuid, List.of(outputDatasetUuid), jobUuid, symlinkTargetJobUuid);
  }

  /** Used to link output datasets to a given job version. */
  @Transaction
  default void upsertOutputDatasetsFor(
      UUID jobVersionUuid, List<UUID> outputDatasetUuids, UUID jobUuid, UUID symlinkTargetJobUuid) {
    flushCurrentJobVersionIoInCurrentTransaction(
        CurrentJobVersionIoWrite.of(
            jobVersionUuid, jobUuid, symlinkTargetJobUuid, List.of(), outputDatasetUuids));
  }

  /**
   * Flushes deterministic job-version I/O mutations in input-then-output order. The caller owns the
   * surrounding transaction.
   */
  default void flushCurrentJobVersionIoInCurrentTransaction(CurrentJobVersionIoWrite write) {
    if (!write.inputDatasetUuids().isEmpty() && !write.outputDatasetUuids().isEmpty()) {
      markInputAndOutputDatasetsAsPreviousFor(write.jobVersionUuid(), write.jobUuid());
      upsertCurrentInputAndOutputDatasetsInCurrentTransaction(write);
    } else if (!write.inputDatasetUuids().isEmpty()) {
      markInputOrOutputDatasetAsPreviousFor(write.jobVersionUuid(), write.jobUuid(), IoType.INPUT);
      upsertCurrentInputOrOutputDatasetsInCurrentTransaction(
          write.jobVersionUuid(),
          write.inputDatasetUuids(),
          write.jobUuid(),
          write.symlinkTargetJobUuid(),
          IoType.INPUT);
    } else if (!write.outputDatasetUuids().isEmpty()) {
      markInputOrOutputDatasetAsPreviousFor(write.jobVersionUuid(), write.jobUuid(), IoType.OUTPUT);
      upsertCurrentInputOrOutputDatasetsInCurrentTransaction(
          write.jobVersionUuid(),
          write.outputDatasetUuids(),
          write.jobUuid(),
          write.symlinkTargetJobUuid(),
          IoType.OUTPUT);
    }
  }

  private void upsertCurrentInputAndOutputDatasetsInCurrentTransaction(
      CurrentJobVersionIoWrite write) {
    List<UUID> inputDatasetUuids = write.inputDatasetUuids().asList();
    List<UUID> outputDatasetUuids = write.outputDatasetUuids().asList();
    int inputFrom = 0;
    int outputFrom = 0;
    while (inputFrom < inputDatasetUuids.size() || outputFrom < outputDatasetUuids.size()) {
      int inputTo = Math.min(inputFrom + JOB_VERSION_IO_BATCH_SIZE, inputDatasetUuids.size());
      int remainingBatchCapacity = JOB_VERSION_IO_BATCH_SIZE - (inputTo - inputFrom);
      int outputTo = Math.min(outputFrom + remainingBatchCapacity, outputDatasetUuids.size());
      upsertCurrentInputAndOutputDatasetsChunk(
          write.jobVersionUuid(),
          inputDatasetUuids.subList(inputFrom, inputTo).toArray(UUID[]::new),
          outputDatasetUuids.subList(outputFrom, outputTo).toArray(UUID[]::new),
          write.jobUuid(),
          write.symlinkTargetJobUuid());
      inputFrom = inputTo;
      outputFrom = outputTo;
    }
  }

  private void upsertCurrentInputOrOutputDatasetsInCurrentTransaction(
      UUID jobVersionUuid,
      ImmutableSortedSet<UUID> datasetUuids,
      UUID jobUuid,
      UUID symlinkTargetJobUuid,
      IoType ioType) {
    List<UUID> sortedDatasetUuids = datasetUuids.asList();
    for (int from = 0; from < sortedDatasetUuids.size(); from += JOB_VERSION_IO_BATCH_SIZE) {
      int to = Math.min(from + JOB_VERSION_IO_BATCH_SIZE, sortedDatasetUuids.size());
      upsertCurrentInputOrOutputDatasetsChunk(
          jobVersionUuid,
          sortedDatasetUuids.subList(from, to).toArray(UUID[]::new),
          jobUuid,
          symlinkTargetJobUuid,
          ioType);
    }
  }

  /**
   * Returns the input datasets to a given job version.
   *
   * @param jobVersionUuid The unique ID of the job version.
   * @return The input datasets for the job version.
   */
  default List<UUID> findInputDatasetsFor(UUID jobVersionUuid) {
    return findInputOrOutputDatasetsFor(jobVersionUuid, IoType.INPUT);
  }

  /**
   * Returns the output datasets to a given job version.
   *
   * @param jobVersionUuid The unique ID of the job version.
   * @return The output datasets for the job version.
   */
  default List<UUID> findOutputDatasetsFor(UUID jobVersionUuid) {
    return findInputOrOutputDatasetsFor(jobVersionUuid, IoType.OUTPUT);
  }

  /**
   * Verifies if a job with a specified job version is present in table.
   *
   * @param version Version identifier
   */
  @SqlQuery("SELECT EXISTS (SELECT 1 FROM job_versions WHERE version = :version)")
  boolean versionExists(UUID version);

  /**
   * Returns the input or output datasets for a given job version.
   *
   * @param jobVersionUuid The unique ID of the job version.
   * @param ioType The {@link IoType} of the dataset.
   */
  @SqlQuery(
      """
    SELECT dataset_uuid
    FROM job_versions_io_mapping
    WHERE job_version_uuid = :jobVersionUuid
    AND io_type = :ioType
  """)
  List<UUID> findInputOrOutputDatasetsFor(UUID jobVersionUuid, IoType ioType);

  @SqlQuery(
      """
      SELECT
        'INPUT' AS io_type,
        dv.uuid,
        dv.created_at,
        dv.dataset_uuid,
        dv.version,
        dv.dataset_schema_version_uuid,
        dv.lifecycle_state,
        dv.run_uuid,
        dv.namespace_name,
        dv.dataset_name
      FROM runs_input_mapping rim
      INNER JOIN dataset_versions dv ON dv.uuid = rim.dataset_version_uuid
      WHERE rim.run_uuid = :runUuid
      UNION ALL
      SELECT
        'OUTPUT' AS io_type,
        dv.uuid,
        dv.created_at,
        dv.dataset_uuid,
        dv.version,
        dv.dataset_schema_version_uuid,
        dv.lifecycle_state,
        dv.run_uuid,
        dv.namespace_name,
        dv.dataset_name
      FROM dataset_versions dv
      WHERE dv.run_uuid = :runUuid
      ORDER BY io_type, namespace_name, dataset_name, uuid
      """)
  List<RunIoRow> findRunIoRows(UUID runUuid);

  /** Returns one immutable, cumulative input/output snapshot for the given run. */
  default RunIoSnapshot findRunIoSnapshot(UUID runUuid) {
    return RunIoSnapshot.from(findRunIoRows(runUuid));
  }

  @SqlQuery(
      """
    SELECT d.namespace_name, d.name, io.io_type
    FROM job_versions_io_mapping io
    INNER JOIN jobs_view j ON j.current_version_uuid = io.job_version_uuid
    INNER JOIN datasets_view d on d.uuid = io.dataset_uuid
    WHERE j.name = :jobName AND j.namespace_name=:jobNamespace
  """)
  List<JobDataset> findCurrentInputOutputDatasetsFor(String jobNamespace, String jobName);

  /**
   * Used to associate a {@link Run} to a given job version. A run is an instance of a job version.
   * When a run object is instantiated, the {@code latest_run_uuid} column in the {@code
   * job_versions} table is updated and set to the unique ID of the latest run of the version. Note,
   * multiple run instances may be linked to a job version as runs are based on a version.
   *
   * @param jobVersionUuid The unique ID of the job version.
   * @param updatedAt The last modified timestamp of the job version.
   * @param latestRunUuid The unique ID of the {@link Run} associated with the job version.
   */
  @SqlUpdate(
      """
    UPDATE job_versions
    SET updated_at = :updatedAt,
      latest_run_uuid = :latestRunUuid
    WHERE uuid = :jobVersionUuid
      AND (latest_run_uuid IS DISTINCT FROM :latestRunUuid
        OR updated_at IS DISTINCT FROM :updatedAt)
  """)
  void updateLatestRunFor(UUID jobVersionUuid, Instant updatedAt, UUID latestRunUuid);

  /** Returns the unique ID of the latest {@link Run} for a given job version. */
  @SqlQuery("SELECT latest_run_uuid FROM job_versions WHERE uuid = :jobVersionUuid")
  Optional<UUID> findLatestRunFor(UUID jobVersionUuid);

  /** Returns the {@link JobVersionRow} object for a given the unique run ID . */
  @SqlQuery("SELECT * FROM job_versions WHERE latest_run_uuid = :runUuid")
  Optional<ExtendedJobVersionRow> findJobVersionFor(UUID runUuid);

  /** Returns the total row count for the {@code job_versions} table; used for testing only. */
  @VisibleForTesting
  @SqlQuery("SELECT COUNT(*) FROM job_versions")
  int count();

  /**
   * Links facets of the given run to
   *
   * @param runUuid
   * @param jobVersionUuid
   */
  @SqlUpdate(
      """
    UPDATE job_facets
    SET job_version_uuid = :jobVersionUuid
    WHERE run_uuid = :runUuid
      AND job_version_uuid IS DISTINCT FROM :jobVersionUuid
  """)
  void linkJobFacetsToJobVersion(UUID runUuid, UUID jobVersionUuid);

  /**
   * Used to upsert an immutable {@link JobVersionRow}. A {@link Version} is generated using {@link
   * Utils#newJobVersionFor} based on the jobs inputs and inputs, source code location, and context.
   *
   * @param jobRow The job.
   * @return A {@link BagOfJobVersionInfo} object.
   */
  @Transaction
  default BagOfJobVersionInfo upsertRunlessJobVersion(
      @NonNull JobRow jobRow, List<DatasetRecord> inputs, List<DatasetRecord> outputs) {
    // Get the namespace for the job.
    final NamespaceRow namespaceRow =
        createNamespaceDao().findNamespaceByName(jobRow.getNamespaceName()).get();

    return upsertRunlessJobVersionInTransaction(jobRow, namespaceRow, inputs, outputs);
  }

  /** Intake variant that reuses the namespace already resolved for the event. */
  @Transaction
  default BagOfJobVersionInfo upsertRunlessJobVersion(
      @NonNull JobRow jobRow,
      @NonNull NamespaceRow namespaceRow,
      List<DatasetRecord> inputs,
      List<DatasetRecord> outputs) {
    return upsertRunlessJobVersionInTransaction(jobRow, namespaceRow, inputs, outputs);
  }

  /** Transaction-assuming runless core that reuses the namespace already resolved for the event. */
  default BagOfJobVersionInfo upsertRunlessJobVersionInTransaction(
      @NonNull JobRow jobRow,
      @NonNull NamespaceRow namespaceRow,
      List<DatasetRecord> inputs,
      List<DatasetRecord> outputs) {
    validateNamespace(jobRow, namespaceRow);

    // Get the job.
    final JobDao jobDao = createJobDao();

    // Generate the version for the job; the version may already exist.
    final Version jobVersion =
        Utils.newJobVersionFor(
            NamespaceName.of(jobRow.getNamespaceName()),
            JobName.of(
                Optional.ofNullable(jobRow.getParentJobName())
                    .map(pn -> pn + "." + jobRow.getSimpleName())
                    .orElse(jobRow.getName())),
            toDatasetIds(
                inputs.stream().map(i -> i.getDatasetVersionRow()).collect(Collectors.toList())),
            toDatasetIds(
                outputs.stream().map(i -> i.getDatasetVersionRow()).collect(Collectors.toList())),
            jobRow.getLocation());

    // Add the job version.
    final JobVersionDao jobVersionDao = createJobVersionDao();
    final JobVersionRow jobVersionRow =
        jobVersionDao.upsertJobVersion(
            UUID.randomUUID(),
            jobRow.getCreatedAt(),
            jobRow.getUuid(),
            jobRow.getLocation(),
            jobVersion.getValue(),
            jobRow.getName(),
            namespaceRow.getUuid(),
            jobRow.getNamespaceName());

    // Link the input and output datasets inside the caller's event transaction.
    jobVersionDao.flushCurrentJobVersionIoInCurrentTransaction(
        CurrentJobVersionIoWrite.of(
            jobVersionRow.getUuid(),
            jobVersionRow.getJobUuid(),
            jobRow.getSymlinkTargetId(),
            inputs.stream()
                .map(input -> input.getDatasetVersionRow().getDatasetUuid())
                .collect(Collectors.toList()),
            outputs.stream()
                .map(output -> output.getDatasetVersionRow().getDatasetUuid())
                .collect(Collectors.toList())));

    jobDao.updateVersionFor(jobRow.getUuid(), jobRow.getCreatedAt(), jobVersionRow.getUuid());

    return new BagOfJobVersionInfo(
        jobRow,
        jobVersionRow,
        inputs.stream()
            .map(JobVersionDao::toExtendedDatasetVersionRow)
            .collect(Collectors.toList()),
        outputs.stream()
            .map(JobVersionDao::toExtendedDatasetVersionRow)
            .collect(Collectors.toList()));
  }

  private static ExtendedDatasetVersionRow toExtendedDatasetVersionRow(DatasetRecord d) {
    return new ExtendedDatasetVersionRow(
        d.getDatasetRow().getUuid(),
        d.getDatasetRow().getCreatedAt(),
        d.getDatasetVersionRow().getDatasetUuid(),
        d.getDatasetVersionRow().getVersion(),
        d.getDatasetVersionRow().getSchemaVersionUuid().orElse(null),
        null,
        null,
        d.getDatasetRow().getNamespaceName(),
        d.getDatasetRow().getName());
  }

  /**
   * Used to upsert an immutable {@link JobVersionRow} object when a {@link Run} has transitioned. A
   * {@link Version} is generated using {@link Utils#newJobVersionFor} based on the jobs inputs and
   * inputs, source code location, and context. A version for a given job is created <i>only</i>
   * when a {@link Run} transitions into a {@code COMPLETED}, {@code ABORTED}, or {@code FAILED}
   * state.
   *
   * @param jobRowRunDetails The job row with run details.
   * @param runState The current run state.
   * @param transitionedAt The timestamp of the run state transition.
   * @return A {@link BagOfJobVersionInfo} object.
   */
  @Transaction
  default BagOfJobVersionInfo upsertJobVersionOnRunTransition(
      @NonNull JobRowRunDetails jobRowRunDetails,
      @NonNull RunState runState,
      @NonNull Instant transitionedAt,
      boolean linkJobToJobVersion) {
    return upsertJobVersionOnRunTransitionInTransaction(
        jobRowRunDetails, runState, transitionedAt, linkJobToJobVersion);
  }

  /** Transaction-assuming core for a run transition that creates or refreshes a job version. */
  default BagOfJobVersionInfo upsertJobVersionOnRunTransitionInTransaction(
      @NonNull JobRowRunDetails jobRowRunDetails,
      @NonNull RunState runState,
      @NonNull Instant transitionedAt,
      boolean linkJobToJobVersion) {
    // Get the job.
    final JobDao jobDao = createJobDao();

    // Add the job version.
    final JobVersionDao jobVersionDao = createJobVersionDao();

    final JobVersionRow jobVersionRow =
        jobVersionDao.upsertJobVersion(
            UUID.randomUUID(),
            transitionedAt, // Use the timestamp of when the run state transitioned.
            jobRowRunDetails.jobRow.getUuid(),
            jobRowRunDetails.jobRow.getLocation(),
            jobRowRunDetails.jobVersion.getValue(),
            jobRowRunDetails.jobRow.getName(),
            jobRowRunDetails.namespaceRow.getUuid(),
            jobRowRunDetails.jobRow.getNamespaceName());

    // Link the input and output datasets inside the caller's event transaction.
    jobVersionDao.flushCurrentJobVersionIoInCurrentTransaction(
        CurrentJobVersionIoWrite.of(
            jobVersionRow.getUuid(),
            jobVersionRow.getJobUuid(),
            jobRowRunDetails.jobRow.getSymlinkTargetId(),
            jobRowRunDetails.jobVersionInputs.stream()
                .map(ExtendedDatasetVersionRow::getDatasetUuid)
                .collect(Collectors.toList()),
            jobRowRunDetails.jobVersionOutputs.stream()
                .map(ExtendedDatasetVersionRow::getDatasetUuid)
                .collect(Collectors.toList())));

    // Link the job version to the run.
    createRunDao().updateJobVersion(jobRowRunDetails.runUuid, jobVersionRow.getUuid());

    // Link the run to the job version; multiple run instances may be linked to a job version.
    jobVersionDao.updateLatestRunFor(
        jobVersionRow.getUuid(), transitionedAt, jobRowRunDetails.runUuid);

    // Link the job facets to this job version
    jobVersionDao.linkJobFacetsToJobVersion(jobRowRunDetails.runUuid, jobVersionRow.getUuid());

    if (linkJobToJobVersion) {
      jobDao.updateVersionFor(
          jobRowRunDetails.jobRow.getUuid(), transitionedAt, jobVersionRow.getUuid());
    }

    return new BagOfJobVersionInfo(
        jobRowRunDetails.jobRow,
        jobVersionRow,
        jobRowRunDetails.jobVersionInputs,
        jobRowRunDetails.jobVersionOutputs);
  }

  /** Returns the specified {@link ExtendedDatasetVersionRow}s as {@link DatasetId}s. */
  default ImmutableSortedSet<DatasetId> toDatasetIds(
      @NonNull final List<DatasetVersionRow> datasetVersionRows) {
    final ImmutableSortedSet.Builder<DatasetId> datasetIds = ImmutableSortedSet.naturalOrder();
    for (final DatasetVersionRow datasetVersionRow : datasetVersionRows) {
      datasetIds.add(toDatasetId(datasetVersionRow));
    }
    return datasetIds.build();
  }

  private DatasetId toDatasetId(DatasetVersionRow dataset) {
    return new DatasetId(
        NamespaceName.of(dataset.getNamespaceName()), DatasetName.of(dataset.getDatasetName()));
  }

  default JobRowRunDetails loadJobRowRunDetails(JobRow jobRow, UUID runUuid) {
    // Get the namespace for the job.
    final NamespaceRow namespaceRow =
        createNamespaceDao().findNamespaceByName(jobRow.getNamespaceName()).get();

    return loadJobRowRunDetails(jobRow, namespaceRow, runUuid, findRunIoSnapshot(runUuid));
  }

  /**
   * Intake variant that reuses the namespace and cumulative run I/O already resolved for an event.
   */
  default JobRowRunDetails loadJobRowRunDetails(
      @NonNull JobRow jobRow,
      @NonNull NamespaceRow namespaceRow,
      UUID runUuid,
      @NonNull RunIoSnapshot runIoSnapshot) {
    validateNamespace(jobRow, namespaceRow);

    // Generate the version for the job; the version may already exist.
    final Version jobVersion =
        Utils.newJobVersionFor(
            NamespaceName.of(jobRow.getNamespaceName()),
            JobName.of(
                Optional.ofNullable(jobRow.getParentJobName())
                    .map(pn -> pn + "." + jobRow.getSimpleName())
                    .orElse(jobRow.getName())),
            toDatasetIds(
                runIoSnapshot.getInputs().stream()
                    .map(i -> (DatasetVersionRow) i)
                    .collect(Collectors.toList())),
            toDatasetIds(
                runIoSnapshot.getOutputs().stream()
                    .map(o -> (DatasetVersionRow) o)
                    .collect(Collectors.toList())),
            jobRow.getLocation());

    return new JobRowRunDetails(
        jobRow,
        runUuid,
        namespaceRow,
        runIoSnapshot.getInputs(),
        runIoSnapshot.getOutputs(),
        jobVersion);
  }

  private static void validateNamespace(JobRow jobRow, NamespaceRow namespaceRow) {
    boolean uuidMatches =
        jobRow.getNamespaceUuid() == null
            || jobRow.getNamespaceUuid().equals(namespaceRow.getUuid());
    if (!uuidMatches || !jobRow.getNamespaceName().equals(namespaceRow.getName())) {
      throw new IllegalArgumentException("The namespace row does not belong to the job");
    }
  }

  /** A container class for job version info. */
  @Value
  class BagOfJobVersionInfo {
    JobRow jobRow;
    JobVersionRow jobVersionRow;
    List<ExtendedDatasetVersionRow> inputs;
    List<ExtendedDatasetVersionRow> outputs;
  }

  record JobDataset(String namespace, String name, IoType ioType) {}

  record CurrentJobVersionIoWrite(
      UUID jobVersionUuid,
      UUID jobUuid,
      @Nullable UUID symlinkTargetJobUuid,
      ImmutableSortedSet<UUID> inputDatasetUuids,
      ImmutableSortedSet<UUID> outputDatasetUuids) {
    public static CurrentJobVersionIoWrite of(
        UUID jobVersionUuid,
        UUID jobUuid,
        @Nullable UUID symlinkTargetJobUuid,
        Iterable<UUID> inputDatasetUuids,
        Iterable<UUID> outputDatasetUuids) {
      return new CurrentJobVersionIoWrite(
          Objects.requireNonNull(jobVersionUuid, "jobVersionUuid"),
          Objects.requireNonNull(jobUuid, "jobUuid"),
          symlinkTargetJobUuid,
          ImmutableSortedSet.copyOf(Objects.requireNonNull(inputDatasetUuids, "inputDatasetUuids")),
          ImmutableSortedSet.copyOf(
              Objects.requireNonNull(outputDatasetUuids, "outputDatasetUuids")));
    }
  }

  record JobRowRunDetails(
      JobRow jobRow,
      UUID runUuid,
      NamespaceRow namespaceRow,
      List<ExtendedDatasetVersionRow> jobVersionInputs,
      List<ExtendedDatasetVersionRow> jobVersionOutputs,
      Version jobVersion) {}

  class JobDatasetMapper implements RowMapper<JobDataset> {
    @Override
    public JobDataset map(ResultSet rs, StatementContext ctx) throws SQLException {
      return new JobDataset(
          stringOrThrow(rs, Columns.NAMESPACE_NAME),
          stringOrThrow(rs, Columns.NAME),
          IoType.valueOf(stringOrThrow(rs, Columns.IO_TYPE)));
    }
  }
}
