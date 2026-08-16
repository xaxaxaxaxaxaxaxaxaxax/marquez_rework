/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.OpenLineageDefaults.DEFAULT_NAMESPACE_OWNER;
import static org.jdbi.v3.sqlobject.customizer.BindList.EmptyHandling.NULL_STRING;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.Value;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetType;
import marquez.common.models.NamespaceName;
import marquez.common.models.TagName;
import marquez.db.mappers.DatasetMapper;
import marquez.db.mappers.DatasetRowMapper;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetSymlinkRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.ProjectionOrder;
import marquez.db.models.SourceRow;
import marquez.db.models.TagRow;
import marquez.service.DatasetService;
import marquez.service.models.Dataset;
import marquez.service.models.DatasetMeta;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.customizer.BindBeanList;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterRowMapper(DatasetRowMapper.class)
@RegisterRowMapper(DatasetMapper.class)
public interface DatasetDao extends BaseDao {
  int MAX_DATASET_CURRENT_VERSION_UPDATES = 1000;
  int MAX_DATASETS_PER_UPSERT = 1000;

  @SqlQuery(
      "SELECT EXISTS ("
          + "SELECT 1 FROM datasets_view AS d "
          + "WHERE d.name = :datasetName AND d.namespace_name = :namespaceName)")
  boolean exists(String namespaceName, String datasetName);

  @SqlBatch(
      "INSERT INTO datasets_tag_mapping (dataset_uuid, tag_uuid, tagged_at) "
          + "VALUES (:rowUuid, :tagUuid, :taggedAt) "
          + "ON CONFLICT DO NOTHING")
  void updateTagMapping(@BindBean List<DatasetTagMapping> datasetTagMappings);

  @SqlUpdate(
      "UPDATE datasets "
          + "SET updated_at = GREATEST(updated_at, :lastModifiedAt), "
          + "    last_modified_at = :lastModifiedAt "
          + "WHERE uuid IN (<rowUuids>)")
  void updateLastModifiedAt(
      @BindList(onEmpty = NULL_STRING) List<UUID> rowUuids, Instant lastModifiedAt);

  @SqlUpdate(
      "UPDATE datasets "
          + "SET updated_at = GREATEST(updated_at, :updatedAt), "
          + "    current_version_uuid = :currentVersionUuid, "
          + "    open_lineage_current_version_time = NULL, "
          + "    open_lineage_current_version_key = NULL "
          + "WHERE uuid = :rowUuid")
  void updateVersion(UUID rowUuid, Instant updatedAt, UUID currentVersionUuid);

  @Transaction
  default int updateVersions(List<DatasetCurrentVersionUpdate> updates) {
    return updateVersionsInTransaction(updates);
  }

  /** Updates current versions without opening a transaction; the caller owns chunk atomicity. */
  default int updateVersionsInTransaction(List<DatasetCurrentVersionUpdate> updates) {
    return updateVersionsInTransaction(updates, null);
  }

  default int updateVersionsInTransaction(
      List<DatasetCurrentVersionUpdate> updates, @Nullable ProjectionOrder order) {
    if (updates.isEmpty()) {
      return 0;
    }

    // A projection can encounter the same dataset more than once. Preserve the singleton path's
    // encounter semantics while issuing each physical row update only once.
    Map<UUID, DatasetCurrentVersionUpdate> lastUpdateByDataset = new LinkedHashMap<>();
    for (DatasetCurrentVersionUpdate update : List.copyOf(updates)) {
      lastUpdateByDataset.put(update.getRowUuid(), update);
    }

    List<DatasetCurrentVersionUpdate> writes = new ArrayList<>(lastUpdateByDataset.values());
    writes.sort((left, right) -> left.getRowUuid().compareTo(right.getRowUuid()));

    int updated = 0;
    for (int start = 0; start < writes.size(); start += MAX_DATASET_CURRENT_VERSION_UPDATES) {
      updated +=
          order == null
              ? updateVersionsChunk(
                  writes.subList(
                      start, Math.min(start + MAX_DATASET_CURRENT_VERSION_UPDATES, writes.size())))
              : updateVersionsOrderedChunk(
                  writes.subList(
                      start, Math.min(start + MAX_DATASET_CURRENT_VERSION_UPDATES, writes.size())),
                  order.getEventTime(),
                  order.getEventKey());
    }
    return updated;
  }

  @SqlUpdate(
      """
          UPDATE datasets AS d
          SET updated_at = GREATEST(d.updated_at, CAST(v.updated_at AS timestamptz)),
              current_version_uuid = CAST(v.current_version_uuid AS uuid),
              open_lineage_current_version_time = NULL,
              open_lineage_current_version_key = NULL
          FROM (VALUES <values>) AS v(row_uuid, updated_at, current_version_uuid)
          WHERE d.uuid = CAST(v.row_uuid AS uuid)
          """)
  int updateVersionsChunk(
      @BindBeanList(
              value = "values",
              propertyNames = {"rowUuid", "updatedAt", "currentVersionUuid"})
          List<DatasetCurrentVersionUpdate> updates);

  @SqlUpdate(
      """
          UPDATE datasets AS d
          SET updated_at = GREATEST(d.updated_at, CAST(v.updated_at AS timestamptz)),
              current_version_uuid = CAST(v.current_version_uuid AS uuid),
              open_lineage_current_version_time = :projectionTime,
              open_lineage_current_version_key = :projectionKey
          FROM (VALUES <values>) AS v(row_uuid, updated_at, current_version_uuid)
          WHERE d.uuid = CAST(v.row_uuid AS uuid)
            AND d.is_hidden IS FALSE
            AND ROW(:projectionTime, :projectionKey) >= ROW(
                COALESCE(
                    d.open_lineage_current_version_time,
                    GREATEST(
                        d.updated_at,
                        (SELECT dv.created_at
                         FROM dataset_versions AS dv
                         WHERE dv.uuid = d.current_version_uuid)),
                    '-infinity'::timestamptz),
                CASE WHEN d.open_lineage_current_version_time IS NULL
                     THEN decode(repeat('00', 32), 'hex')
                     ELSE d.open_lineage_current_version_key END)
          """)
  int updateVersionsOrderedChunk(
      @BindBeanList(
              value = "values",
              propertyNames = {"rowUuid", "updatedAt", "currentVersionUuid"})
          List<DatasetCurrentVersionUpdate> updates,
      Instant projectionTime,
      byte[] projectionKey);

  @SqlQuery(
      """
          SELECT d.*, dv.fields, dv.lifecycle_state, sv.schema_location, t.tags, facets
          FROM datasets_view d
          LEFT JOIN dataset_versions dv ON d.current_version_uuid = dv.uuid
          LEFT JOIN stream_versions AS sv ON sv.dataset_version_uuid = dv.uuid
          LEFT JOIN (
              SELECT ARRAY_AGG(t.name) AS tags, m.dataset_uuid
              FROM tags AS t
                       INNER JOIN datasets_tag_mapping AS m ON m.tag_uuid = t.uuid
              GROUP BY m.dataset_uuid
          ) t ON t.dataset_uuid = d.uuid
          LEFT JOIN (
              SELECT
                  df.dataset_version_uuid,
                  JSONB_AGG(df.facet ORDER BY df.lineage_event_time ASC) AS facets
              FROM dataset_facets AS df
              WHERE df.facet IS NOT NULL AND
               (df.type ILIKE 'dataset' OR df.type ILIKE 'unknown' OR df.type ILIKE 'input') AND
                df.dataset_uuid = (SELECT uuid FROM datasets WHERE name = :datasetName AND namespace_name = :namespaceName)
              GROUP BY df.dataset_version_uuid
          ) f ON f.dataset_version_uuid = d.current_version_uuid
          WHERE CAST((:namespaceName, :datasetName) AS DATASET_NAME) = ANY(d.dataset_symlinks)
      """)
  Optional<Dataset> findDatasetByName(String namespaceName, String datasetName);

  default Optional<Dataset> findWithTags(String namespaceName, String datasetName) {
    Optional<Dataset> dataset = findDatasetByName(namespaceName, datasetName);
    dataset.ifPresent(this::setFields);
    return dataset;
  }

  default void setFields(Dataset ds) {
    DatasetFieldDao datasetFieldDao = createDatasetFieldDao();

    ds.getCurrentVersion()
        .ifPresent(
            dsv -> {
              ds.setFields(datasetFieldDao.findByDatasetVersion(dsv));
            });
  }

  @SqlQuery(
      "SELECT d.* FROM datasets_view AS d WHERE d.name = :datasetName AND d.namespace_name = :namespaceName")
  Optional<DatasetRow> findDatasetAsRow(String namespaceName, String datasetName);

  @SqlQuery(
      "SELECT * FROM datasets_view WHERE name = :datasetName AND namespace_name = :namespaceName")
  Optional<DatasetRow> getUuid(String namespaceName, String datasetName);

  @SqlQuery(
      """
      WITH facets_t AS
          (SELECT df.dataset_version_uuid,
                  df.facet,
                  df."name",
                  df.created_at,
                  rank() OVER (PARTITION BY df.dataset_version_uuid, "name"
                               ORDER BY created_at DESC) AS r
          FROM dataset_facets AS df
          WHERE df.facet IS NOT NULL
             AND (df.type ILIKE 'dataset'
                  OR df.type ILIKE 'unknown'
                  OR df.type ILIKE 'input')
             AND df.dataset_uuid IN
               (SELECT UUID
                FROM datasets_view
                WHERE namespace_name = :namespaceName
                ORDER BY name
                LIMIT 10
                OFFSET :offset))
      SELECT d.*,
          dv.fields,
          dv.lifecycle_state,
          sv.schema_location,
          t.tags,
          facets
      FROM datasets_view d
      LEFT JOIN dataset_versions dv ON d.current_version_uuid = dv.uuid
      LEFT JOIN stream_versions AS sv ON sv.dataset_version_uuid = dv.uuid
      LEFT JOIN
        (SELECT ARRAY_AGG(t.name) AS tags,
                m.dataset_uuid
            FROM tags AS t
            INNER JOIN datasets_tag_mapping AS m ON m.tag_uuid = t.uuid
            GROUP BY m.dataset_uuid) t ON t.dataset_uuid = d.uuid
      LEFT JOIN
        (SELECT df.dataset_version_uuid,
                JSONB_AGG(df.facet) AS facets
          FROM facets_t AS df
          WHERE r = 1
          GROUP BY df.dataset_version_uuid) f ON f.dataset_version_uuid = d.current_version_uuid
      WHERE d.namespace_name = :namespaceName
      ORDER BY d.name
      LIMIT :limit
      OFFSET :offset
      """)
  List<Dataset> findAll(String namespaceName, int limit, int offset);

  @SqlQuery("SELECT count(*) FROM datasets_view")
  int count();

  @SqlQuery("SELECT count(*) FROM datasets_view AS j WHERE j.namespace_name = :namespaceName")
  int countFor(String namespaceName);

  default List<Dataset> findAllWithTags(String namespaceName, int limit, int offset) {
    List<Dataset> datasets = findAll(namespaceName, limit, offset);
    return datasets.stream().peek(this::setFields).collect(Collectors.toList());
  }

  @SqlQuery(
      """
      INSERT INTO datasets (
          uuid,
          type,
          created_at,
          updated_at,
          namespace_uuid,
          namespace_name,
          source_uuid,
          source_name,
          name,
          physical_name,
          description,
          is_deleted,
          is_hidden
          ) VALUES (
            :uuid,
            :type,
            :now,
            :now,
            :namespaceUuid,
            :namespaceName,
            :sourceUuid,
            :sourceName,
            :name,
            :physicalName,
            :description,
            :isDeleted,
            false
          ) ON CONFLICT (uuid)
          DO UPDATE SET
          type = EXCLUDED.type,
          updated_at = GREATEST(datasets.updated_at, EXCLUDED.updated_at),
          physical_name = EXCLUDED.physical_name,
          description = EXCLUDED.description,
          is_deleted = EXCLUDED.is_deleted,
          is_hidden = EXCLUDED.is_hidden,
          open_lineage_snapshot_time = NULL,
          open_lineage_snapshot_key = NULL
          RETURNING *
    """)
  DatasetRow upsert(
      UUID uuid,
      DatasetType type,
      Instant now,
      UUID namespaceUuid,
      String namespaceName,
      UUID sourceUuid,
      String sourceName,
      String name,
      String physicalName,
      String description,
      boolean isDeleted);

  /**
   * Upserts datasets without opening a transaction. The caller must provide one outer transaction
   * so every chunk is atomic with the rest of the projection.
   */
  default List<DatasetRow> upsertAllInTransaction(List<DatasetUpsert> datasets) {
    return upsertAllInTransaction(datasets, null);
  }

  default List<DatasetRow> upsertAllInTransaction(
      List<DatasetUpsert> datasets, @Nullable ProjectionOrder order) {
    if (datasets.isEmpty()) {
      return Collections.emptyList();
    }

    List<DatasetUpsert> inputs = List.copyOf(datasets);
    Map<UUID, DatasetUpsert> selectedWriteByUuid = new LinkedHashMap<>();
    boolean duplicateUuid = false;
    for (DatasetUpsert dataset : inputs) {
      if (order == null) {
        duplicateUuid |= selectedWriteByUuid.putIfAbsent(dataset.getUuid(), dataset) != null;
      } else {
        duplicateUuid |= selectedWriteByUuid.put(dataset.getUuid(), dataset) != null;
      }
    }

    if (duplicateUuid && order == null) {
      // A single PostgreSQL upsert cannot affect the same target row twice. More importantly,
      // legacy dataset updates are occurrence-sensitive, so preserve each intermediate state.
      List<DatasetRow> ordered = new ArrayList<>(inputs.size());
      for (DatasetUpsert dataset : inputs) {
        ordered.add(
            upsert(
                dataset.getUuid(),
                dataset.getType(),
                dataset.getNow(),
                dataset.getNamespaceUuid(),
                dataset.getNamespaceName(),
                dataset.getSourceUuid(),
                dataset.getSourceName(),
                dataset.getName(),
                dataset.getPhysicalName(),
                dataset.getDescription(),
                dataset.isDeleted()));
      }
      return ordered;
    }

    // One ordered event owns one persisted tuple. Collapse aliases of the same canonical dataset
    // to their last occurrence on this I/O side. The ordered upsert accepts the same tuple again so
    // the later output side can deterministically replace an earlier input-side occurrence.
    List<DatasetUpsert> writes = new ArrayList<>(selectedWriteByUuid.values());
    writes.sort((left, right) -> compareUuidLikePostgres(left.getUuid(), right.getUuid()));

    Map<UUID, DatasetRow> rowsByUuid = new HashMap<>();
    for (int start = 0; start < writes.size(); start += MAX_DATASETS_PER_UPSERT) {
      List<DatasetRow> returned =
          order == null
              ? upsertAllChunk(
                  writes.subList(start, Math.min(start + MAX_DATASETS_PER_UPSERT, writes.size())))
              : upsertAllOpenLineageChunk(
                  writes.subList(start, Math.min(start + MAX_DATASETS_PER_UPSERT, writes.size())),
                  order.getEventTime(),
                  order.getEventKey());
      for (DatasetRow row : returned) {
        rowsByUuid.put(row.getUuid(), row);
      }
    }

    List<DatasetRow> ordered = new ArrayList<>(inputs.size());
    for (DatasetUpsert input : inputs) {
      DatasetRow row = rowsByUuid.get(input.getUuid());
      if (row == null) {
        throw new IllegalStateException(
            "Dataset upsert did not return a row for "
                + input.getNamespaceName()
                + "/"
                + input.getName());
      }
      ordered.add(row);
    }
    return ordered;
  }

  private static int compareUuidLikePostgres(UUID left, UUID right) {
    int compared =
        Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
    return compared != 0
        ? compared
        : Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
  }

  @SqlQuery(
      """
          WITH requested(
              uuid,
              type,
              created_at,
              namespace_uuid,
              namespace_name,
              source_uuid,
              source_name,
              name,
              physical_name,
              description,
              is_deleted
          ) AS (
              VALUES <values>
          )
          INSERT INTO datasets (
              uuid,
              type,
              created_at,
              updated_at,
              namespace_uuid,
              namespace_name,
              source_uuid,
              source_name,
              name,
              physical_name,
              description,
              is_deleted,
              is_hidden
          )
          SELECT
              CAST(requested.uuid AS uuid),
              CAST(requested.type AS varchar),
              CAST(requested.created_at AS timestamptz),
              CAST(requested.created_at AS timestamptz),
              CAST(requested.namespace_uuid AS uuid),
              CAST(requested.namespace_name AS varchar),
              CAST(requested.source_uuid AS uuid),
              CAST(requested.source_name AS varchar),
              CAST(requested.name AS varchar),
              CAST(requested.physical_name AS varchar),
              CAST(requested.description AS text),
              CAST(requested.is_deleted AS boolean),
              false
          FROM requested
          ORDER BY CAST(requested.uuid AS uuid)
          ON CONFLICT (uuid)
          DO UPDATE SET
              type = EXCLUDED.type,
              updated_at = GREATEST(datasets.updated_at, EXCLUDED.updated_at),
              physical_name = EXCLUDED.physical_name,
              description = EXCLUDED.description,
              is_deleted = EXCLUDED.is_deleted,
              is_hidden = EXCLUDED.is_hidden,
              open_lineage_snapshot_time = NULL,
              open_lineage_snapshot_key = NULL
          RETURNING *
          """)
  List<DatasetRow> upsertAllChunk(
      @BindBeanList(
              value = "values",
              propertyNames = {
                "uuid",
                "type",
                "now",
                "namespaceUuid",
                "namespaceName",
                "sourceUuid",
                "sourceName",
                "name",
                "physicalName",
                "description",
                "deleted"
              })
          List<DatasetUpsert> datasets);

  @SqlQuery(
      """
          WITH requested(
              uuid, type, created_at, namespace_uuid, namespace_name, source_uuid, source_name,
              name, physical_name, description, is_deleted
          ) AS (VALUES <values>)
          INSERT INTO datasets (
              uuid, type, created_at, updated_at, namespace_uuid, namespace_name, source_uuid,
              source_name, name, physical_name, description, is_deleted, is_hidden,
              open_lineage_snapshot_time, open_lineage_snapshot_key)
          SELECT
              CAST(requested.uuid AS uuid),
              CAST(requested.type AS varchar),
              CAST(requested.created_at AS timestamptz),
              CAST(requested.created_at AS timestamptz),
              CAST(requested.namespace_uuid AS uuid),
              CAST(requested.namespace_name AS varchar),
              CAST(requested.source_uuid AS uuid),
              CAST(requested.source_name AS varchar),
              CAST(requested.name AS varchar),
              CAST(requested.physical_name AS varchar),
              CAST(requested.description AS text),
              CAST(requested.is_deleted AS boolean),
              false,
              :projectionTime,
              :projectionKey
          FROM requested
          ORDER BY CAST(requested.uuid AS uuid)
          ON CONFLICT (uuid) DO UPDATE SET
              type = CASE WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                                        EXCLUDED.open_lineage_snapshot_key) >=
                                   ROW(COALESCE(datasets.open_lineage_snapshot_time,
                                                datasets.updated_at),
                                       COALESCE(datasets.open_lineage_snapshot_key,
                                                decode(repeat('00', 32), 'hex')))
                          THEN EXCLUDED.type ELSE datasets.type END,
              updated_at = CASE WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                                              EXCLUDED.open_lineage_snapshot_key) >=
                                         ROW(COALESCE(datasets.open_lineage_snapshot_time,
                                                      datasets.updated_at),
                                             COALESCE(datasets.open_lineage_snapshot_key,
                                                      decode(repeat('00', 32), 'hex')))
                                THEN GREATEST(datasets.updated_at, EXCLUDED.updated_at)
                                ELSE datasets.updated_at END,
              physical_name = CASE WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                                                 EXCLUDED.open_lineage_snapshot_key) >=
                                            ROW(COALESCE(datasets.open_lineage_snapshot_time,
                                                         datasets.updated_at),
                                                COALESCE(datasets.open_lineage_snapshot_key,
                                                         decode(repeat('00', 32), 'hex')))
                                   THEN EXCLUDED.physical_name ELSE datasets.physical_name END,
              description = CASE WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                                               EXCLUDED.open_lineage_snapshot_key) >=
                                          ROW(COALESCE(datasets.open_lineage_snapshot_time,
                                                       datasets.updated_at),
                                              COALESCE(datasets.open_lineage_snapshot_key,
                                                       decode(repeat('00', 32), 'hex')))
                                 THEN EXCLUDED.description ELSE datasets.description END,
              is_deleted = CASE WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                                              EXCLUDED.open_lineage_snapshot_key) >=
                                         ROW(COALESCE(datasets.open_lineage_snapshot_time,
                                                      datasets.updated_at),
                                             COALESCE(datasets.open_lineage_snapshot_key,
                                                      decode(repeat('00', 32), 'hex')))
                                THEN EXCLUDED.is_deleted ELSE datasets.is_deleted END,
              is_hidden = CASE WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                                             EXCLUDED.open_lineage_snapshot_key) >=
                                        ROW(COALESCE(datasets.open_lineage_snapshot_time,
                                                     datasets.updated_at),
                                            COALESCE(datasets.open_lineage_snapshot_key,
                                                     decode(repeat('00', 32), 'hex')))
                               THEN EXCLUDED.is_hidden ELSE datasets.is_hidden END,
              open_lineage_snapshot_time = CASE
                  WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                           EXCLUDED.open_lineage_snapshot_key) >=
                       ROW(COALESCE(datasets.open_lineage_snapshot_time, datasets.updated_at),
                           COALESCE(datasets.open_lineage_snapshot_key,
                                    decode(repeat('00', 32), 'hex')))
                    THEN EXCLUDED.open_lineage_snapshot_time
                  ELSE datasets.open_lineage_snapshot_time END,
              open_lineage_snapshot_key = CASE
                  WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                           EXCLUDED.open_lineage_snapshot_key) >=
                       ROW(COALESCE(datasets.open_lineage_snapshot_time, datasets.updated_at),
                           COALESCE(datasets.open_lineage_snapshot_key,
                                    decode(repeat('00', 32), 'hex')))
                    THEN EXCLUDED.open_lineage_snapshot_key
                  ELSE datasets.open_lineage_snapshot_key END
          RETURNING *
          """)
  List<DatasetRow> upsertAllOpenLineageChunk(
      @BindBeanList(
              value = "values",
              propertyNames = {
                "uuid",
                "type",
                "now",
                "namespaceUuid",
                "namespaceName",
                "sourceUuid",
                "sourceName",
                "name",
                "physicalName",
                "description",
                "deleted"
              })
          List<DatasetUpsert> datasets,
      Instant projectionTime,
      byte[] projectionKey);

  @SqlQuery(
      "INSERT INTO datasets ("
          + "uuid, "
          + "type, "
          + "created_at, "
          + "updated_at, "
          + "namespace_uuid, "
          + "namespace_name, "
          + "source_uuid, "
          + "source_name, "
          + "name, "
          + "physical_name "
          + ") VALUES ( "
          + ":uuid, "
          + ":type, "
          + ":now, "
          + ":now, "
          + ":namespaceUuid, "
          + ":namespaceName, "
          + ":sourceUuid, "
          + ":sourceName, "
          + ":name, "
          + ":physicalName) "
          + "ON CONFLICT (uuid) "
          + "DO UPDATE SET "
          + "type = EXCLUDED.type, "
          + "updated_at = GREATEST(datasets.updated_at, EXCLUDED.updated_at), "
          + "physical_name = EXCLUDED.physical_name, "
          + "open_lineage_snapshot_time = NULL, "
          + "open_lineage_snapshot_key = NULL "
          + "RETURNING *")
  DatasetRow upsert(
      UUID uuid,
      DatasetType type,
      Instant now,
      UUID namespaceUuid,
      String namespaceName,
      UUID sourceUuid,
      String sourceName,
      String name,
      String physicalName);

  @SqlUpdate(
      """
        UPDATE datasets d
        SET is_hidden = true,
            updated_at = GREATEST(d.updated_at, statement_timestamp()),
            open_lineage_snapshot_time = NULL,
            open_lineage_snapshot_key = NULL
        FROM namespaces n
        WHERE n.uuid=d.namespace_uuid
        AND n.name=:namespaceName
      """)
  void deleteByNamespaceName(String namespaceName);

  @SqlQuery(
      """
        UPDATE datasets d
        SET is_hidden = true,
            updated_at = GREATEST(d.updated_at, statement_timestamp()),
            open_lineage_snapshot_time = NULL,
            open_lineage_snapshot_key = NULL
        FROM namespaces n
        WHERE n.uuid = d.namespace_uuid
        AND n.name=:namespaceName AND d.name=:name
        RETURNING *
      """)
  Optional<DatasetRow> delete(String namespaceName, String name);

  @SqlUpdate(
      """
        DELETE FROM datasets_tag_mapping dtm
        WHERE EXISTS (
            SELECT 1
            FROM
              datasets d
            JOIN
              tags t
            ON
              d.uuid = dtm.dataset_uuid
            AND
              t.uuid = dtm.tag_uuid
            JOIN
              namespaces n
            ON
              d.namespace_uuid = n.uuid
            WHERE
              d.name = :datasetName
            AND
              t.name = :tagName
            AND
              n.name = :namespaceName
        );
      """)
  void deleteDatasetTag(String namespaceName, String datasetName, String tagName);

  @Transaction
  default Dataset upsertDatasetMeta(
      NamespaceName namespaceName, DatasetName datasetName, DatasetMeta datasetMeta) {
    Instant now = Instant.now();
    NamespaceRow namespaceRow =
        createNamespaceDao()
            .upsertNamespaceRow(
                UUID.randomUUID(), now, namespaceName.getValue(), DEFAULT_NAMESPACE_OWNER);
    DatasetSymlinkRow symlinkRow =
        createDatasetSymlinkDao()
            .upsertDatasetSymlinkRow(
                UUID.randomUUID(), datasetName.getValue(), namespaceRow.getUuid(), true, null, now);
    SourceRow sourceRow =
        createSourceDao()
            .upsertOrDefault(
                UUID.randomUUID(),
                toDefaultSourceType(datasetMeta.getType()),
                now,
                datasetMeta.getSourceName().getValue(),
                "");
    DatasetRow datasetRow;

    if (datasetMeta.getDescription().isPresent()) {
      datasetRow =
          upsert(
              symlinkRow.getUuid(),
              datasetMeta.getType(),
              now,
              namespaceRow.getUuid(),
              namespaceRow.getName(),
              sourceRow.getUuid(),
              sourceRow.getName(),
              datasetName.getValue(),
              datasetMeta.getPhysicalName().getValue(),
              datasetMeta.getDescription().orElse(null),
              false);
    } else {
      datasetRow =
          upsert(
              symlinkRow.getUuid(),
              datasetMeta.getType(),
              now,
              namespaceRow.getUuid(),
              namespaceRow.getName(),
              sourceRow.getUuid(),
              sourceRow.getName(),
              datasetName.getValue(),
              datasetMeta.getPhysicalName().getValue());
    }

    updateDatasetMetric(
        namespaceName, datasetMeta.getType(), symlinkRow.getUuid(), datasetRow.getUuid());

    TagDao tagDao = createTagDao();
    List<DatasetTagMapping> datasetTagMappings = new ArrayList<>();
    for (TagName tagName : datasetMeta.getTags()) {
      TagRow tag = tagDao.upsert(UUID.randomUUID(), now, tagName.getValue());
      datasetTagMappings.add(new DatasetTagMapping(datasetRow.getUuid(), tag.getUuid(), now));
    }
    updateTagMapping(datasetTagMappings);

    DatasetVersionRow dvRow =
        createDatasetVersionDao()
            .upsertDatasetVersion(
                datasetRow,
                now,
                namespaceName.getValue(),
                datasetName.getValue(),
                null,
                datasetMeta);

    return findWithTags(namespaceName.getValue(), datasetName.getValue()).get();
  }

  default String toDefaultSourceType(DatasetType type) {
    return "POSTGRES";
  }

  default void updateDatasetMetric(
      NamespaceName namespaceName,
      DatasetType datasetType,
      UUID newDatasetUuid,
      UUID currentDatasetUuid) {
    if (newDatasetUuid != currentDatasetUuid) {
      DatasetService.datasets.labels(namespaceName.getValue(), datasetType.toString()).inc();
    }
  }

  default Dataset updateTags(String namespaceName, String datasetName, String tagName) {
    Instant now = Instant.now();
    DatasetRow datasetRow = findDatasetAsRow(namespaceName, datasetName).get();
    TagRow tagRow = createTagDao().upsert(UUID.randomUUID(), now, tagName);
    updateTagMapping(
        ImmutableList.of(new DatasetTagMapping(datasetRow.getUuid(), tagRow.getUuid(), now)));
    return findDatasetByName(namespaceName, datasetName).get();
  }

  @Value
  class DatasetTagMapping {
    UUID rowUuid;
    UUID tagUuid;
    Instant taggedAt;
  }

  @Value
  class DatasetCurrentVersionUpdate {
    @NonNull UUID rowUuid;
    @NonNull Instant updatedAt;
    @NonNull UUID currentVersionUuid;
  }

  @Value
  class DatasetUpsert {
    @NonNull UUID uuid;
    @NonNull DatasetType type;
    @NonNull Instant now;
    @NonNull UUID namespaceUuid;
    @NonNull String namespaceName;
    @NonNull UUID sourceUuid;
    @NonNull String sourceName;
    @NonNull String name;
    @NonNull String physicalName;
    @Nullable String description;
    boolean deleted;
  }
}
