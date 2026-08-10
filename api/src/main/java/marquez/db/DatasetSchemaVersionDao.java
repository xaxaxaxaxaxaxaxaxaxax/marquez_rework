/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import marquez.common.Utils;
import marquez.common.models.Version;
import marquez.db.mappers.DatasetSchemaVersionRowMapper;
import marquez.db.models.DatasetFieldRow;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetSchemaVersionRow;
import org.apache.commons.lang3.tuple.Pair;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

@RegisterRowMapper(DatasetSchemaVersionRowMapper.class)
public interface DatasetSchemaVersionDao extends BaseDao {
  int MAX_FIELD_MAPPINGS_PER_INSERT = 1000;

  @Transaction
  default Version upsertSchemaVersion(
      DatasetRow datasetRow, List<DatasetFieldRow> datasetFields, Instant now) {
    return upsertSchemaVersionInTransaction(datasetRow, datasetFields, now);
  }

  /** Creates a schema version and its field mappings inside the caller's transaction. */
  default Version upsertSchemaVersionInTransaction(
      DatasetRow datasetRow, List<DatasetFieldRow> datasetFields, Instant now) {
    final Version computedVersion =
        Utils.newDatasetSchemaVersionFor(
            datasetRow.getNamespaceName(),
            datasetRow.getName(),
            datasetFields.stream()
                .map(field -> Pair.of(field.getName(), field.getType()))
                .collect(Collectors.toSet()));
    upsertSchemaVersion(computedVersion.getValue(), datasetRow.getUuid(), now)
        .ifPresent(
            newRow -> {
              // if not null it means a new insert, so we have to do the fields as well
              // if null then it means the version already exists, and so the fields must already
              // exist
              upsertFieldMappingsInTransaction(
                  newRow.getUuid(),
                  datasetFields.stream()
                      .map(DatasetFieldRow::getUuid)
                      .collect(Collectors.toList()));
            });
    return computedVersion;
  }

  @SqlQuery(
      "INSERT INTO dataset_schema_versions "
          + "(uuid, dataset_uuid, created_at) "
          + "VALUES (:uuid, :datasetUuid, :now) "
          + "ON CONFLICT DO NOTHING "
          + "RETURNING *")
  Optional<DatasetSchemaVersionRow> upsertSchemaVersion(UUID uuid, UUID datasetUuid, Instant now);

  @SqlUpdate(
      """
          INSERT INTO dataset_schema_versions_field_mapping (
              dataset_schema_version_uuid,
              dataset_field_uuid
          )
          SELECT DISTINCT :schemaVersionUuid, mapping.dataset_field_uuid
          FROM unnest(CAST(:datasetFieldUuids AS uuid[]))
              AS mapping(dataset_field_uuid)
          ORDER BY mapping.dataset_field_uuid
          ON CONFLICT DO NOTHING
          """)
  void insertFieldMappingsChunk(
      @Bind("schemaVersionUuid") UUID schemaVersionUuid,
      @Bind("datasetFieldUuids") UUID[] datasetFieldUuids);

  @Transaction
  default void upsertFieldMappings(UUID schemaVersionUuid, Iterable<UUID> fieldUuids) {
    upsertFieldMappingsInTransaction(schemaVersionUuid, fieldUuids);
  }

  /** Inserts schema field mappings inside the caller's transaction. */
  default void upsertFieldMappingsInTransaction(UUID schemaVersionUuid, Iterable<UUID> fieldUuids) {
    Objects.requireNonNull(schemaVersionUuid, "schemaVersionUuid");
    Iterator<UUID> iterator = Objects.requireNonNull(fieldUuids, "fieldUuids").iterator();
    List<UUID> chunk = new ArrayList<>(MAX_FIELD_MAPPINGS_PER_INSERT);
    while (iterator.hasNext()) {
      chunk.add(Objects.requireNonNull(iterator.next(), "fieldUuids contains null"));
      if (chunk.size() == MAX_FIELD_MAPPINGS_PER_INSERT) {
        insertFieldMappingsChunk(schemaVersionUuid, chunk.toArray(UUID[]::new));
        chunk.clear();
      }
    }
    if (!chunk.isEmpty()) {
      insertFieldMappingsChunk(schemaVersionUuid, chunk.toArray(UUID[]::new));
    }
  }
}
