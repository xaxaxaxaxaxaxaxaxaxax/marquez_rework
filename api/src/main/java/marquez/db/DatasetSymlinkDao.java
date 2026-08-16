/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.Value;
import marquez.db.mappers.DatasetSymlinksRowMapper;
import marquez.db.models.DatasetSymlinkRow;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBeanList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@RegisterRowMapper(DatasetSymlinksRowMapper.class)
public interface DatasetSymlinkDao extends BaseDao {
  int MAX_SYMLINKS_PER_RESOLVE = 1000;

  default DatasetSymlinkRow upsertDatasetSymlinkRow(
      UUID uuid, String name, UUID namespaceUuid, boolean isPrimary, String type, Instant now) {
    doUpsertDatasetSymlinkRow(uuid, name, namespaceUuid, isPrimary, type, now);
    return findDatasetSymlinkByNamespaceUuidAndName(namespaceUuid, name).orElseThrow();
  }

  @SqlQuery("SELECT * FROM dataset_symlinks WHERE namespace_uuid = :namespaceUuid and name = :name")
  Optional<DatasetSymlinkRow> findDatasetSymlinkByNamespaceUuidAndName(
      UUID namespaceUuid, String name);

  @SqlQuery(
      """
          SELECT symlink.*
          FROM dataset_symlinks AS symlink
          INNER JOIN unnest(
              CAST(:namespaceUuids AS uuid[]),
              CAST(:names AS varchar[])
          ) AS requested(namespace_uuid, name)
              ON requested.namespace_uuid = symlink.namespace_uuid
             AND requested.name = symlink.name
          """)
  List<DatasetSymlinkRow> findDatasetSymlinksByKeys(
      @Bind("namespaceUuids") UUID[] namespaceUuids, @Bind("names") String[] names);

  @SqlQuery(
      """
          WITH requested(dataset_uuid, name, namespace_uuid, is_primary, type, created_at) AS (
              VALUES <values>
          )
          INSERT INTO dataset_symlinks (
              dataset_uuid,
              name,
              namespace_uuid,
              is_primary,
              type,
              created_at,
              updated_at
          )
          SELECT
              CAST(requested.dataset_uuid AS uuid),
              CAST(requested.name AS varchar),
              CAST(requested.namespace_uuid AS uuid),
              CAST(requested.is_primary AS boolean),
              CAST(requested.type AS varchar),
              CAST(requested.created_at AS timestamptz),
              CAST(requested.created_at AS timestamptz)
          FROM requested
          ORDER BY
              CAST(requested.namespace_uuid AS uuid),
              CAST(requested.name AS varchar)
          ON CONFLICT (name, namespace_uuid) DO NOTHING
          RETURNING *
          """)
  List<DatasetSymlinkRow> insertPlannedSymlinksChunk(
      @BindBeanList(
              value = "values",
              propertyNames = {"uuid", "name", "namespaceUuid", "primary", "type", "now"})
          List<PlannedDatasetSymlinkUpsert> symlinks);

  /** Finds a bounded set of symlink identities without opening or nesting a transaction. */
  default List<DatasetSymlinkRow> findDatasetSymlinksByKeysInTransaction(
      List<DatasetSymlinkKey> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyList();
    }

    List<DatasetSymlinkKey> unique = new ArrayList<>(new LinkedHashSet<>(List.copyOf(keys)));
    List<DatasetSymlinkRow> rows = new ArrayList<>();
    for (int start = 0; start < unique.size(); start += MAX_SYMLINKS_PER_RESOLVE) {
      int end = Math.min(start + MAX_SYMLINKS_PER_RESOLVE, unique.size());
      UUID[] namespaceUuids = new UUID[end - start];
      String[] names = new String[end - start];
      for (int index = start; index < end; index++) {
        DatasetSymlinkKey key = unique.get(index);
        namespaceUuids[index - start] = key.getNamespaceUuid();
        names[index - start] = key.getName();
      }
      rows.addAll(findDatasetSymlinksByKeys(namespaceUuids, names));
    }
    return rows;
  }

  /**
   * Materializes a prevalidated alias plan and maps every requested identity back to its persisted
   * row. A concurrent conflicting winner is returned to the caller for final validation.
   */
  default List<DatasetSymlinkRow> resolvePlannedSymlinksInTransaction(
      List<PlannedDatasetSymlinkUpsert> requested) {
    if (requested.isEmpty()) {
      return Collections.emptyList();
    }

    List<PlannedDatasetSymlinkUpsert> inputs = List.copyOf(requested);
    Map<DatasetSymlinkIdentity, PlannedDatasetSymlinkUpsert> firstWriteByIdentity =
        new LinkedHashMap<>();
    for (PlannedDatasetSymlinkUpsert input : inputs) {
      firstWriteByIdentity.putIfAbsent(DatasetSymlinkIdentity.of(input), input);
    }
    List<PlannedDatasetSymlinkUpsert> writes = new ArrayList<>(firstWriteByIdentity.values());
    writes.sort(DatasetSymlinkDao::compareForWrite);

    Map<DatasetSymlinkIdentity, DatasetSymlinkRow> resolved = new HashMap<>();
    addResolvedRows(resolved, findDatasetSymlinksByKeysInTransaction(toKeys(writes)));

    List<PlannedDatasetSymlinkUpsert> missing = unresolved(writes, resolved);
    for (int start = 0; start < missing.size(); start += MAX_SYMLINKS_PER_RESOLVE) {
      int end = Math.min(start + MAX_SYMLINKS_PER_RESOLVE, missing.size());
      addResolvedRows(resolved, insertPlannedSymlinksChunk(missing.subList(start, end)));
    }

    // ON CONFLICT may have waited for a concurrent insert invisible to the first read.
    addResolvedRows(
        resolved, findDatasetSymlinksByKeysInTransaction(toKeys(unresolved(missing, resolved))));

    List<DatasetSymlinkRow> ordered = new ArrayList<>(inputs.size());
    for (PlannedDatasetSymlinkUpsert input : inputs) {
      DatasetSymlinkIdentity identity = DatasetSymlinkIdentity.of(input);
      DatasetSymlinkRow row = resolved.get(identity);
      if (row == null) {
        throw new IllegalStateException(
            "Planned dataset symlink disappeared after a concurrent insert: "
                + identity.getNamespaceUuid()
                + "/"
                + identity.getName());
      }
      ordered.add(row);
    }
    return ordered;
  }

  private static List<PlannedDatasetSymlinkUpsert> unresolved(
      List<PlannedDatasetSymlinkUpsert> requested,
      Map<DatasetSymlinkIdentity, DatasetSymlinkRow> resolved) {
    return requested.stream()
        .filter(input -> !resolved.containsKey(DatasetSymlinkIdentity.of(input)))
        .toList();
  }

  private static List<DatasetSymlinkKey> toKeys(List<PlannedDatasetSymlinkUpsert> requested) {
    return requested.stream()
        .map(input -> new DatasetSymlinkKey(input.getNamespaceUuid(), input.getName()))
        .toList();
  }

  private static int compareForWrite(
      PlannedDatasetSymlinkUpsert left, PlannedDatasetSymlinkUpsert right) {
    int compared = compareUuidLikePostgres(left.getNamespaceUuid(), right.getNamespaceUuid());
    return compared != 0 ? compared : left.getName().compareTo(right.getName());
  }

  private static int compareUuidLikePostgres(UUID left, UUID right) {
    int compared =
        Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
    return compared != 0
        ? compared
        : Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
  }

  private static void addResolvedRows(
      Map<DatasetSymlinkIdentity, DatasetSymlinkRow> resolved, List<DatasetSymlinkRow> rows) {
    rows.forEach(row -> resolved.put(DatasetSymlinkIdentity.of(row), row));
  }

  @SqlUpdate(
      """
          INSERT INTO dataset_symlinks (
          dataset_uuid,
          name,
          namespace_uuid,
          is_primary,
          type,
          created_at,
          updated_at
          ) VALUES (
          :uuid,
          :name,
          :namespaceUuid,
          :isPrimary,
          :type,
          :now,
          :now)
          ON CONFLICT (name, namespace_uuid) DO NOTHING""")
  void doUpsertDatasetSymlinkRow(
      UUID uuid, String name, UUID namespaceUuid, boolean isPrimary, String type, Instant now);

  /** Raw OpenLineage namespace alias whose canonical target is the sanitized primary identity. */
  @SqlUpdate(
      """
          INSERT INTO dataset_symlinks (
              dataset_uuid, name, namespace_uuid, is_primary, type, created_at, updated_at)
          VALUES (:datasetUuid, :name, :namespaceUuid, false, NULL, :now, :now)
          ON CONFLICT (name, namespace_uuid) DO UPDATE
          SET dataset_uuid = EXCLUDED.dataset_uuid,
              is_primary = false,
              type = NULL,
              updated_at = GREATEST(dataset_symlinks.updated_at, EXCLUDED.updated_at)
          WHERE dataset_symlinks.dataset_uuid = EXCLUDED.dataset_uuid
             OR dataset_symlinks.is_primary IS NOT TRUE
          """)
  int upsertOpenLineageRawAlias(UUID datasetUuid, String name, UUID namespaceUuid, Instant now);

  @Value
  class PlannedDatasetSymlinkUpsert {
    @NonNull UUID uuid;
    @NonNull String name;
    @NonNull UUID namespaceUuid;
    boolean primary;
    @Nullable String type;
    @NonNull Instant now;
  }

  @Value
  class DatasetSymlinkKey {
    @NonNull UUID namespaceUuid;
    @NonNull String name;
  }

  @Value
  class DatasetSymlinkIdentity {
    @NonNull UUID namespaceUuid;
    @NonNull String name;

    static DatasetSymlinkIdentity of(DatasetSymlinkRow symlink) {
      return new DatasetSymlinkIdentity(symlink.getNamespaceUuid(), symlink.getName());
    }

    static DatasetSymlinkIdentity of(PlannedDatasetSymlinkUpsert symlink) {
      return new DatasetSymlinkIdentity(symlink.getNamespaceUuid(), symlink.getName());
    }
  }
}
