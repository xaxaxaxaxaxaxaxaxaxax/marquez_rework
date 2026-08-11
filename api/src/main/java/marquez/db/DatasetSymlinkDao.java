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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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
  int MAX_PRIMARY_SYMLINKS_PER_RESOLVE = 1000;

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
          WITH requested(dataset_uuid, name, namespace_uuid, created_at) AS (
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
              true,
              NULL,
              CAST(requested.created_at AS timestamptz),
              CAST(requested.created_at AS timestamptz)
          FROM requested
          ORDER BY
              CAST(requested.namespace_uuid AS uuid),
              CAST(requested.name AS varchar)
          ON CONFLICT (name, namespace_uuid) DO NOTHING
          RETURNING *
          """)
  List<DatasetSymlinkRow> insertPrimarySymlinksChunk(
      @BindBeanList(
              value = "values",
              propertyNames = {"uuid", "name", "namespaceUuid", "now"})
          List<PrimaryDatasetSymlinkUpsert> symlinks);

  /**
   * Resolves primary symlinks without opening a transaction. The caller must provide one outer
   * transaction so every chunk and concurrent-winner readback is atomic with the projection.
   */
  default List<DatasetSymlinkRow> resolvePrimarySymlinksInTransaction(
      List<PrimaryDatasetSymlinkUpsert> symlinks) {
    if (symlinks.isEmpty()) {
      return Collections.emptyList();
    }

    List<PrimaryDatasetSymlinkUpsert> inputs = List.copyOf(symlinks);
    Map<DatasetSymlinkIdentity, PrimaryDatasetSymlinkUpsert> firstWriteByIdentity =
        new LinkedHashMap<>();
    for (PrimaryDatasetSymlinkUpsert symlink : inputs) {
      firstWriteByIdentity.putIfAbsent(DatasetSymlinkIdentity.of(symlink), symlink);
    }

    List<PrimaryDatasetSymlinkUpsert> writes = new ArrayList<>(firstWriteByIdentity.values());
    writes.sort(DatasetSymlinkDao::compareForWrite);

    Map<DatasetSymlinkIdentity, DatasetSymlinkRow> resolved = new HashMap<>();
    findExistingSymlinks(writes, resolved);

    List<PrimaryDatasetSymlinkUpsert> missing = new ArrayList<>();
    for (PrimaryDatasetSymlinkUpsert write : writes) {
      if (!resolved.containsKey(DatasetSymlinkIdentity.of(write))) {
        missing.add(write);
      }
    }

    for (int start = 0; start < missing.size(); start += MAX_PRIMARY_SYMLINKS_PER_RESOLVE) {
      int end = Math.min(start + MAX_PRIMARY_SYMLINKS_PER_RESOLVE, missing.size());
      addResolvedRows(resolved, insertPrimarySymlinksChunk(missing.subList(start, end)));
    }

    // ON CONFLICT may have waited for a concurrent insert that was invisible to the first read.
    // Read only those winners now that a new READ COMMITTED statement can observe them.
    List<PrimaryDatasetSymlinkUpsert> unresolved = new ArrayList<>();
    for (PrimaryDatasetSymlinkUpsert write : missing) {
      if (!resolved.containsKey(DatasetSymlinkIdentity.of(write))) {
        unresolved.add(write);
      }
    }
    findExistingSymlinks(unresolved, resolved);

    List<DatasetSymlinkRow> ordered = new ArrayList<>(inputs.size());
    for (PrimaryDatasetSymlinkUpsert input : inputs) {
      DatasetSymlinkIdentity identity = DatasetSymlinkIdentity.of(input);
      DatasetSymlinkRow row = resolved.get(identity);
      if (row == null) {
        throw new IllegalStateException(
            "Primary dataset symlink disappeared after a concurrent insert: "
                + input.getNamespaceUuid()
                + "/"
                + input.getName());
      }
      ordered.add(row);
    }
    return ordered;
  }

  private void findExistingSymlinks(
      List<PrimaryDatasetSymlinkUpsert> requested,
      Map<DatasetSymlinkIdentity, DatasetSymlinkRow> resolved) {
    for (int start = 0; start < requested.size(); start += MAX_PRIMARY_SYMLINKS_PER_RESOLVE) {
      int end = Math.min(start + MAX_PRIMARY_SYMLINKS_PER_RESOLVE, requested.size());
      UUID[] namespaceUuids = new UUID[end - start];
      String[] names = new String[end - start];
      for (int index = start; index < end; index++) {
        PrimaryDatasetSymlinkUpsert symlink = requested.get(index);
        int chunkIndex = index - start;
        namespaceUuids[chunkIndex] = symlink.getNamespaceUuid();
        names[chunkIndex] = symlink.getName();
      }
      addResolvedRows(resolved, findDatasetSymlinksByKeys(namespaceUuids, names));
    }
  }

  private static void addResolvedRows(
      Map<DatasetSymlinkIdentity, DatasetSymlinkRow> resolved, List<DatasetSymlinkRow> rows) {
    for (DatasetSymlinkRow row : rows) {
      resolved.put(DatasetSymlinkIdentity.of(row), row);
    }
  }

  private static int compareForWrite(
      PrimaryDatasetSymlinkUpsert left, PrimaryDatasetSymlinkUpsert right) {
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

  @Value
  class PrimaryDatasetSymlinkUpsert {
    @NonNull UUID uuid;
    @NonNull String name;
    @NonNull UUID namespaceUuid;
    @NonNull Instant now;
  }

  @Value
  class DatasetSymlinkIdentity {
    @NonNull UUID namespaceUuid;
    @NonNull String name;

    static DatasetSymlinkIdentity of(PrimaryDatasetSymlinkUpsert symlink) {
      return new DatasetSymlinkIdentity(symlink.getNamespaceUuid(), symlink.getName());
    }

    static DatasetSymlinkIdentity of(DatasetSymlinkRow symlink) {
      return new DatasetSymlinkIdentity(symlink.getNamespaceUuid(), symlink.getName());
    }
  }
}
