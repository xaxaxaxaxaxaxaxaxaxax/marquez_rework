/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("DataAccessTests")
@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class OpenLineageQueueStorageConfigTest {
  private static Jdbi jdbi;

  @BeforeAll
  static void setUp(Jdbi configuredJdbi) {
    jdbi = configuredJdbi;
  }

  @Test
  void queueHeapFillfactorAndToastAutovacuumOptionsMatchTheOperationalContract() {
    Map<String, RelationOptions> actual = new LinkedHashMap<>();
    jdbi.useHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT heap.relname,
                           heap.reloptions AS heap_options,
                           toast.reloptions AS toast_options
                    FROM pg_class AS heap
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = heap.relnamespace
                    JOIN pg_class AS toast
                      ON toast.oid = heap.reltoastrelid
                    WHERE namespace.nspname = current_schema()
                      AND heap.relname IN (
                        'open_lineage_queue',
                        'open_lineage_queue_heads',
                        'open_lineage_dead_letters')
                    ORDER BY heap.relname
                    """)
                .map(
                    (resultSet, context) ->
                        Map.entry(
                            resultSet.getString("relname"),
                            new RelationOptions(
                                options(resultSet.getArray("heap_options")),
                                options(resultSet.getArray("toast_options")))))
                .forEach(entry -> actual.put(entry.getKey(), entry.getValue())));

    Set<String> payloadOptions =
        Set.of("autovacuum_vacuum_threshold=5000", "autovacuum_vacuum_scale_factor=0.02");
    Set<String> headOptions =
        Set.of(
            "fillfactor=80",
            "autovacuum_vacuum_threshold=1000",
            "autovacuum_vacuum_scale_factor=0.02");

    assertThat(actual)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "open_lineage_queue",
                new RelationOptions(payloadOptions, toastOptions(5000)),
                "open_lineage_queue_heads",
                new RelationOptions(headOptions, toastOptions(1000)),
                "open_lineage_dead_letters",
                new RelationOptions(payloadOptions, toastOptions(5000))));
  }

  @Test
  void headSchemaIsCompactAndSchedulingStateStaysNonindexed() {
    Map<String, ColumnContract> columns = new LinkedHashMap<>();
    jdbi.useHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT column_name,
                           data_type,
                           is_nullable,
                           column_default
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'open_lineage_queue_heads'
                    ORDER BY ordinal_position
                    """)
                .map(
                    (resultSet, context) ->
                        Map.entry(
                            resultSet.getString("column_name"),
                            new ColumnContract(
                                resultSet.getString("data_type"),
                                resultSet.getString("is_nullable"),
                                resultSet.getString("column_default"))))
                .forEach(entry -> columns.put(entry.getKey(), entry.getValue())));
    Map<String, IndexContract> indexesByName = new LinkedHashMap<>();
    jdbi.useHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT index_relation.relname AS index_name,
                           pg_get_indexdef(indexes.indexrelid) AS definition,
                           indexes.indnatts AS total_columns,
                           ARRAY(
                             SELECT pg_get_indexdef(indexes.indexrelid, key_position, TRUE)
                             FROM generate_series(1, indexes.indnkeyatts)
                               AS positions(key_position)
                             ORDER BY key_position
                           ) AS key_columns
                    FROM pg_index AS indexes
                    JOIN pg_class AS table_relation
                      ON table_relation.oid = indexes.indrelid
                    JOIN pg_namespace AS namespace
                      ON namespace.oid = table_relation.relnamespace
                    JOIN pg_class AS index_relation
                      ON index_relation.oid = indexes.indexrelid
                    WHERE namespace.nspname = current_schema()
                      AND table_relation.relname = 'open_lineage_queue_heads'
                    ORDER BY index_relation.relname
                    """)
                .map(
                    (resultSet, context) ->
                        Map.entry(
                            resultSet.getString("index_name"),
                            new IndexContract(
                                resultSet.getString("definition"),
                                resultSet.getInt("total_columns"),
                                options(resultSet.getArray("key_columns")))))
                .forEach(entry -> indexesByName.put(entry.getKey(), entry.getValue())));

    assertThat(columns.keySet())
        .containsExactly(
            "ordering_key",
            "event_id",
            "available_at",
            "attempt_count",
            "refresh_due_on_advance",
            "last_error");
    assertThat(columns.get("ordering_key")).isEqualTo(new ColumnContract("uuid", "NO", null));
    assertThat(columns.get("event_id")).isEqualTo(new ColumnContract("bigint", "NO", null));
    assertThat(columns.get("available_at").dataType()).isEqualTo("timestamp with time zone");
    assertThat(columns.get("available_at").nullable()).isEqualTo("NO");
    assertThat(columns.get("available_at").defaultExpression())
        .contains("date_trunc", "clock_timestamp");
    assertThat(columns.get("attempt_count")).isEqualTo(new ColumnContract("integer", "NO", "0"));
    assertThat(columns.get("refresh_due_on_advance"))
        .isEqualTo(new ColumnContract("boolean", "NO", "false"));
    assertThat(columns.get("last_error")).isEqualTo(new ColumnContract("text", "YES", null));
    assertThat(indexesByName.get("open_lineage_queue_heads_due_idx").keyColumns())
        .containsExactly("available_at");
    assertThat(indexesByName.get("open_lineage_queue_heads_due_idx").totalColumns()).isEqualTo(1);
    assertThat(indexesByName.values())
        .allSatisfy(
            index ->
                assertThat(index.definition())
                    .doesNotContain("refresh_due_on_advance", "attempt_count", "last_error"));
  }

  @Test
  void queueIdentitySequenceDoesNotCacheIdsAcrossConnections() {
    long cacheSize =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT sequence.seqcache
                        FROM pg_sequence AS sequence
                        WHERE sequence.seqrelid =
                          to_regclass(
                            pg_get_serial_sequence(
                              current_schema() || '.open_lineage_queue',
                              'id'))
                        """)
                    .mapTo(Long.class)
                    .one());

    // The lane advisory lock is acquired before nextval. A larger cache could let two pooled
    // connections commit same-lane IDs out of advisory-lock order.
    assertThat(cacheSize).isEqualTo(1);
  }

  private static Set<String> toastOptions(int threshold) {
    return Set.of(
        "autovacuum_vacuum_threshold=" + threshold, "autovacuum_vacuum_scale_factor=0.02");
  }

  private static Set<String> options(Array options) throws SQLException {
    return Arrays.stream((Object[]) options.getArray())
        .map(Object::toString)
        .collect(Collectors.toUnmodifiableSet());
  }

  private record RelationOptions(Set<String> heap, Set<String> toast) {}

  private record ColumnContract(String dataType, String nullable, String defaultExpression) {}

  private record IndexContract(String definition, int totalColumns, Set<String> keyColumns) {}
}
