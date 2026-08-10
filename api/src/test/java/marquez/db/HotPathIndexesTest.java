/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class HotPathIndexesTest {
  private static final int DESC = 1;
  private static final int NULLS_FIRST = 2;

  private static final String CURRENT_PREDICATE = "is_current_job_version IS TRUE";
  private static final String CURRENT_SYMLINK_PREDICATE =
      "is_current_job_version IS TRUE AND job_symlink_target_uuid IS NOT NULL";
  private static final String VISIBLE_JOB_PREDICATE =
      "is_hidden IS FALSE AND symlink_target_uuid IS NULL";
  private static final String SYMLINK_TARGET_PREDICATE = "symlink_target_uuid IS NOT NULL";

  private static final List<String> ABSENT_INDEX_NAMES =
      List.of(
          "run_facets_run_uuid_event_time_idx",
          "job_facets_run_job_event_time_idx",
          "job_facets_job_version_event_time_idx",
          "dataset_facets_version_event_time_idx",
          "dataset_facets_dataset_version_event_time_idx",
          "dataset_facets_run_created_at_idx",
          "runs_job_uuid_started_at_uuid_idx",
          "column_lineage_latest_pair_idx",
          "jobs_symlink_target_uuid_index",
          "datasetversion_datasetid_idx",
          "job_facets_job_uuid_index",
          "runs_created_at_index");

  private static final Map<String, ExpectedIndex> EXPECTED_INDEXES =
      Map.ofEntries(
          Map.entry(
              "job_versions_io_mapping_current_job_idx",
              new ExpectedIndex(
                  List.of("job_uuid", "io_type"),
                  List.of("dataset_uuid"),
                  CURRENT_PREDICATE,
                  List.of(0, 0))),
          Map.entry(
              "job_versions_io_mapping_current_symlink_idx",
              new ExpectedIndex(
                  List.of("job_symlink_target_uuid", "io_type"),
                  List.of("dataset_uuid"),
                  CURRENT_SYMLINK_PREDICATE,
                  List.of(0, 0))),
          Map.entry(
              "job_versions_io_mapping_current_dataset_idx",
              new ExpectedIndex(
                  List.of("dataset_uuid"),
                  List.of("job_uuid", "job_symlink_target_uuid"),
                  CURRENT_PREDICATE,
                  List.of(0))),
          Map.entry(
              "job_versions_io_mapping_dataset_io_idx",
              new ExpectedIndex(
                  List.of("dataset_uuid", "io_type"), List.of(), null, List.of(0, 0))),
          Map.entry(
              "jobs_visible_updated_at_uuid_idx",
              new ExpectedIndex(
                  List.of("updated_at", "uuid"),
                  List.of(),
                  VISIBLE_JOB_PREDICATE,
                  List.of(DESC | NULLS_FIRST, DESC | NULLS_FIRST))),
          Map.entry(
              "jobs_visible_namespace_updated_at_uuid_idx",
              new ExpectedIndex(
                  List.of("namespace_uuid", "updated_at", "uuid"),
                  List.of(),
                  VISIBLE_JOB_PREDICATE,
                  List.of(0, DESC | NULLS_FIRST, DESC | NULLS_FIRST))),
          Map.entry(
              "jobs_tag_mapping_job_uuid_idx",
              new ExpectedIndex(List.of("job_uuid"), List.of("tag_uuid"), null, List.of(0))),
          Map.entry(
              "jobs_symlinks",
              new ExpectedIndex(
                  List.of("symlink_target_uuid"),
                  List.of("uuid", "namespace_name", "simple_name"),
                  SYMLINK_TARGET_PREDICATE,
                  List.of(0))),
          Map.entry(
              "dataset_versions_dataset_uuid_version_key",
              new ExpectedIndex(
                  List.of("dataset_uuid", "version"), List.of(), null, List.of(0, 0))),
          Map.entry(
              "job_facets_job_uuid_run_uuid_idx",
              new ExpectedIndex(List.of("job_uuid", "run_uuid"), List.of(), null, List.of(0, 0))),
          Map.entry(
              "runs_created_at_current_run_state_index",
              new ExpectedIndex(
                  List.of("created_at", "current_run_state"),
                  List.of(),
                  null,
                  List.of(DESC | NULLS_FIRST, 0))));

  private static Jdbi jdbi;

  @BeforeAll
  static void setUpOnce(Jdbi jdbi) {
    HotPathIndexesTest.jdbi = jdbi;
  }

  @Test
  void hotPathIndexesHaveExpectedCatalogDefinitions() {
    List<String> inspectedIndexNames =
        EXPECTED_INDEXES.keySet().stream().collect(Collectors.toList());
    inspectedIndexNames.addAll(ABSENT_INDEX_NAMES);

    Map<String, IndexMetadata> actualIndexes =
        jdbi
            .withHandle(
                handle ->
                    handle
                        .createQuery(
                            """
                        SELECT index_class.relname AS index_name,
                               access_method.amname AS access_method,
                               index_metadata.indisvalid,
                               index_metadata.indisready,
                               index_metadata.indnkeyatts,
                               pg_get_expr(index_metadata.indpred, index_metadata.indrelid) AS predicate,
                               ARRAY(
                                 SELECT attribute.attname
                                 FROM unnest(index_metadata.indkey::smallint[]) WITH ORDINALITY
                                      AS index_column(attribute_number, position_number)
                                 INNER JOIN pg_attribute AS attribute
                                   ON attribute.attrelid = index_metadata.indrelid
                                  AND attribute.attnum = index_column.attribute_number
                                 ORDER BY index_column.position_number
                               )::text[] AS columns,
                               index_metadata.indoption::smallint[] AS ordering_options
                        FROM pg_index AS index_metadata
                        INNER JOIN pg_class AS index_class
                          ON index_class.oid = index_metadata.indexrelid
                        INNER JOIN pg_namespace AS index_namespace
                          ON index_namespace.oid = index_class.relnamespace
                        INNER JOIN pg_am AS access_method
                          ON access_method.oid = index_class.relam
                        WHERE index_namespace.nspname = current_schema()
                          AND index_class.relname IN (<indexNames>)
                        """)
                        .bindList("indexNames", inspectedIndexNames)
                        .map(
                            (resultSet, context) ->
                                new IndexMetadata(
                                    resultSet.getString("index_name"),
                                    resultSet.getString("access_method"),
                                    resultSet.getBoolean("indisvalid"),
                                    resultSet.getBoolean("indisready"),
                                    resultSet.getInt("indnkeyatts"),
                                    readStringArray(resultSet, "columns"),
                                    normalizePredicate(resultSet.getString("predicate")),
                                    readIntegerArray(resultSet, "ordering_options")))
                        .list())
            .stream()
            .collect(Collectors.toMap(IndexMetadata::name, metadata -> metadata));

    assertThat(actualIndexes.keySet())
        .as("indexes expected to be absent after hot-path migrations")
        .doesNotContainAnyElementsOf(ABSENT_INDEX_NAMES);
    assertThat(actualIndexes.keySet())
        .containsExactlyInAnyOrderElementsOf(EXPECTED_INDEXES.keySet());

    EXPECTED_INDEXES.forEach(
        (indexName, expected) -> {
          IndexMetadata actual = actualIndexes.get(indexName);

          assertThat(actual.accessMethod()).as("%s access method", indexName).isEqualTo("btree");
          assertThat(actual.valid()).as("%s validity", indexName).isTrue();
          assertThat(actual.ready()).as("%s readiness", indexName).isTrue();
          assertThat(actual.keyColumns())
              .as("%s key columns", indexName)
              .containsExactlyElementsOf(expected.keyColumns());
          assertThat(actual.includedColumns())
              .as("%s included columns", indexName)
              .containsExactlyElementsOf(expected.includedColumns());
          assertThat(actual.predicate())
              .as("%s partial predicate", indexName)
              .isEqualTo(expected.predicate());
          assertThat(actual.orderingOptions())
              .as("%s ordering flags", indexName)
              .containsExactlyElementsOf(expected.orderingOptions());
        });
  }

  private static List<String> readStringArray(ResultSet resultSet, String column)
      throws SQLException {
    return Arrays.stream((Object[]) resultSet.getArray(column).getArray())
        .map(Object::toString)
        .collect(Collectors.toList());
  }

  private static List<Integer> readIntegerArray(ResultSet resultSet, String column)
      throws SQLException {
    return Arrays.stream((Object[]) resultSet.getArray(column).getArray())
        .map(value -> ((Number) value).intValue())
        .collect(Collectors.toList());
  }

  private static String normalizePredicate(String predicate) {
    if (predicate == null) {
      return null;
    }
    return predicate.replace("(", "").replace(")", "").replaceAll("\\s+", " ").trim();
  }

  private record ExpectedIndex(
      List<String> keyColumns,
      List<String> includedColumns,
      String predicate,
      List<Integer> orderingOptions) {}

  private record IndexMetadata(
      String name,
      String accessMethod,
      boolean valid,
      boolean ready,
      int keyColumnCount,
      List<String> columns,
      String predicate,
      List<Integer> orderingOptions) {
    List<String> keyColumns() {
      return columns.subList(0, keyColumnCount);
    }

    List<String> includedColumns() {
      return columns.subList(keyColumnCount, columns.size());
    }
  }
}
