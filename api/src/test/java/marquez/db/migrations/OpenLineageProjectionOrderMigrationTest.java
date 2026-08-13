/*
 * Copyright 2018-2022 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.migrations;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("IntegrationTests")
@Testcontainers
class OpenLineageProjectionOrderMigrationTest {
  private static final String SCHEMA = "open_lineage_projection_order";
  private static final String[] MIGRATION_LOCATIONS = {
    "classpath:marquez/db/migration", "classpath:marquez/db/migrations"
  };
  private static final List<String> CONSTRAINT_NAMES =
      List.of(
          "jobs_open_lineage_snapshot_order_pair",
          "jobs_open_lineage_current_run_order_pair",
          "jobs_open_lineage_current_version_order_pair",
          "datasets_open_lineage_snapshot_order_pair",
          "datasets_open_lineage_current_version_order_pair",
          "job_versions_open_lineage_latest_run_order_pair");
  private static final List<String> COLUMN_NAMES =
      List.of(
          "jobs.open_lineage_snapshot_time",
          "jobs.open_lineage_snapshot_key",
          "jobs.open_lineage_current_run_time",
          "jobs.open_lineage_current_run_key",
          "jobs.open_lineage_current_version_time",
          "jobs.open_lineage_current_version_key",
          "datasets.open_lineage_snapshot_time",
          "datasets.open_lineage_snapshot_key",
          "datasets.open_lineage_current_version_time",
          "datasets.open_lineage_current_version_key",
          "job_versions.open_lineage_latest_run_time",
          "job_versions.open_lineage_latest_run_key",
          "runs.open_lineage_parent_placeholder");

  @Container
  private static final PostgreSQLContainer<?> DB_CONTAINER =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:14"));

  @Test
  void stagesColumnsBackfillAndValidationInSeparateMigrations() throws Exception {
    migrateTo("77");
    Jdbi jdbi =
        Jdbi.create(
            DB_CONTAINER.getJdbcUrl(), DB_CONTAINER.getUsername(), DB_CONTAINER.getPassword());
    Fixture fixture = seedV77Fixture(jdbi);
    assertThat(openLineageColumns(jdbi)).doesNotContain("runs.open_lineage_parent_placeholder");

    migrateTo("78");
    assertThat(openLineageColumns(jdbi)).containsExactlyInAnyOrderElementsOf(COLUMN_NAMES);
    assertThat(columnDefinition(jdbi, "runs", "open_lineage_parent_placeholder"))
        .isEqualTo(new ColumnDefinition("boolean", true, null));
    assertThat(parentPlaceholder(jdbi, fixture.preexistingRealRunUuid())).isNull();
    assertThat(parentPlaceholder(jdbi, fixture.preexistingSyntheticLikeRunUuid())).isNull();
    assertConstraintValidation(jdbi, false);
    assertThat(updatedAt(jdbi, "jobs", fixture.hiddenJobUuid())).isEqualTo(fixture.oldTime());
    assertThat(updatedAt(jdbi, "datasets", fixture.hiddenDatasetUuid()))
        .isEqualTo(fixture.oldTime());
    assertThat(symlink(jdbi, fixture.namespaceUuid(), fixture.datasetName()))
        .containsEntry("dataset_uuid", fixture.otherDatasetUuid())
        .containsEntry("is_primary", true);
    assertThat(symlink(jdbi, fixture.aliasNamespaceUuid(), fixture.aliasName()))
        .containsEntry("dataset_uuid", fixture.hiddenDatasetUuid())
        .containsEntry("is_primary", true);

    Instant postLockRepairBaseline = migrateV79BehindOrderedLocks(jdbi, fixture);
    assertConstraintValidation(jdbi, false);
    assertThat(updatedAt(jdbi, "jobs", fixture.hiddenJobUuid()))
        .isAfterOrEqualTo(postLockRepairBaseline);
    assertThat(updatedAt(jdbi, "datasets", fixture.hiddenDatasetUuid()))
        .isAfterOrEqualTo(postLockRepairBaseline);
    assertThat(symlink(jdbi, fixture.namespaceUuid(), fixture.datasetName()))
        .containsEntry("dataset_uuid", fixture.hiddenDatasetUuid())
        .containsEntry("is_primary", true)
        .containsEntry("type", null);
    assertThat(symlink(jdbi, fixture.aliasNamespaceUuid(), fixture.aliasName()))
        .containsEntry("dataset_uuid", fixture.hiddenDatasetUuid())
        .containsEntry("is_primary", false);

    migrateTo("80");
    assertConstraintValidation(jdbi, true);
    List<String> migrationOrder =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT version
                        FROM %s.flyway_schema_history
                        WHERE version IN ('78', '79', '80') AND success
                        ORDER BY installed_rank
                        """
                            .formatted(SCHEMA))
                    .mapTo(String.class)
                    .list());
    assertThat(migrationOrder).containsExactly("78", "79", "80");
    int migrationTransactions =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT count(DISTINCT xmin::text)
                        FROM %s.flyway_schema_history
                        WHERE version IN ('78', '79', '80') AND success
                        """
                            .formatted(SCHEMA))
                    .mapTo(Integer.class)
                    .one());
    assertThat(migrationTransactions).isEqualTo(3);
    int repeatableMigrations =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT count(*)
                        FROM %s.flyway_schema_history
                        WHERE version IS NULL AND type = 'SQL'
                        """
                            .formatted(SCHEMA))
                    .mapTo(Integer.class)
                    .one());
    assertThat(repeatableMigrations).isZero();
  }

  private static Instant migrateV79BehindOrderedLocks(Jdbi jdbi, Fixture fixture) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Handle jobsBlocker = jdbi.open();
        Handle symlinksBlocker = jdbi.open();
        Handle datasetsBlocker = jdbi.open()) {
      jobsBlocker.begin();
      jobsBlocker.execute("LOCK TABLE %s.jobs IN ROW EXCLUSIVE MODE".formatted(SCHEMA));
      symlinksBlocker.begin();
      symlinksBlocker.execute(
          "LOCK TABLE %s.dataset_symlinks IN ACCESS SHARE MODE".formatted(SCHEMA));
      datasetsBlocker.begin();
      datasetsBlocker.execute("LOCK TABLE %s.datasets IN ROW EXCLUSIVE MODE".formatted(SCHEMA));

      Future<?> migration = executor.submit(() -> migrateTo("79"));
      awaitWaitingRelationLock(jdbi, "jobs", "ShareRowExclusiveLock");
      jobsBlocker.rollback();
      awaitWaitingRelationLock(jdbi, "dataset_symlinks", "AccessExclusiveLock");
      symlinksBlocker.rollback();
      awaitWaitingRelationLock(jdbi, "datasets", "ShareRowExclusiveLock");
      assertThat(updatedAt(jdbi, "jobs", fixture.hiddenJobUuid())).isEqualTo(fixture.oldTime());
      assertThat(updatedAt(jdbi, "datasets", fixture.hiddenDatasetUuid()))
          .isEqualTo(fixture.oldTime());
      Instant postLockRepairBaseline = databaseTime(jdbi);
      datasetsBlocker.rollback();
      migration.get(30, TimeUnit.SECONDS);
      return postLockRepairBaseline;
    } finally {
      executor.shutdownNow();
    }
  }

  private static void awaitWaitingRelationLock(Jdbi jdbi, String relationName, String lockMode) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    boolean waiting;
    do {
      waiting =
          jdbi.withHandle(
              handle ->
                  handle
                      .createQuery(
                          """
                          SELECT EXISTS (
                            SELECT 1
                            FROM pg_locks AS relation_lock
                            JOIN pg_class AS relation
                              ON relation.oid = relation_lock.relation
                            JOIN pg_namespace AS namespace
                              ON namespace.oid = relation.relnamespace
                            WHERE namespace.nspname = :schema
                              AND relation.relname = :relationName
                              AND relation_lock.mode = :lockMode
                              AND relation_lock.granted IS FALSE)
                          """)
                      .bind("schema", SCHEMA)
                      .bind("relationName", relationName)
                      .bind("lockMode", lockMode)
                      .mapTo(Boolean.class)
                      .one());
    } while (!waiting && System.nanoTime() < deadline);
    assertThat(waiting).as("V79 should wait for %s on %s", lockMode, relationName).isTrue();
  }

  private static Instant databaseTime(Jdbi jdbi) {
    return jdbi.withHandle(
        handle -> handle.createQuery("SELECT clock_timestamp()").mapTo(Instant.class).one());
  }

  private static void migrateTo(String target) {
    Flyway.configure()
        .dataSource(
            DB_CONTAINER.getJdbcUrl(), DB_CONTAINER.getUsername(), DB_CONTAINER.getPassword())
        .schemas(SCHEMA)
        .defaultSchema(SCHEMA)
        .locations(MIGRATION_LOCATIONS)
        .repeatableSqlMigrationPrefix("Z__")
        .target(target)
        .load()
        .migrate();
  }

  private static Fixture seedV77Fixture(Jdbi jdbi) {
    UUID namespaceUuid = UUID.randomUUID();
    UUID aliasNamespaceUuid = UUID.randomUUID();
    UUID sourceUuid = UUID.randomUUID();
    UUID hiddenJobUuid = UUID.randomUUID();
    UUID realRunArgsUuid = UUID.randomUUID();
    UUID syntheticRunArgsUuid = UUID.randomUUID();
    UUID preexistingRealRunUuid = UUID.randomUUID();
    UUID preexistingSyntheticLikeRunUuid = UUID.randomUUID();
    UUID hiddenDatasetUuid = UUID.randomUUID();
    UUID otherDatasetUuid = UUID.randomUUID();
    Instant oldTime = Instant.parse("2000-01-01T00:00:00Z");
    String datasetName = "canonical-dataset";
    String aliasName = "old-primary-alias";
    jdbi.useHandle(
        handle -> {
          handle
              .createUpdate(
                  """
                  INSERT INTO %s.namespaces
                    (uuid, created_at, updated_at, name, is_hidden)
                  VALUES
                    (:namespaceUuid, :oldTime, :oldTime, 'migration-namespace', FALSE),
                    (:aliasNamespaceUuid, :oldTime, :oldTime, 'alias-namespace', FALSE)
                  """
                      .formatted(SCHEMA))
              .bind("namespaceUuid", namespaceUuid)
              .bind("aliasNamespaceUuid", aliasNamespaceUuid)
              .bind("oldTime", oldTime)
              .execute();
          handle
              .createUpdate(
                  """
                  INSERT INTO %s.sources
                    (uuid, type, created_at, updated_at, name, connection_url)
                  VALUES (:uuid, 'POSTGRESQL', :oldTime, :oldTime, 'migration-source',
                          'postgres://migration')
                  """
                      .formatted(SCHEMA))
              .bind("uuid", sourceUuid)
              .bind("oldTime", oldTime)
              .execute();
          handle
              .createUpdate(
                  """
                  INSERT INTO %s.jobs
                    (uuid, type, created_at, updated_at, namespace_uuid, simple_name, name,
                     namespace_name, is_hidden)
                  VALUES (:uuid, 'BATCH', :oldTime, :oldTime, :namespaceUuid, 'hidden-job',
                          'hidden-job', 'migration-namespace', TRUE)
                  """
                      .formatted(SCHEMA))
              .bind("uuid", hiddenJobUuid)
              .bind("namespaceUuid", namespaceUuid)
              .bind("oldTime", oldTime)
              .execute();
          handle
              .createUpdate(
                  """
                  INSERT INTO %s.run_args (uuid, created_at, args, checksum)
                  VALUES
                    (:realArgsUuid, :oldTime, '{"kind":"real"}', 'migration-real-run-args'),
                    (:syntheticArgsUuid, :oldTime, '{}', 'migration-synthetic-run-args')
                  """
                      .formatted(SCHEMA))
              .bind("realArgsUuid", realRunArgsUuid)
              .bind("syntheticArgsUuid", syntheticRunArgsUuid)
              .bind("oldTime", oldTime)
              .execute();
          handle
              .createUpdate(
                  """
                  INSERT INTO %s.runs
                    (uuid, created_at, updated_at, external_id, job_uuid, run_args_uuid,
                     namespace_name, job_name)
                  VALUES
                    (:realRunUuid, :oldTime, :oldTime, 'preexisting-real-run', :jobUuid,
                     :realArgsUuid, 'migration-namespace', 'hidden-job'),
                    (:syntheticRunUuid, :oldTime, :oldTime, 'preexisting-parent-like-run', :jobUuid,
                     :syntheticArgsUuid, 'migration-namespace', 'hidden-job')
                  """
                      .formatted(SCHEMA))
              .bind("realRunUuid", preexistingRealRunUuid)
              .bind("syntheticRunUuid", preexistingSyntheticLikeRunUuid)
              .bind("jobUuid", hiddenJobUuid)
              .bind("realArgsUuid", realRunArgsUuid)
              .bind("syntheticArgsUuid", syntheticRunArgsUuid)
              .bind("oldTime", oldTime)
              .execute();
          handle
              .createUpdate(
                  """
                  INSERT INTO %s.datasets
                    (uuid, type, created_at, updated_at, namespace_uuid, source_uuid, name,
                     physical_name, namespace_name, source_name, is_hidden, is_deleted)
                  VALUES
                    (:hiddenUuid, 'DB_TABLE', :oldTime, :oldTime, :namespaceUuid, :sourceUuid,
                     :datasetName, :datasetName, 'migration-namespace', 'migration-source', TRUE,
                     FALSE),
                    (:otherUuid, 'DB_TABLE', :oldTime, :oldTime, :namespaceUuid, :sourceUuid,
                     'other-dataset', 'other-dataset', 'migration-namespace', 'migration-source',
                     FALSE, FALSE)
                  """
                      .formatted(SCHEMA))
              .bind("hiddenUuid", hiddenDatasetUuid)
              .bind("otherUuid", otherDatasetUuid)
              .bind("namespaceUuid", namespaceUuid)
              .bind("sourceUuid", sourceUuid)
              .bind("datasetName", datasetName)
              .bind("oldTime", oldTime)
              .execute();
          handle
              .createUpdate(
                  """
                  INSERT INTO %s.dataset_symlinks
                    (dataset_uuid, name, namespace_uuid, type, is_primary, created_at, updated_at)
                  VALUES
                    (:otherUuid, :datasetName, :namespaceUuid, 'alias', TRUE, :oldTime, :oldTime),
                    (:hiddenUuid, :aliasName, :aliasNamespaceUuid, 'alias', TRUE, :oldTime, :oldTime)
                  """
                      .formatted(SCHEMA))
              .bind("hiddenUuid", hiddenDatasetUuid)
              .bind("otherUuid", otherDatasetUuid)
              .bind("datasetName", datasetName)
              .bind("aliasName", aliasName)
              .bind("namespaceUuid", namespaceUuid)
              .bind("aliasNamespaceUuid", aliasNamespaceUuid)
              .bind("oldTime", oldTime)
              .execute();
        });
    return new Fixture(
        namespaceUuid,
        aliasNamespaceUuid,
        hiddenJobUuid,
        preexistingRealRunUuid,
        preexistingSyntheticLikeRunUuid,
        hiddenDatasetUuid,
        otherDatasetUuid,
        datasetName,
        aliasName,
        oldTime);
  }

  private static List<String> openLineageColumns(Jdbi jdbi) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT table_name || '.' || column_name
                    FROM information_schema.columns
                    WHERE table_schema = :schema
                      AND table_name IN ('jobs', 'datasets', 'job_versions', 'runs')
                      AND column_name LIKE 'open_lineage_%'
                    """)
                .bind("schema", SCHEMA)
                .mapTo(String.class)
                .list());
  }

  private static ColumnDefinition columnDefinition(Jdbi jdbi, String tableName, String columnName) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT data_type, is_nullable = 'YES' AS nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = :schema
                      AND table_name = :tableName
                      AND column_name = :columnName
                    """)
                .bind("schema", SCHEMA)
                .bind("tableName", tableName)
                .bind("columnName", columnName)
                .map(
                    (resultSet, context) ->
                        new ColumnDefinition(
                            resultSet.getString("data_type"),
                            resultSet.getBoolean("nullable"),
                            resultSet.getString("column_default")))
                .one());
  }

  private static void assertConstraintValidation(Jdbi jdbi, boolean expected) {
    Map<String, Boolean> states =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT conname, convalidated
                        FROM pg_constraint AS constraint_row
                        JOIN pg_namespace AS namespace
                          ON namespace.oid = constraint_row.connamespace
                        WHERE namespace.nspname = :schema AND conname IN (<names>)
                        """)
                    .bind("schema", SCHEMA)
                    .bindList("names", CONSTRAINT_NAMES)
                    .map(
                        (resultSet, context) ->
                            Map.entry(resultSet.getString(1), resultSet.getBoolean(2)))
                    .list()
                    .stream()
                    .collect(
                        java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    assertThat(states).containsOnlyKeys(CONSTRAINT_NAMES.toArray(String[]::new));
    assertThat(states.values()).containsOnly(expected);
  }

  private static Instant updatedAt(Jdbi jdbi, String table, UUID uuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT updated_at FROM %s.%s WHERE uuid = :uuid".formatted(SCHEMA, table))
                .bind("uuid", uuid)
                .mapTo(Instant.class)
                .one());
  }

  private static Boolean parentPlaceholder(Jdbi jdbi, UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT open_lineage_parent_placeholder FROM %s.runs WHERE uuid = :runUuid"
                        .formatted(SCHEMA))
                .bind("runUuid", runUuid)
                .map((resultSet, context) -> resultSet.getObject(1, Boolean.class))
                .one());
  }

  private static Map<String, Object> symlink(Jdbi jdbi, UUID namespaceUuid, String name) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT dataset_uuid, is_primary, type
                    FROM %s.dataset_symlinks
                    WHERE namespace_uuid = :namespaceUuid AND name = :name
                    """
                        .formatted(SCHEMA))
                .bind("namespaceUuid", namespaceUuid)
                .bind("name", name)
                .mapToMap()
                .one());
  }

  private record Fixture(
      UUID namespaceUuid,
      UUID aliasNamespaceUuid,
      UUID hiddenJobUuid,
      UUID preexistingRealRunUuid,
      UUID preexistingSyntheticLikeRunUuid,
      UUID hiddenDatasetUuid,
      UUID otherDatasetUuid,
      String datasetName,
      String aliasName,
      Instant oldTime) {}

  private record ColumnDefinition(String dataType, boolean nullable, String defaultValue) {}
}
