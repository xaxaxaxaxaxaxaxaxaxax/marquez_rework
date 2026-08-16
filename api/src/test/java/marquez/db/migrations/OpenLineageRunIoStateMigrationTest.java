/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.migrations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("IntegrationTests")
@Testcontainers
class OpenLineageRunIoStateMigrationTest {
  private static final String SCHEMA = "open_lineage_run_io_state";
  private static final String[] LOCATIONS = {
    "classpath:marquez/db/migration", "classpath:marquez/db/migrations"
  };
  private static final Instant CREATED_AT = Instant.parse("2024-01-01T00:00:00Z");

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:14"));

  private static Jdbi jdbi;

  @Test
  void createsForwardOnlyConstrainedStateWithoutBackfill() {
    migrateTo("82");
    jdbi = Jdbi.create(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    UUID existingRun = UUID.randomUUID();
    insertRun(existingRun);

    migrateTo("83");

    assertThat(columns("open_lineage_run_io_state", true))
        .containsExactly("run_uuid", "io_type", "event_time", "event_key", "dataset_version_uuids");
    assertThat(stateCount()).isZero();
    assertThat(primaryKeyColumns()).containsExactly("run_uuid", "io_type");
    assertThat(columns("runs", false))
        .containsExactly("open_lineage_job_version_time", "open_lineage_job_version_key");
    assertThat(runOrderIsNull(existingRun)).isTrue();

    assertThatThrownBy(() -> updateRunTimeOnly(existingRun)).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> updateRunOrder(existingRun, new byte[31]))
        .isInstanceOf(RuntimeException.class);
    updateRunOrder(existingRun, new byte[32]);
    assertThat(runOrderIsNull(existingRun)).isFalse();

    insertState(existingRun, "INPUT", new byte[32]);
    assertThat(stateCount()).isOne();
    assertThat(
            integer(
                "SELECT cardinality(dataset_version_uuids) FROM "
                    + SCHEMA
                    + ".open_lineage_run_io_state"))
        .isZero();

    UUID secondRun = UUID.randomUUID();
    insertRun(secondRun);
    assertThatThrownBy(() -> insertState(secondRun, "UNKNOWN", new byte[32]))
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> insertState(secondRun, "OUTPUT", new byte[31]))
        .isInstanceOf(RuntimeException.class);

    execute("DELETE FROM " + SCHEMA + ".runs WHERE uuid = ?", existingRun);
    assertThat(stateCount()).isZero();
  }

  private static void insertRun(UUID runUuid) {
    execute(
        "INSERT INTO " + SCHEMA + ".runs (uuid, created_at, updated_at) VALUES (?, ?, ?)",
        runUuid,
        CREATED_AT,
        CREATED_AT);
  }

  private static void insertState(UUID runUuid, String ioType, byte[] eventKey) {
    execute(
        "INSERT INTO "
            + SCHEMA
            + ".open_lineage_run_io_state "
            + "(run_uuid, io_type, event_time, event_key, dataset_version_uuids) "
            + "VALUES (?, ?, ?, ?, CAST('{}' AS uuid[]))",
        runUuid,
        ioType,
        Instant.parse("2025-01-01T00:00:00Z"),
        eventKey);
  }

  private static void updateRunOrder(UUID runUuid, byte[] eventKey) {
    execute(
        "UPDATE "
            + SCHEMA
            + ".runs SET open_lineage_job_version_time = ?, "
            + "open_lineage_job_version_key = ? WHERE uuid = ?",
        CREATED_AT,
        eventKey,
        runUuid);
  }

  private static void updateRunTimeOnly(UUID runUuid) {
    execute(
        "UPDATE " + SCHEMA + ".runs SET open_lineage_job_version_time = ? WHERE uuid = ?",
        CREATED_AT,
        runUuid);
  }

  private static int stateCount() {
    return integer("SELECT count(*) FROM " + SCHEMA + ".open_lineage_run_io_state");
  }

  private static boolean runOrderIsNull(UUID runUuid) {
    return scalar(
        "SELECT open_lineage_job_version_time IS NULL AND "
            + "open_lineage_job_version_key IS NULL FROM "
            + SCHEMA
            + ".runs WHERE uuid = ?",
        Boolean.class,
        runUuid);
  }

  private static List<String> columns(String table, boolean allColumns) {
    return strings(
        "SELECT column_name FROM information_schema.columns "
            + "WHERE table_schema = ? AND table_name = ? AND (? OR column_name IN "
            + "('open_lineage_job_version_time', 'open_lineage_job_version_key')) "
            + "ORDER BY ordinal_position",
        SCHEMA,
        table,
        allColumns);
  }

  private static List<String> primaryKeyColumns() {
    return strings(
        "SELECT k.column_name FROM information_schema.table_constraints c "
            + "JOIN information_schema.key_column_usage k ON k.constraint_schema = "
            + "c.constraint_schema AND k.constraint_name = c.constraint_name "
            + "WHERE c.table_schema = ? AND c.table_name = 'open_lineage_run_io_state' "
            + "AND c.constraint_type = 'PRIMARY KEY' ORDER BY k.ordinal_position",
        SCHEMA);
  }

  private static void execute(String sql, Object... arguments) {
    jdbi.useHandle(
        handle -> {
          var update = handle.createUpdate(sql);
          for (int index = 0; index < arguments.length; index++) {
            update.bind(index, arguments[index]);
          }
          update.execute();
        });
  }

  private static int integer(String sql, Object... arguments) {
    return scalar(sql, Integer.class, arguments);
  }

  private static List<String> strings(String sql, Object... arguments) {
    return jdbi.withHandle(
        handle -> {
          var query = handle.createQuery(sql);
          for (int index = 0; index < arguments.length; index++) {
            query.bind(index, arguments[index]);
          }
          return query.mapTo(String.class).list();
        });
  }

  private static <T> T scalar(String sql, Class<T> type, Object... arguments) {
    return jdbi.withHandle(
        handle -> {
          var query = handle.createQuery(sql);
          for (int index = 0; index < arguments.length; index++) {
            query.bind(index, arguments[index]);
          }
          return query.mapTo(type).one();
        });
  }

  private static void migrateTo(String target) {
    Flyway.configure()
        .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
        .schemas(SCHEMA)
        .defaultSchema(SCHEMA)
        .locations(LOCATIONS)
        .repeatableSqlMigrationPrefix("Z__")
        .target(target)
        .load()
        .migrate();
  }
}
