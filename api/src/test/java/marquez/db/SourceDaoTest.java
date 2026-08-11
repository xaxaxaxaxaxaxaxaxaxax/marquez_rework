/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import marquez.api.JdbiUtils;
import marquez.db.models.SourceRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.SqlStatements;
import org.jdbi.v3.core.statement.StatementContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class SourceDaoTest {
  private static final Instant CREATED_AT = Instant.parse("2024-01-01T00:00:00Z");
  private static final Instant LATER = CREATED_AT.plusSeconds(60);

  private static Jdbi jdbi;
  private static SourceDao sourceDao;

  @BeforeAll
  static void setUpOnce(Jdbi jdbi) {
    SourceDaoTest.jdbi = jdbi;
    sourceDao = jdbi.onDemand(SourceDao.class);
  }

  @AfterEach
  void tearDown() {
    JdbiUtils.cleanDatabase(jdbi);
  }

  @Test
  void upsertOrDefaultCreatesMissingSource() {
    UUID uuid = UUID.randomUUID();

    SourceRow source =
        sourceDao.upsertOrDefault(uuid, "POSTGRES", CREATED_AT, "default", "connection");

    assertThat(source.getUuid()).isEqualTo(uuid);
    assertThat(source.getType()).isEqualTo("POSTGRES");
    assertThat(source.getCreatedAt()).isEqualTo(CREATED_AT);
    assertThat(source.getUpdatedAt()).isEqualTo(CREATED_AT);
    assertThat(source.getName()).isEqualTo("default");
    assertThat(source.getConnectionUrl()).isEqualTo("connection");
  }

  @Test
  void upsertOrDefaultReturnsExistingSourceWithoutChangingOrLockingIt() {
    UUID existingUuid = UUID.randomUUID();
    SourceRow existing =
        sourceDao.upsert(
            existingUuid,
            "EXPLICIT",
            CREATED_AT,
            "shared-source",
            "explicit-connection",
            "explicit description");

    try (Handle reader = jdbi.open();
        Handle writer = jdbi.open()) {
      reader.begin();
      SourceRow resolved =
          reader
              .attach(SourceDao.class)
              .upsertOrDefault(
                  UUID.randomUUID(), "DEFAULT", LATER, "shared-source", "default-connection");

      assertThat(resolved).isEqualTo(existing);

      writer.execute("SET lock_timeout TO '500ms'");
      SourceRow updated =
          assertDoesNotThrow(
              () ->
                  writer
                      .attach(SourceDao.class)
                      .upsert(
                          UUID.randomUUID(),
                          "UPDATED",
                          LATER,
                          "shared-source",
                          "updated-connection",
                          "updated description"));

      assertThat(updated.getUuid()).isEqualTo(existingUuid);
      assertThat(updated.getType()).isEqualTo("UPDATED");
      assertThat(updated.getUpdatedAt()).isEqualTo(LATER);
      reader.rollback();
    }
  }

  @Test
  void inTransactionCoreUsesTheCallersTransactionWithoutIsolationMetadata() {
    String name = "source-core-" + UUID.randomUUID();
    SourceRow stored =
        sourceDao.upsert(
            UUID.randomUUID(), "EXPLICIT", CREATED_AT, name, "connection", "description");
    List<String> executedSql = new ArrayList<>();
    SourceRow[] resolved = new SourceRow[1];

    jdbi.useTransaction(
        handle -> {
          handle
              .getConfig(SqlStatements.class)
              .setSqlLogger(
                  new SqlLogger() {
                    @Override
                    public void logAfterExecution(StatementContext context) {
                      executedSql.add(context.getRawSql());
                    }
                  });
          resolved[0] =
              handle
                  .attach(SourceDao.class)
                  .upsertOrDefaultInTransaction(
                      UUID.randomUUID(), "DEFAULT", LATER, name, "other-connection");
        });

    assertThat(resolved[0]).isEqualTo(stored);
    assertThat(executedSql).hasSize(1);
    assertThat(executedSql.get(0)).contains("SELECT * FROM sources WHERE name");
    assertThat(executedSql).noneMatch(sql -> sql.contains("TRANSACTION ISOLATION"));
  }

  @Test
  void concurrentMissingDefaultsResolveToOneSource() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<SourceRow> first =
          executor.submit(() -> createDefaultAfter(ready, start, UUID.randomUUID()));
      Future<SourceRow> second =
          executor.submit(() -> createDefaultAfter(ready, start, UUID.randomUUID()));

      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      start.countDown();

      SourceRow firstResult = first.get(5, TimeUnit.SECONDS);
      SourceRow secondResult = second.get(5, TimeUnit.SECONDS);
      assertThat(firstResult.getUuid()).isEqualTo(secondResult.getUuid());
      int sourceCount =
          jdbi.withHandle(
              handle ->
                  handle
                      .createQuery("SELECT count(*) FROM sources WHERE name = :name")
                      .bind("name", "concurrent-default")
                      .mapTo(Integer.class)
                      .one());
      assertThat(sourceCount).isEqualTo(1);
    } finally {
      start.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static SourceRow createDefaultAfter(CountDownLatch ready, CountDownLatch start, UUID uuid)
      throws InterruptedException {
    ready.countDown();
    if (!start.await(5, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Timed out waiting to start concurrent source inserts");
    }
    return jdbi.onDemand(SourceDao.class)
        .upsertOrDefault(uuid, "POSTGRES", CREATED_AT, "concurrent-default", "");
  }
}
