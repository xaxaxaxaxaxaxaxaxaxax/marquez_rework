/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import marquez.common.models.NamespaceName;
import marquez.common.models.OwnerName;
import marquez.db.models.NamespaceRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.NamespaceMeta;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.SqlStatements;
import org.jdbi.v3.core.statement.StatementContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
public class NamespaceDaoTest {

  private static NamespaceDao namespaceDao;
  private static Jdbi jdbi;

  @BeforeAll
  public static void setUpOnce(Jdbi jdbi) {
    NamespaceDaoTest.jdbi = jdbi;
    namespaceDao = jdbi.onDemand(NamespaceDao.class);
  }

  @Test
  void testWriteAndReadNamespace() {
    var namespaceName = NamespaceName.of("postgres://localhost:5432");
    var namespaceMeta = new NamespaceMeta(new OwnerName("marquez"), null);
    namespaceDao.upsertNamespaceMeta(namespaceName, namespaceMeta);

    assertTrue(namespaceDao.exists(namespaceName.getValue()));
  }

  @Test
  void existingNamespaceUsesOneReadAndPreservesStoredValues() {
    String name = "existing-namespace-" + UUID.randomUUID();
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    NamespaceRow stored =
        namespaceDao.upsertNamespaceRow(UUID.randomUUID(), createdAt, name, "first-owner");

    List<String> executedSql = new ArrayList<>();
    NamespaceRow[] resolved = new NamespaceRow[1];
    jdbi.useHandle(
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
                  .attach(NamespaceDao.class)
                  .upsertNamespaceRow(
                      UUID.randomUUID(), createdAt.plusSeconds(60), name, "second-owner");
        });

    assertThat(resolved[0]).isEqualTo(stored);
    assertThat(executedSql).hasSize(1);
    assertThat(executedSql.get(0)).contains("SELECT * FROM namespaces WHERE name");
    assertThat(executedSql).noneMatch(sql -> sql.contains("TRANSACTION ISOLATION"));
  }

  @Test
  void missingNamespaceUsesReadThenInsertReturning() {
    String name = "missing-namespace-" + UUID.randomUUID();
    UUID uuid = UUID.randomUUID();
    Instant now = Instant.parse("2024-01-01T00:00:00Z");
    List<String> executedSql = new ArrayList<>();
    NamespaceRow[] inserted = new NamespaceRow[1];

    jdbi.useHandle(
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
          inserted[0] =
              handle.attach(NamespaceDao.class).upsertNamespaceRow(uuid, now, name, "owner");
        });

    assertThat(inserted[0].getUuid()).isEqualTo(uuid);
    assertThat(executedSql).hasSize(2);
    assertThat(executedSql.get(0)).contains("SELECT * FROM namespaces WHERE name");
    assertThat(executedSql.get(1)).contains("INSERT INTO namespaces").contains("RETURNING *");
    assertThat(executedSql).noneMatch(sql -> sql.contains("TRANSACTION ISOLATION"));
  }

  @Test
  void insertConflictReadsBackTheConcurrentWinner() {
    NamespaceDao racingDao = mock(NamespaceDao.class, CALLS_REAL_METHODS);
    UUID candidateUuid = UUID.randomUUID();
    Instant now = Instant.parse("2024-01-01T00:00:00Z");
    String name = "racing-namespace";
    String owner = "owner";
    NamespaceRow winner =
        new NamespaceRow(UUID.randomUUID(), now, now, name, null, "winning-owner", false);
    when(racingDao.findNamespaceByName(name)).thenReturn(Optional.empty(), Optional.of(winner));
    when(racingDao.insertNamespaceIfAbsent(candidateUuid, now, name, owner))
        .thenReturn(Optional.empty());

    assertThat(racingDao.upsertNamespaceRow(candidateUuid, now, name, owner)).isEqualTo(winner);

    InOrder calls = inOrder(racingDao);
    calls.verify(racingDao).findNamespaceByName(name);
    calls.verify(racingDao).insertNamespaceIfAbsent(candidateUuid, now, name, owner);
    calls.verify(racingDao).findNamespaceByName(name);
  }
}
