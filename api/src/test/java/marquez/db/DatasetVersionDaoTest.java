/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.OpenLineageDefaults.DEFAULT_NAMESPACE_OWNER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import marquez.api.JdbiUtils;
import marquez.common.models.DatasetType;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.SourceRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class DatasetVersionDaoTest {
  private static final Instant CREATED_AT = Instant.parse("2024-01-01T00:00:00Z");

  private static Jdbi jdbi;
  private static DatasetVersionDao versionDao;
  private static DatasetDao datasetDao;
  private static NamespaceDao namespaceDao;
  private static SourceDao sourceDao;

  private DatasetRow dataset;

  @BeforeAll
  static void setUpOnce(Jdbi jdbi) {
    DatasetVersionDaoTest.jdbi = jdbi;
    versionDao = jdbi.onDemand(DatasetVersionDao.class);
    datasetDao = jdbi.onDemand(DatasetDao.class);
    namespaceDao = jdbi.onDemand(NamespaceDao.class);
    sourceDao = jdbi.onDemand(SourceDao.class);
  }

  @BeforeEach
  void setUp() {
    NamespaceRow namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), CREATED_AT, "version-lookup-test", DEFAULT_NAMESPACE_OWNER);
    SourceRow source =
        sourceDao.upsertOrDefault(
            UUID.randomUUID(), "POSTGRES", CREATED_AT, "version-lookup-test-source", "");
    dataset =
        datasetDao.upsert(
            UUID.randomUUID(),
            DatasetType.DB_TABLE,
            CREATED_AT,
            namespace.getUuid(),
            namespace.getName(),
            source.getUuid(),
            source.getName(),
            "version-lookup-test-dataset",
            "version-lookup-test-dataset");
  }

  @AfterEach
  void tearDown() {
    JdbiUtils.cleanDatabase(jdbi);
  }

  @Test
  void findRowsByUuidsSkipsAnEmptyLookup() {
    DatasetVersionDao lookupDao = mock(DatasetVersionDao.class, CALLS_REAL_METHODS);

    assertThat(lookupDao.findRowsByUuids(List.of())).isEmpty();

    verify(lookupDao, never()).findRowsByUuidArray(any(UUID[].class));
  }

  @Test
  void findRowsByUuidsReturnsTheExistingSetWithoutInputDuplicates() {
    DatasetVersionRow first = newVersion();
    DatasetVersionRow second = newVersion();

    List<DatasetVersionRow> rows =
        versionDao.findRowsByUuids(
            List.of(second.getUuid(), UUID.randomUUID(), first.getUuid(), first.getUuid()));

    assertThat(rows)
        .extracting(DatasetVersionRow::getUuid)
        .containsExactlyInAnyOrder(first.getUuid(), second.getUuid());
  }

  private DatasetVersionRow newVersion() {
    return versionDao.upsert(
        UUID.randomUUID(),
        CREATED_AT,
        dataset.getUuid(),
        UUID.randomUUID(),
        null,
        null,
        null,
        dataset.getNamespaceName(),
        dataset.getName(),
        null);
  }
}
