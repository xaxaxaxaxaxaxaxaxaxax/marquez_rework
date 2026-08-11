/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.OpenLineageDao.DEFAULT_NAMESPACE_OWNER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import marquez.api.JdbiUtils;
import marquez.db.DatasetSymlinkDao.PrimaryDatasetSymlinkUpsert;
import marquez.db.models.DatasetSymlinkRow;
import marquez.db.models.NamespaceRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class DatasetSymlinkDaoTest {
  private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");

  private static Jdbi jdbi;
  private static DatasetSymlinkDao symlinkDao;
  private static NamespaceDao namespaceDao;

  @BeforeAll
  static void setUpOnce(Jdbi jdbi) {
    DatasetSymlinkDaoTest.jdbi = jdbi;
    symlinkDao = jdbi.onDemand(DatasetSymlinkDao.class);
    namespaceDao = jdbi.onDemand(NamespaceDao.class);
  }

  @AfterEach
  void tearDown() {
    JdbiUtils.cleanDatabase(jdbi);
  }

  @Test
  void resolvesHotRowsInEncounterOrderAndRestoresDuplicateOccurrences() {
    NamespaceRow namespace = newNamespace("hot-symlinks");
    DatasetSymlinkRow first =
        symlinkDao.upsertDatasetSymlinkRow(
            UUID.randomUUID(), "first", namespace.getUuid(), true, null, NOW);
    DatasetSymlinkRow second =
        symlinkDao.upsertDatasetSymlinkRow(
            UUID.randomUUID(), "second", namespace.getUuid(), true, null, NOW);

    List<PrimaryDatasetSymlinkUpsert> inputs =
        List.of(
            primary(UUID.randomUUID(), "second", namespace.getUuid()),
            primary(UUID.randomUUID(), "first", namespace.getUuid()),
            primary(UUID.randomUUID(), "second", namespace.getUuid()));

    List<DatasetSymlinkRow> resolved =
        jdbi.inTransaction(
            handle ->
                handle.attach(DatasetSymlinkDao.class).resolvePrimarySymlinksInTransaction(inputs));

    assertThat(resolved).containsExactly(second, first, second);
  }

  @Test
  void missingDuplicateKeyUsesTheFirstCandidateUuid() {
    NamespaceRow namespace = newNamespace("duplicate-symlinks");
    UUID firstUuid = UUID.randomUUID();
    List<PrimaryDatasetSymlinkUpsert> inputs =
        List.of(
            primary(firstUuid, "shared", namespace.getUuid()),
            primary(UUID.randomUUID(), "shared", namespace.getUuid()));

    List<DatasetSymlinkRow> resolved =
        jdbi.inTransaction(
            handle ->
                handle.attach(DatasetSymlinkDao.class).resolvePrimarySymlinksInTransaction(inputs));

    assertThat(resolved).hasSize(2).extracting(DatasetSymlinkRow::getUuid).containsOnly(firstUuid);
    assertThat(symlinkDao.findDatasetSymlinkByNamespaceUuidAndName(namespace.getUuid(), "shared"))
        .get()
        .extracting(DatasetSymlinkRow::getUuid)
        .isEqualTo(firstUuid);
  }

  @Test
  @SuppressWarnings("unchecked")
  void usesBoundedGloballyOrderedChunks() {
    DatasetSymlinkDao batchingDao = mock(DatasetSymlinkDao.class, CALLS_REAL_METHODS);
    when(batchingDao.findDatasetSymlinksByKeys(any(UUID[].class), any(String[].class)))
        .thenReturn(List.of());
    when(batchingDao.insertPrimarySymlinksChunk(anyList()))
        .thenAnswer(
            invocation -> {
              List<PrimaryDatasetSymlinkUpsert> chunk = invocation.getArgument(0);
              List<DatasetSymlinkRow> rows = new ArrayList<>(chunk.size());
              for (PrimaryDatasetSymlinkUpsert symlink : chunk) {
                rows.add(rowFor(symlink));
              }
              return rows;
            });

    UUID namespaceUuid = UUID.randomUUID();
    List<PrimaryDatasetSymlinkUpsert> inputs = new ArrayList<>();
    for (int index = DatasetSymlinkDao.MAX_PRIMARY_SYMLINKS_PER_RESOLVE; index >= 0; index--) {
      inputs.add(primary(UUID.randomUUID(), String.format("dataset-%04d", index), namespaceUuid));
    }

    List<DatasetSymlinkRow> resolved = batchingDao.resolvePrimarySymlinksInTransaction(inputs);

    assertThat(resolved)
        .extracting(DatasetSymlinkRow::getUuid)
        .containsExactlyElementsOf(
            inputs.stream().map(PrimaryDatasetSymlinkUpsert::getUuid).toList());

    ArgumentCaptor<List<PrimaryDatasetSymlinkUpsert>> chunks = ArgumentCaptor.forClass(List.class);
    verify(batchingDao, times(2)).insertPrimarySymlinksChunk(chunks.capture());
    assertThat(chunks.getAllValues().get(0))
        .hasSize(DatasetSymlinkDao.MAX_PRIMARY_SYMLINKS_PER_RESOLVE)
        .isSortedAccordingTo(
            (left, right) -> {
              int compared = left.getNamespaceUuid().compareTo(right.getNamespaceUuid());
              return compared != 0 ? compared : left.getName().compareTo(right.getName());
            });
    assertThat(chunks.getAllValues().get(1)).hasSize(1);
    verify(batchingDao, times(2)).findDatasetSymlinksByKeys(any(UUID[].class), any(String[].class));
  }

  @Test
  void readsBackAConcurrentWinnerAfterInsertReturningIsEmpty() {
    DatasetSymlinkDao racingDao = mock(DatasetSymlinkDao.class, CALLS_REAL_METHODS);
    PrimaryDatasetSymlinkUpsert requested =
        primary(UUID.randomUUID(), "concurrent", UUID.randomUUID());
    DatasetSymlinkRow winner =
        new DatasetSymlinkRow(
            UUID.randomUUID(),
            requested.getName(),
            requested.getNamespaceUuid(),
            null,
            true,
            NOW,
            NOW);
    when(racingDao.findDatasetSymlinksByKeys(any(UUID[].class), any(String[].class)))
        .thenReturn(List.of(), List.of(winner));
    when(racingDao.insertPrimarySymlinksChunk(anyList())).thenReturn(List.of());

    assertThat(racingDao.resolvePrimarySymlinksInTransaction(List.of(requested)))
        .containsExactly(winner);
    verify(racingDao, times(2)).findDatasetSymlinksByKeys(any(UUID[].class), any(String[].class));
    verify(racingDao).insertPrimarySymlinksChunk(anyList());
  }

  private NamespaceRow newNamespace(String prefix) {
    return namespaceDao.upsertNamespaceRow(
        UUID.randomUUID(), NOW, prefix + "-" + UUID.randomUUID(), DEFAULT_NAMESPACE_OWNER);
  }

  private static PrimaryDatasetSymlinkUpsert primary(UUID uuid, String name, UUID namespaceUuid) {
    return new PrimaryDatasetSymlinkUpsert(uuid, name, namespaceUuid, NOW);
  }

  private static DatasetSymlinkRow rowFor(PrimaryDatasetSymlinkUpsert symlink) {
    return new DatasetSymlinkRow(
        symlink.getUuid(),
        symlink.getName(),
        symlink.getNamespaceUuid(),
        null,
        true,
        symlink.getNow(),
        symlink.getNow());
  }
}
