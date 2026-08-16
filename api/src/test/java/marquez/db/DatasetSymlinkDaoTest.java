/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.OpenLineageDefaults.DEFAULT_NAMESPACE_OWNER;
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
import marquez.db.DatasetSymlinkDao.PlannedDatasetSymlinkUpsert;
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

    List<PlannedDatasetSymlinkUpsert> inputs =
        List.of(
            planned(UUID.randomUUID(), "second", namespace.getUuid()),
            planned(UUID.randomUUID(), "first", namespace.getUuid()),
            planned(UUID.randomUUID(), "second", namespace.getUuid()));

    List<DatasetSymlinkRow> resolved =
        jdbi.inTransaction(
            handle ->
                handle.attach(DatasetSymlinkDao.class).resolvePlannedSymlinksInTransaction(inputs));

    assertThat(resolved).containsExactly(second, first, second);
  }

  @Test
  void missingDuplicateKeyUsesTheFirstCandidateUuid() {
    NamespaceRow namespace = newNamespace("duplicate-symlinks");
    UUID firstUuid = UUID.randomUUID();
    List<PlannedDatasetSymlinkUpsert> inputs =
        List.of(
            planned(firstUuid, "shared", namespace.getUuid()),
            planned(UUID.randomUUID(), "shared", namespace.getUuid()));

    List<DatasetSymlinkRow> resolved =
        jdbi.inTransaction(
            handle ->
                handle.attach(DatasetSymlinkDao.class).resolvePlannedSymlinksInTransaction(inputs));

    assertThat(resolved).hasSize(2).extracting(DatasetSymlinkRow::getUuid).containsOnly(firstUuid);
    assertThat(symlinkDao.findDatasetSymlinkByNamespaceUuidAndName(namespace.getUuid(), "shared"))
        .get()
        .extracting(DatasetSymlinkRow::getUuid)
        .isEqualTo(firstUuid);
  }

  @Test
  void plannedAliasCannotReplaceAnExistingPrimaryIdentity() {
    NamespaceRow namespace = newNamespace("protected-primary");
    DatasetSymlinkRow primary =
        symlinkDao.upsertDatasetSymlinkRow(
            UUID.randomUUID(), "protected", namespace.getUuid(), true, null, NOW);
    PlannedDatasetSymlinkUpsert alias =
        new PlannedDatasetSymlinkUpsert(
            UUID.randomUUID(), "protected", namespace.getUuid(), false, "alias", NOW);

    List<DatasetSymlinkRow> resolved =
        jdbi.inTransaction(
            handle ->
                handle
                    .attach(DatasetSymlinkDao.class)
                    .resolvePlannedSymlinksInTransaction(List.of(alias)));

    assertThat(resolved).containsExactly(primary);
    DatasetSymlinkRow persisted =
        symlinkDao
            .findDatasetSymlinkByNamespaceUuidAndName(namespace.getUuid(), "protected")
            .orElseThrow();
    assertThat(persisted).isEqualTo(primary);
    assertThat(persisted.isPrimary()).isTrue();
  }

  @Test
  @SuppressWarnings("unchecked")
  void usesBoundedGloballyOrderedChunks() {
    DatasetSymlinkDao batchingDao = mock(DatasetSymlinkDao.class, CALLS_REAL_METHODS);
    when(batchingDao.findDatasetSymlinksByKeys(any(UUID[].class), any(String[].class)))
        .thenReturn(List.of());
    when(batchingDao.insertPlannedSymlinksChunk(anyList()))
        .thenAnswer(
            invocation -> {
              List<PlannedDatasetSymlinkUpsert> chunk = invocation.getArgument(0);
              List<DatasetSymlinkRow> rows = new ArrayList<>(chunk.size());
              for (PlannedDatasetSymlinkUpsert symlink : chunk) {
                rows.add(rowFor(symlink));
              }
              return rows;
            });

    UUID namespaceUuid = UUID.randomUUID();
    List<PlannedDatasetSymlinkUpsert> inputs = new ArrayList<>();
    for (int index = DatasetSymlinkDao.MAX_SYMLINKS_PER_RESOLVE; index >= 0; index--) {
      inputs.add(planned(UUID.randomUUID(), String.format("dataset-%04d", index), namespaceUuid));
    }

    List<DatasetSymlinkRow> resolved = batchingDao.resolvePlannedSymlinksInTransaction(inputs);

    assertThat(resolved)
        .extracting(DatasetSymlinkRow::getUuid)
        .containsExactlyElementsOf(
            inputs.stream().map(PlannedDatasetSymlinkUpsert::getUuid).toList());

    ArgumentCaptor<List<PlannedDatasetSymlinkUpsert>> chunks = ArgumentCaptor.forClass(List.class);
    verify(batchingDao, times(2)).insertPlannedSymlinksChunk(chunks.capture());
    assertThat(chunks.getAllValues().get(0))
        .hasSize(DatasetSymlinkDao.MAX_SYMLINKS_PER_RESOLVE)
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
    PlannedDatasetSymlinkUpsert requested =
        planned(UUID.randomUUID(), "concurrent", UUID.randomUUID());
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
    when(racingDao.insertPlannedSymlinksChunk(anyList())).thenReturn(List.of());

    assertThat(racingDao.resolvePlannedSymlinksInTransaction(List.of(requested)))
        .containsExactly(winner);
    verify(racingDao, times(2)).findDatasetSymlinksByKeys(any(UUID[].class), any(String[].class));
    verify(racingDao).insertPlannedSymlinksChunk(anyList());
  }

  private NamespaceRow newNamespace(String prefix) {
    return namespaceDao.upsertNamespaceRow(
        UUID.randomUUID(), NOW, prefix + "-" + UUID.randomUUID(), DEFAULT_NAMESPACE_OWNER);
  }

  private static PlannedDatasetSymlinkUpsert planned(UUID uuid, String name, UUID namespaceUuid) {
    return new PlannedDatasetSymlinkUpsert(uuid, name, namespaceUuid, true, null, NOW);
  }

  private static DatasetSymlinkRow rowFor(PlannedDatasetSymlinkUpsert symlink) {
    return new DatasetSymlinkRow(
        symlink.getUuid(),
        symlink.getName(),
        symlink.getNamespaceUuid(),
        null,
        symlink.isPrimary(),
        symlink.getNow(),
        symlink.getNow());
  }
}
