/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.common.models.CommonModelGenerator.newDatasetName;
import static marquez.common.models.CommonModelGenerator.newJobName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.ProjectionOrder;
import marquez.db.models.RunIoSnapshot;
import marquez.db.models.RunIoState;
import marquez.db.models.RunIoState.IoType;
import marquez.db.models.RunRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

@Tag("IntegrationTests")
@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class RunIoStateDaoTest {
  private static final Instant EVENT_TIME = Instant.parse("2025-01-01T00:00:00Z");

  private static Jdbi jdbi;
  private static RunIoStateDao stateDao;
  private static JobVersionDao jobVersionDao;
  private static DatasetDao datasetDao;
  private static DatasetVersionDao datasetVersionDao;
  private static RunDao runDao;

  private NamespaceRow namespace;
  private RunRow run;

  @BeforeAll
  static void setUpOnce(Jdbi configuredJdbi) {
    jdbi = configuredJdbi;
    stateDao = jdbi.onDemand(RunIoStateDao.class);
    jobVersionDao = jdbi.onDemand(JobVersionDao.class);
    datasetDao = jdbi.onDemand(DatasetDao.class);
    datasetVersionDao = jdbi.onDemand(DatasetVersionDao.class);
    runDao = jdbi.onDemand(RunDao.class);
  }

  @BeforeEach
  void setUp() {
    namespace = DbTestUtils.newNamespace(jdbi);
    JobRow job =
        DbTestUtils.createJobWithoutSymlinkTarget(
            jdbi, namespace, newJobName().getValue(), "run I/O state test");
    run = DbTestUtils.newRun(jdbi, job);
  }

  @AfterEach
  void tearDown() {
    JdbiUtils.cleanDatabase(jdbi);
  }

  @Test
  void appliesLastWriterWinsByEventTimeThenKey() {
    DatasetVersionRow first = newDatasetVersion(null);
    DatasetVersionRow second = newDatasetVersion(null);
    byte[] lowKey = eventKey(1);
    byte[] highKey = eventKey(2);

    assertThat(
            stateDao.upsert(
                state(IoType.INPUT, EVENT_TIME, lowKey, List.of(first.getUuid(), first.getUuid()))))
        .isTrue();
    assertThat(
            stateDao.upsert(
                state(
                    IoType.INPUT, EVENT_TIME.minusSeconds(1), highKey, List.of(second.getUuid()))))
        .isFalse();
    assertThat(stateDao.upsert(state(IoType.INPUT, EVENT_TIME, highKey, List.of(second.getUuid()))))
        .isTrue();

    RunIoState winner = stateDao.findForRunAndType(run.getUuid(), IoType.INPUT).orElseThrow();
    assertThat(winner.getEventTime()).isEqualTo(EVENT_TIME);
    assertThat(winner.getEventKey()).containsExactly(highKey);
    assertThat(winner.getDatasetVersionUuids()).containsExactly(second.getUuid());
  }

  @Test
  void authoritativeSidesPreserveOccurrencesClearAndFallBackIndependently() {
    DatasetVersionRow legacyInput = newDatasetVersion(null);
    DatasetVersionRow authoritativeInput = newDatasetVersion(null);
    DatasetVersionRow legacyOutput = newDatasetVersion(run.getUuid());
    runDao.updateInputMapping(run.getUuid(), legacyInput.getUuid());

    RunIoSnapshot legacy = jobVersionDao.findRunIoSnapshot(run.getUuid());
    assertSnapshot(legacy, List.of(legacyInput.getUuid()), List.of(legacyOutput.getUuid()));

    stateDao.upsert(
        state(
            IoType.INPUT,
            EVENT_TIME,
            eventKey(1),
            List.of(
                authoritativeInput.getUuid(),
                legacyInput.getUuid(),
                authoritativeInput.getUuid())));

    RunIoSnapshot inputAuthoritative = jobVersionDao.findRunIoSnapshot(run.getUuid());
    List<UUID> authoritativeOccurrences =
        List.of(authoritativeInput.getUuid(), legacyInput.getUuid(), authoritativeInput.getUuid());
    assertSnapshot(inputAuthoritative, authoritativeOccurrences, List.of(legacyOutput.getUuid()));

    stateDao.upsert(state(IoType.OUTPUT, EVENT_TIME, eventKey(1), List.of()));

    RunIoSnapshot outputCleared = jobVersionDao.findRunIoSnapshot(run.getUuid());
    assertSnapshot(outputCleared, authoritativeOccurrences, List.of());
    assertThat(datasetVersionDao.findOutputDatasetVersionsFor(run.getUuid()))
        .extracting(DatasetVersionRow::getUuid)
        .containsExactly(legacyOutput.getUuid());
    assertThat(datasetVersionDao.findInputDatasetVersionsFor(run.getUuid()))
        .extracting(DatasetVersionRow::getUuid)
        .containsExactly(legacyInput.getUuid());
  }

  @Test
  void validatesTheDigestAndDefensivelyCopiesStateValues() {
    byte[] key = eventKey(7);
    List<UUID> occurrences = new ArrayList<>(List.of(UUID.randomUUID()));
    RunIoState state = state(IoType.INPUT, EVENT_TIME, key, occurrences);

    key[31] = 99;
    occurrences.clear();

    assertThat(state.getEventKey()).containsExactly(eventKey(7));
    assertThat(state.getDatasetVersionUuids()).hasSize(1);
    assertThatThrownBy(
            () -> new RunIoState(run.getUuid(), IoType.INPUT, EVENT_TIME, new byte[31], List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void boundsMultiWriteStatementsInPostgresUuidOrderAcrossTheSignBit() {
    RunIoStateDao batchingDao = mock(RunIoStateDao.class, CALLS_REAL_METHODS);
    when(batchingDao.upsertChunk(anyList())).thenReturn(RunIoStateDao.MAX_STATES_PER_UPSERT, 1);
    List<RunIoState> states = new ArrayList<>();
    for (int index = 0; index <= RunIoStateDao.MAX_STATES_PER_UPSERT; index++) {
      long unsignedHighBits = index / 2L;
      long mostSignificantBits =
          index % 2 == 0 ? unsignedHighBits : Long.MIN_VALUE + unsignedHighBits;
      states.add(
          new RunIoState(
              new UUID(mostSignificantBits, Long.MAX_VALUE - index),
              IoType.INPUT,
              new ProjectionOrder(EVENT_TIME, eventKey(index)),
              List.of()));
    }

    assertThat(batchingDao.upsertAllInTransaction(states))
        .isEqualTo(RunIoStateDao.MAX_STATES_PER_UPSERT + 1);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<RunIoState>> chunks = ArgumentCaptor.forClass(List.class);
    verify(batchingDao, times(2)).upsertChunk(chunks.capture());
    assertThat(chunks.getAllValues()).extracting(List::size).containsExactly(1000, 1);

    List<UUID> actualOrder =
        chunks.getAllValues().stream().flatMap(List::stream).map(RunIoState::getRunUuid).toList();
    List<UUID> expectedOrder =
        states.stream()
            .map(RunIoState::getRunUuid)
            .sorted(RunIoStateDaoTest::compareUuidLikePostgres)
            .toList();
    assertThat(actualOrder).containsExactlyElementsOf(expectedOrder);
    assertThat(actualOrder.get(0).getMostSignificantBits()).isZero();
    assertThat(actualOrder.get(actualOrder.size() - 1).getMostSignificantBits()).isNegative();
  }

  private RunIoState state(
      IoType ioType, Instant eventTime, byte[] eventKey, List<UUID> datasetVersionUuids) {
    return new RunIoState(run.getUuid(), ioType, eventTime, eventKey, datasetVersionUuids);
  }

  private DatasetVersionRow newDatasetVersion(UUID producingRunUuid) {
    String datasetName = newDatasetName().getValue();
    DbTestUtils.newDataset(jdbi, namespace.getName(), datasetName);
    UUID datasetUuid =
        datasetDao.findDatasetAsRow(namespace.getName(), datasetName).orElseThrow().getUuid();
    return datasetVersionDao.upsert(
        UUID.randomUUID(),
        EVENT_TIME.minusSeconds(10),
        datasetUuid,
        UUID.randomUUID(),
        null,
        producingRunUuid,
        null,
        namespace.getName(),
        datasetName,
        null);
  }

  private static void assertSnapshot(
      RunIoSnapshot snapshot, List<UUID> inputUuids, List<UUID> outputUuids) {
    assertThat(snapshot.getInputs())
        .extracting(DatasetVersionRow::getUuid)
        .containsExactlyElementsOf(inputUuids);
    assertThat(snapshot.getOutputs())
        .extracting(DatasetVersionRow::getUuid)
        .containsExactlyElementsOf(outputUuids);
  }

  private static byte[] eventKey(int suffix) {
    byte[] key = new byte[32];
    key[28] = (byte) (suffix >>> 24);
    key[29] = (byte) (suffix >>> 16);
    key[30] = (byte) (suffix >>> 8);
    key[31] = (byte) suffix;
    return key;
  }

  private static int compareUuidLikePostgres(UUID left, UUID right) {
    int compared =
        Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
    return compared != 0
        ? compared
        : Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
  }
}
