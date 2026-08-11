/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.common.models.CommonModelGenerator.newJobName;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import marquez.api.JdbiUtils;
import marquez.common.models.RunState;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunRow;
import marquez.db.models.RunStateRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** The test suite for {@link RunStateDao}. */
@org.junit.jupiter.api.Tag("IntegrationTests")
@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class RunStateDaoTest {
  private static Jdbi jdbi;
  private static RunDao runDao;
  private static RunStateDao runStateDao;

  @BeforeAll
  static void setUpOnce(Jdbi configuredJdbi) {
    jdbi = configuredJdbi;
    runDao = jdbi.onDemand(RunDao.class);
    runStateDao = jdbi.onDemand(RunStateDao.class);
  }

  @AfterEach
  void tearDown(Jdbi configuredJdbi) {
    JdbiUtils.cleanDatabase(configuredJdbi);
  }

  @Test
  void insertsRunningStateAndLinksStartWithoutReplacingProjectedStateFields() {
    RunRow run = newRun();
    Instant projectedAt = Instant.parse("2024-01-01T00:00:00Z");
    Instant linkedAt = projectedAt.plusSeconds(1);
    runDao.updateRunState(run.getUuid(), projectedAt, RunState.RUNNING);
    UUID runStateUuid = UUID.randomUUID();

    RunStateRow inserted =
        runStateDao.insertAndLinkRunState(runStateUuid, linkedAt, run.getUuid(), RunState.RUNNING);

    RunRow linked = runDao.findRunByUuidAsRow(run.getUuid()).orElseThrow();
    assertThat(inserted.getUuid()).isEqualTo(runStateUuid);
    assertThat(inserted.getTransitionedAt()).isEqualTo(linkedAt);
    assertThat(inserted.getState()).isEqualTo(RunState.RUNNING.name());
    assertThat(linked.getCurrentRunState()).contains(RunState.RUNNING.name());
    assertThat(findTransitionedAt(run.getUuid())).isEqualTo(projectedAt);
    assertThat(linked.getUpdatedAt()).isEqualTo(linkedAt);
    assertThat(linked.getStartRunStateUuid()).contains(runStateUuid);
    assertThat(linked.getStartedAt()).contains(linkedAt);
    assertThat(linked.getEndRunStateUuid()).isEmpty();
    assertThat(linked.getEndedAt()).isEmpty();
  }

  @Test
  void insertsAndLinksEachTerminalStateWithoutReplacingProjectedStateFields() {
    List<RunState> terminalStates = List.of(RunState.COMPLETED, RunState.ABORTED, RunState.FAILED);
    for (int index = 0; index < terminalStates.size(); index++) {
      RunState terminalState = terminalStates.get(index);
      RunRow run = newRun();
      Instant projectedAt = Instant.parse("2024-02-01T00:00:00Z").plusSeconds(index * 10L);
      Instant linkedAt = projectedAt.plusSeconds(1);
      runDao.updateRunState(run.getUuid(), projectedAt, terminalState);
      UUID runStateUuid = UUID.randomUUID();

      RunStateRow inserted =
          runStateDao.insertAndLinkRunState(runStateUuid, linkedAt, run.getUuid(), terminalState);

      RunRow linked = runDao.findRunByUuidAsRow(run.getUuid()).orElseThrow();
      assertThat(inserted.getUuid()).isEqualTo(runStateUuid);
      assertThat(inserted.getState()).isEqualTo(terminalState.name());
      assertThat(linked.getCurrentRunState()).contains(terminalState.name());
      assertThat(findTransitionedAt(run.getUuid())).isEqualTo(projectedAt);
      assertThat(linked.getUpdatedAt()).isEqualTo(linkedAt);
      assertThat(linked.getStartRunStateUuid()).isEmpty();
      assertThat(linked.getStartedAt()).isEmpty();
      assertThat(linked.getEndRunStateUuid()).contains(runStateUuid);
      assertThat(linked.getEndedAt()).contains(linkedAt);
    }
  }

  @Test
  void nonPointerStateIsAppendedWithoutUpdatingTheRun() {
    RunRow run = newRun();
    Instant projectedAt = Instant.parse("2024-03-01T00:00:00Z");
    Instant insertedAt = projectedAt.plusSeconds(1);
    runDao.updateRunState(run.getUuid(), projectedAt, RunState.OTHER);
    UUID runStateUuid = UUID.randomUUID();

    RunStateRow inserted =
        runStateDao.insertAndLinkRunState(runStateUuid, insertedAt, run.getUuid(), RunState.OTHER);

    RunRow unchanged = runDao.findRunByUuidAsRow(run.getUuid()).orElseThrow();
    assertThat(inserted.getUuid()).isEqualTo(runStateUuid);
    assertThat(inserted.getState()).isEqualTo(RunState.OTHER.name());
    assertThat(unchanged.getCurrentRunState()).contains(RunState.OTHER.name());
    assertThat(findTransitionedAt(run.getUuid())).isEqualTo(projectedAt);
    assertThat(unchanged.getUpdatedAt()).isEqualTo(projectedAt);
    assertThat(unchanged.getStartRunStateUuid()).isEmpty();
    assertThat(unchanged.getEndRunStateUuid()).isEmpty();
  }

  @Test
  void replayAppendsHistoryAndRelinksThePointer() {
    RunRow run = newRun();
    long statesBefore = countRunStates(run.getUuid());
    Instant projectedAt = Instant.parse("2024-04-01T00:00:00Z");
    runDao.updateRunState(run.getUuid(), projectedAt, RunState.RUNNING);
    UUID firstStateUuid = UUID.randomUUID();
    UUID secondStateUuid = UUID.randomUUID();

    runStateDao.insertAndLinkRunState(firstStateUuid, projectedAt, run.getUuid(), RunState.RUNNING);
    runStateDao.insertAndLinkRunState(
        secondStateUuid, projectedAt.plusSeconds(1), run.getUuid(), RunState.RUNNING);

    RunRow replayed = runDao.findRunByUuidAsRow(run.getUuid()).orElseThrow();
    assertThat(countRunStates(run.getUuid())).isEqualTo(statesBefore + 2);
    assertThat(replayed.getStartRunStateUuid()).contains(secondStateUuid);
    assertThat(replayed.getStartedAt()).contains(projectedAt.plusSeconds(1));
    assertThat(findTransitionedAt(run.getUuid())).isEqualTo(projectedAt);
  }

  private RunRow newRun() {
    NamespaceRow namespace = DbTestUtils.newNamespace(jdbi);
    JobRow job = DbTestUtils.newJob(jdbi, namespace.getName(), newJobName().getValue());
    return DbTestUtils.newRun(jdbi, job);
  }

  private Instant findTransitionedAt(UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT transitioned_at FROM runs WHERE uuid = :runUuid")
                .bind("runUuid", runUuid)
                .mapTo(Instant.class)
                .one());
  }

  private long countRunStates(UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT count(*) FROM run_states WHERE run_uuid = :runUuid")
                .bind("runUuid", runUuid)
                .mapTo(Long.class)
                .one());
  }
}
