/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Savepoint;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import marquez.common.Utils;
import marquez.db.OpenLineageQueueDao.PreparedAdmission;
import marquez.db.OpenLineageQueueDao.PreparedEvent;
import marquez.db.models.OpenLineageQueueRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InOrder;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class OpenLineageQueueDaoTest {
  private static final Instant EVENT_TIME = Instant.parse("2026-08-11T00:00:00Z");
  private static final String PRODUCER = "https://example.com/producer";
  private static final URI RUN_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/RunEvent");
  private static final URI JOB_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/JobEvent");
  private static final URI DATASET_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/DatasetEvent");

  private static Jdbi jdbi;
  private static OpenLineageQueueDao dao;

  @BeforeAll
  static void setUpOnce(Jdbi configuredJdbi) {
    jdbi = configuredJdbi;
    dao = jdbi.onDemand(OpenLineageQueueDao.class);
  }

  @BeforeEach
  void clearQueue() {
    jdbi.useHandle(
        handle ->
            handle.execute(
                "TRUNCATE TABLE open_lineage_queue_heads, open_lineage_dead_letters, "
                    + "open_lineage_queue RESTART IDENTITY"));
  }

  @Test
  void causalKeysAreCanonicalSanitizedAndDomainSeparated() throws Exception {
    long first = dao.enqueue(runEvent("same-run", "START", "team namespace", "first-name"));
    long follower = dao.enqueue(runEvent("same-run", "COMPLETE", "other namespace", "renamed-job"));
    long independentRun =
        dao.enqueue(runEvent("other-run", "START", "team namespace", "first-name"));
    long jobEvent = dao.enqueue(jobEvent("team namespace", "shared"));
    long sanitizedJobEvent = dao.enqueue(jobEvent("team_namespace", "shared"));
    long datasetEvent = dao.enqueue(datasetEvent("team namespace", "shared"));
    long sanitizedDatasetEvent = dao.enqueue(datasetEvent("team_namespace", "shared"));

    assertThat(orderingKey(first)).isEqualTo(orderingKey(follower));
    assertThat(orderingKey(first)).isNotEqualTo(orderingKey(independentRun));
    assertThat(orderingKey(jobEvent)).isEqualTo(orderingKey(sanitizedJobEvent));
    assertThat(orderingKey(datasetEvent)).isEqualTo(orderingKey(sanitizedDatasetEvent));
    assertThat(orderingKey(jobEvent)).isNotEqualTo(orderingKey(datasetEvent));
    assertThat(headId(orderingKey(first))).isEqualTo(first);
    assertThat(headId(orderingKey(jobEvent))).isEqualTo(jobEvent);
    assertThat(headId(orderingKey(datasetEvent))).isEqualTo(datasetEvent);
    assertQueueIntegrity();

    BaseEvent restored = Utils.getMapper().readValue(eventJson(first), BaseEvent.class);
    assertThat(restored).isInstanceOf(LineageEvent.class);
    assertThat(((LineageEvent) restored).getEventType()).isEqualTo("START");
  }

  @Test
  void validUuidAndProjectedNonUuidRunsUseCanonicalRunLanes() {
    String lower = "a0b1c2d3-e4f5-4678-9abc-def012345678";
    String upper = lower.toUpperCase(Locale.ROOT);
    UUID expectedKey =
        UUID.nameUUIDFromBytes(
            ("run" + '\0' + UUID.fromString(lower)).getBytes(StandardCharsets.UTF_8));
    long uuidFirst = dao.enqueue(runEvent(lower, "START"));
    long uuidFollower = dao.enqueue(runEvent(upper, "COMPLETE"));

    String reported = "reported-non-uuid-run";
    long namedFirst = dao.enqueue(runEvent(reported, "START"));
    long namedFollower =
        dao.enqueue(runEvent(Utils.openLineageRunUuid(reported).toString(), "COMPLETE"));

    assertThat(orderingKey(uuidFirst)).isEqualTo(expectedKey).isEqualTo(orderingKey(uuidFollower));
    assertThat(orderingKey(namedFirst)).isEqualTo(orderingKey(namedFollower));
    assertThat(orderingKey(namedFirst)).isNotEqualTo(orderingKey(uuidFirst));
    assertQueueIntegrity();
  }

  @Test
  void admissionRejectsMissingIdentityAndActiveNulBeforeWriting() {
    LineageEvent missingRun = runEvent("run", "START");
    missingRun.setRun(null);
    assertThatThrownBy(() -> dao.enqueue(missingRun))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("lineage event run is required");
    assertThatThrownBy(() -> dao.enqueue(runEvent(null, "START")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("lineage event runId is required");
    assertThatThrownBy(() -> dao.enqueue(jobEvent(null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("job event job is required");

    UUID key = UUID.randomUUID();
    assertThatThrownBy(
            () -> new PreparedEvent(key, Utils.toJson(Map.of("value", String.valueOf('\0')))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("OpenLineage event text values must not contain U+0000");

    assertThat(queueIds()).isEmpty();
    assertThat(headCount()).isZero();
  }

  @Test
  void preparedAdmissionOwnsASequentialInputSnapshotAndBindsItsColumnsDirectly() {
    LineageEvent first = runEvent("prepared-admission", "START");
    LineageEvent second = runEvent("prepared-admission", "COMPLETE");
    UUID lane = OpenLineageQueueDao.orderingKeyFor(first);
    List<BaseEvent> source = new LinkedList<>();
    source.add(first);
    source.add(second);

    PreparedAdmission admission = OpenLineageQueueDao.prepareAll(source);
    source.clear();

    assertThat(admission.size()).isEqualTo(2);
    assertThat(PreparedAdmission.class.getConstructors()).isEmpty();
    assertThat(PreparedAdmission.class.getFields()).isEmpty();
    assertThat(PreparedAdmission.class.getDeclaredMethods())
        .filteredOn(method -> Modifier.isPublic(method.getModifiers()))
        .extracting(method -> method.getName())
        .containsExactly("size");
    assertThat(dao.enqueueAll(admission)).isEqualTo(2);
    assertThat(lanePayloads(lane)).containsExactly(Utils.toJson(first), Utils.toJson(second));
    List<Long> ids = queueIds();
    assertThat(ids).hasSize(2);
    assertThat(admissionId(ids.get(0))).isPositive().isEqualTo(admissionId(ids.get(1)));
    assertQueueIntegrity();
  }

  @Test
  void bulkAdmissionPreservesExactJsonLaneOrderHeadsAndMembership() {
    long singleton = dao.enqueue(UUID.randomUUID(), payloadJson(0));
    UUID firstLane = UUID.randomUUID();
    UUID secondLane = UUID.randomUUID();
    UUID thirdLane = UUID.randomUUID();
    String first = "{ \"last\" : 2, \"first\" : 1 }";
    String second = "{\"literalNulEscape\":\"\\\\u0000\"}";
    String third = "{\n  \"third\" : true\n}";
    String duplicate = "{ \"duplicate\" : true }";

    int inserted =
        dao.enqueueAll(
            new LinkedList<>(
                List.of(
                    new PreparedEvent(firstLane, first),
                    new PreparedEvent(secondLane, second),
                    new PreparedEvent(firstLane, third),
                    new PreparedEvent(thirdLane, duplicate),
                    new PreparedEvent(secondLane, duplicate),
                    new PreparedEvent(firstLane, duplicate))));

    assertThat(inserted).isEqualTo(6);
    assertThat(lanePayloads(firstLane)).containsExactly(first, third, duplicate);
    assertThat(lanePayloads(secondLane)).containsExactly(second, duplicate);
    assertThat(lanePayloads(thirdLane)).containsExactly(duplicate);
    assertThat(headId(firstLane)).isEqualTo(laneIds(firstLane).get(0));
    assertThat(headId(secondLane)).isEqualTo(laneIds(secondLane).get(0));
    assertThat(headId(thirdLane)).isEqualTo(laneIds(thirdLane).get(0));
    assertThat(headCount()).isEqualTo(4);
    List<Long> ids = queueIds();
    assertThat(ids).hasSize(7);
    assertThat(admissionId(singleton)).isNull();
    Long firstAdmission = admissionId(ids.get(1));
    assertThat(firstAdmission).isPositive();
    assertThat(ids.subList(1, ids.size()))
        .allSatisfy(id -> assertThat(admissionId(id)).isEqualTo(firstAdmission));

    dao.enqueueAll(List.of(new PreparedEvent(UUID.randomUUID(), payloadJson(4))));
    long laterAdmissionEvent = queueIds().get(7);
    assertThat(admissionId(laterAdmissionEvent)).isPositive().isNotEqualTo(firstAdmission);
    assertQueueIntegrity();
  }

  @Test
  void bulkAdmissionValidatesAllBeforeWritingAndEmptyIsNoOp() {
    UUID key = UUID.randomUUID();
    List<PreparedEvent> containsNull = new ArrayList<>();
    containsNull.add(new PreparedEvent(key, payloadJson(1)));
    containsNull.add(null);
    List<PreparedEvent> tooMany =
        Collections.nCopies(
            OpenLineageQueueDao.MAX_ADMISSION_EVENTS + 1, new PreparedEvent(key, payloadJson(2)));
    List<BaseEvent> containsNullEvent = new ArrayList<>();
    containsNullEvent.add(runEvent("valid", "START"));
    containsNullEvent.add(null);

    assertThatThrownBy(() -> dao.enqueueAll((List<PreparedEvent>) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("prepared events are required");
    assertThatThrownBy(() -> dao.enqueueAll((PreparedAdmission) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("prepared admission is required");
    assertThatThrownBy(() -> dao.enqueueAll(containsNull))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("prepared event is required");
    assertThatThrownBy(() -> dao.enqueueAll(tooMany))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("prepared events must not exceed 1000");
    assertThatThrownBy(() -> OpenLineageQueueDao.prepareAll(containsNullEvent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("event is required");
    assertThatThrownBy(
            () ->
                OpenLineageQueueDao.prepareAll(
                    Collections.nCopies(
                        OpenLineageQueueDao.MAX_ADMISSION_EVENTS + 1,
                        runEvent("too-many", "START"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("events must not exceed 1000");

    try (Handle handle = jdbi.open()) {
      handle.setTransactionIsolationLevel(TransactionIsolationLevel.REPEATABLE_READ);
      assertThat(handle.attach(OpenLineageQueueDao.class).enqueueAll(List.of())).isZero();
      assertThat(
              handle
                  .attach(OpenLineageQueueDao.class)
                  .enqueueAll(OpenLineageQueueDao.prepareAll(List.of())))
          .isZero();
      assertThat(handle.isInTransaction()).isFalse();
    }

    assertThat(queueIds()).isEmpty();
    assertThat(headCount()).isZero();
  }

  @Test
  void sameLaneEnqueueFollowsCommitOrderAndRecoversFromRollback() throws Exception {
    assertConcurrentEnqueueAfterFirstTransaction(true);
    clearQueue();
    assertConcurrentEnqueueAfterFirstTransaction(false);
  }

  @Test
  void sameLaneBulkEnqueueUsesFreshSnapshotAfterPredecessorCommitOrRollback() throws Exception {
    assertConcurrentBulkEnqueueAfterFirstTransaction(true);
    clearQueue();
    assertConcurrentBulkEnqueueAfterFirstTransaction(false);
  }

  @Test
  void enqueueJoinsOuterReadCommittedTransactionAndRejectsRepeatableRead() {
    UUID key = UUID.randomUUID();
    assertThatThrownBy(
            () ->
                jdbi.useTransaction(
                    TransactionIsolationLevel.READ_COMMITTED,
                    handle -> {
                      handle.attach(OpenLineageQueueDao.class).enqueue(key, payloadJson(1));
                      throw new IllegalStateException("force rollback");
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("force rollback");

    assertThatThrownBy(
            () ->
                jdbi.useTransaction(
                    TransactionIsolationLevel.REPEATABLE_READ,
                    handle ->
                        handle.attach(OpenLineageQueueDao.class).enqueue(key, payloadJson(2))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("OpenLineage enqueue requires READ COMMITTED isolation");

    assertThat(queueIds()).isEmpty();
    assertThat(headCount()).isZero();
  }

  @Test
  void bulkEnqueueJoinsOuterReadCommittedTransactionAndRejectsRepeatableRead() {
    UUID firstKey = UUID.randomUUID();
    UUID secondKey = UUID.randomUUID();
    List<PreparedEvent> batch =
        List.of(
            new PreparedEvent(firstKey, payloadJson(1)),
            new PreparedEvent(secondKey, payloadJson(2)),
            new PreparedEvent(firstKey, payloadJson(3)));

    assertThatThrownBy(
            () ->
                jdbi.useTransaction(
                    TransactionIsolationLevel.READ_COMMITTED,
                    handle -> {
                      assertThat(handle.attach(OpenLineageQueueDao.class).enqueueAll(batch))
                          .isEqualTo(3);
                      throw new IllegalStateException("force bulk rollback");
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("force bulk rollback");

    assertThatThrownBy(
            () ->
                jdbi.useTransaction(
                    TransactionIsolationLevel.REPEATABLE_READ,
                    handle -> handle.attach(OpenLineageQueueDao.class).enqueueAll(batch)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("OpenLineage bulk enqueue requires READ COMMITTED isolation");

    try (Handle handle = jdbi.open()) {
      handle.setTransactionIsolationLevel(TransactionIsolationLevel.REPEATABLE_READ);
      OpenLineageQueueDao repeatableRead = handle.attach(OpenLineageQueueDao.class);

      assertThatThrownBy(() -> repeatableRead.enqueueAll(batch))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("OpenLineage bulk enqueue requires READ COMMITTED isolation");
      assertThat(handle.isInTransaction()).isFalse();
    }

    assertThat(queueIds()).isEmpty();
    assertThat(headCount()).isZero();
  }

  @Test
  void overlappingReverseLaneBatchesDoNotDeadlockOrReorderWithinLanes() throws Exception {
    UUID firstLane = UUID.randomUUID();
    UUID secondLane = UUID.randomUUID();
    String firstA = "{\"batch\":1,\"lane\":\"a\",\"position\":1}";
    String firstB = "{\"batch\":1,\"lane\":\"b\",\"position\":1}";
    String secondA = "{\"batch\":1,\"lane\":\"a\",\"position\":2}";
    String secondB = "{\"batch\":1,\"lane\":\"b\",\"position\":2}";
    String thirdB = "{\"batch\":2,\"lane\":\"b\",\"position\":1}";
    String thirdA = "{\"batch\":2,\"lane\":\"a\",\"position\":1}";
    String fourthB = "{\"batch\":2,\"lane\":\"b\",\"position\":2}";
    String fourthA = "{\"batch\":2,\"lane\":\"a\",\"position\":2}";
    List<PreparedEvent> forward =
        List.of(
            new PreparedEvent(firstLane, firstA),
            new PreparedEvent(secondLane, firstB),
            new PreparedEvent(firstLane, secondA),
            new PreparedEvent(secondLane, secondB));
    List<PreparedEvent> reverse =
        List.of(
            new PreparedEvent(secondLane, thirdB),
            new PreparedEvent(firstLane, thirdA),
            new PreparedEvent(secondLane, fourthB),
            new PreparedEvent(firstLane, fourthA));

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<Integer> forwardResult =
          executor.submit(() -> enqueueBulkAfterSignal(forward, ready, start));
      Future<Integer> reverseResult =
          executor.submit(() -> enqueueBulkAfterSignal(reverse, ready, start));
      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      start.countDown();

      assertThat(forwardResult.get(5, TimeUnit.SECONDS)).isEqualTo(4);
      assertThat(reverseResult.get(5, TimeUnit.SECONDS)).isEqualTo(4);
    } finally {
      start.countDown();
      executor.shutdownNow();
    }

    List<String> firstLanePayloads = lanePayloads(firstLane);
    List<String> secondLanePayloads = lanePayloads(secondLane);
    boolean forwardFirst =
        firstLanePayloads.equals(List.of(firstA, secondA, thirdA, fourthA))
            && secondLanePayloads.equals(List.of(firstB, secondB, thirdB, fourthB));
    boolean reverseFirst =
        firstLanePayloads.equals(List.of(thirdA, fourthA, firstA, secondA))
            && secondLanePayloads.equals(List.of(thirdB, fourthB, firstB, secondB));
    assertThat(forwardFirst || reverseFirst)
        .as("both lanes should contain the same two internally ordered batch segments")
        .isTrue();
    assertThat(headId(firstLane)).isEqualTo(laneIds(firstLane).get(0));
    assertThat(headId(secondLane)).isEqualTo(laneIds(secondLane).get(0));
    assertQueueIntegrity();
  }

  @Test
  void cachedSqlObjectPreparedEnqueueCommitsAndPreservesExactJson() {
    UUID key = UUID.randomUUID();
    String exactJson = "{ \"last\" : 2, \"first\" : 1 }";
    PreparedEvent event = new PreparedEvent(key, exactJson);
    long committedId = dao.enqueue(event);

    assertThat(committedId).isPositive();
    assertThat(eventJson(committedId)).isEqualTo(exactJson);
    assertThat(queueIds()).containsExactly(committedId);
    assertThat(headId(key)).isEqualTo(committedId);
    assertQueueIntegrity();
  }

  @Test
  void sqlObjectEnqueueRejectsInheritedRepeatableRead() {
    UUID key = UUID.randomUUID();

    try (Handle handle = jdbi.open()) {
      handle.setTransactionIsolationLevel(TransactionIsolationLevel.REPEATABLE_READ);
      OpenLineageQueueDao repeatableRead = handle.attach(OpenLineageQueueDao.class);

      assertThatThrownBy(() -> repeatableRead.enqueue(key, payloadJson(1)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("OpenLineage enqueue requires READ COMMITTED isolation");
      assertThat(handle.isInTransaction()).isFalse();
    }

    assertThat(queueIds()).isEmpty();
    assertThat(headCount()).isZero();
  }

  @Test
  void dequeueAndTerminalMethodsRequireAttachedReadCommittedTransaction() {
    UUID key = UUID.randomUUID();
    long id = dao.enqueue(key, payloadJson(1));

    assertThatThrownBy(dao::lockNextDue)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("OpenLineage dequeue requires an attached transaction");
    assertThatThrownBy(() -> dao.ackLocked(key, id))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("OpenLineage acknowledgement requires an attached transaction");
    assertThatThrownBy(() -> dao.retryLocked(key, id, 1, "retry", 0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("OpenLineage retry requires an attached transaction");
    assertThatThrownBy(() -> dao.deadLetterLocked(key, id, 1, "dead"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("OpenLineage dead-letter transition requires an attached transaction");

    assertThatThrownBy(
            () ->
                jdbi.useTransaction(
                    TransactionIsolationLevel.REPEATABLE_READ,
                    handle -> handle.attach(OpenLineageQueueDao.class).lockNextDue()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("OpenLineage dequeue requires READ COMMITTED isolation");

    assertThat(headState(key).attemptCount()).isZero();
    assertThat(queueIds()).containsExactly(id);
    assertQueueIntegrity();
  }

  @Test
  void twoReadCommittedTransactionsSkipLockedBatchHeadsWithoutOverlap() {
    List<UUID> lanes =
        List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
    dao.enqueueAll(
        List.of(
            new PreparedEvent(lanes.get(0), payloadJson(1)),
            new PreparedEvent(lanes.get(1), payloadJson(2)),
            new PreparedEvent(lanes.get(2), payloadJson(3)),
            new PreparedEvent(lanes.get(3), payloadJson(4))));
    List<Long> expectedIds = queueIds();
    setAllHeadsAvailableAt(Instant.parse("2000-01-01T00:00:00Z"));

    try (Handle firstHandle = jdbi.open();
        Handle secondHandle = jdbi.open()) {
      firstHandle.begin();
      secondHandle.begin();
      List<OpenLineageQueueRow> firstLocked =
          firstHandle.attach(OpenLineageQueueDao.class).lockNextDueBatch(2);
      List<OpenLineageQueueRow> secondLocked =
          secondHandle.attach(OpenLineageQueueDao.class).lockNextDueBatch(2);

      List<Long> firstIds = firstLocked.stream().map(OpenLineageQueueRow::id).toList();
      List<Long> secondIds = secondLocked.stream().map(OpenLineageQueueRow::id).toList();
      List<Long> allLocked = new ArrayList<>(firstIds);
      allLocked.addAll(secondIds);
      assertThat(firstIds).hasSize(2).doesNotContainAnyElementsOf(secondIds);
      assertThat(secondIds).hasSize(2);
      assertThat(allLocked).containsExactlyInAnyOrderElementsOf(expectedIds);
      assertThat(firstLocked).extracting(OpenLineageQueueRow::attemptCount).containsOnly(1);
      assertThat(secondLocked).extracting(OpenLineageQueueRow::attemptCount).containsOnly(1);
      firstHandle.rollback();
      secondHandle.rollback();
    }

    lanes.forEach(lane -> assertThat(headAttempt(lane)).isZero());
    assertQueueIntegrity();
  }

  @Test
  void batchClaimLocksHeadsBeforeFillingQ2CreditAndReturnsAdmissionOrder() {
    UUID firstLane = UUID.randomUUID();
    UUID secondLane = UUID.randomUUID();
    UUID thirdLane = UUID.randomUUID();
    dao.enqueueAll(
        List.of(
            new PreparedEvent(firstLane, payloadJson(1)),
            new PreparedEvent(secondLane, payloadJson(2)),
            new PreparedEvent(firstLane, payloadJson(3)),
            new PreparedEvent(thirdLane, payloadJson(4))));
    List<Long> expectedIds = queueIds();

    List<OpenLineageQueueRow> claimed =
        jdbi.inTransaction(
            TransactionIsolationLevel.READ_COMMITTED,
            handle -> handle.attach(OpenLineageQueueDao.class).lockNextDueBatch(4));

    assertThat(claimed).extracting(OpenLineageQueueRow::id).containsExactlyElementsOf(expectedIds);
    assertThat(claimed).extracting(OpenLineageQueueRow::admissionId).doesNotContainNull();
    assertThat(claimed)
        .extracting(OpenLineageQueueRow::admissionId)
        .containsOnly(admissionId(expectedIds.get(0)));
    assertThat(claimed.get(2).attemptCount()).isEqualTo(1);
    assertQueueIntegrity();
  }

  @Test
  void batchClaimTestsOnlyTheImmediateSuccessorAgainstTheAdmissionBoundary() {
    UUID lane = UUID.randomUUID();
    long first = dao.enqueue(lane, payloadJson(1));
    long intervening = dao.enqueue(lane, payloadJson(2));
    long laterMatching = dao.enqueue(lane, payloadJson(3));
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    UPDATE open_lineage_queue
                    SET admission_id = CASE WHEN id = :intervening THEN 82 ELSE 81 END
                    WHERE ordering_key = :lane
                    """)
                .bind("intervening", intervening)
                .bind("lane", lane)
                .execute());
    setHeadAvailableAt(lane, Instant.parse("2000-01-01T00:00:00Z"));

    List<OpenLineageQueueRow> claimed =
        jdbi.inTransaction(
            TransactionIsolationLevel.READ_COMMITTED,
            handle -> handle.attach(OpenLineageQueueDao.class).lockNextDueBatch(3));

    assertThat(claimed).extracting(OpenLineageQueueRow::id).containsExactly(first);
    assertThat(claimed.get(0).admissionId()).isEqualTo(81L);
    assertThat(queueIds()).containsExactly(first, intervening, laterMatching);
    assertQueueIntegrity();
  }

  @Test
  void batchClaimLimitOneUsesTheLegacyClaimAndValidatesBounds() {
    long eventId = dao.enqueue(UUID.randomUUID(), payloadJson(1));
    OpenLineageQueueRow legacy;
    List<OpenLineageQueueRow> bounded;
    try (Handle handle = jdbi.open()) {
      handle.begin();
      legacy = handle.attach(OpenLineageQueueDao.class).lockNextDue().orElseThrow();
      handle.rollback();
      handle.begin();
      bounded = handle.attach(OpenLineageQueueDao.class).lockNextDueBatch(1);
      handle.rollback();
    }

    assertThat(legacy.id()).isEqualTo(eventId);
    assertThat(bounded).containsExactly(legacy);
    assertThatThrownBy(() -> dao.lockNextDueBatch(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxEvents must be positive");
    assertThatThrownBy(() -> dao.lockNextDueBatch(1001))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxEvents must not exceed 1000");
  }

  @Test
  void olderPredecessorBlocksOnlyItsLaneFromTheAdmissionFrontier() {
    UUID blockedLane = UUID.randomUUID();
    UUID readyLane = UUID.randomUUID();
    long predecessor = dao.enqueue(blockedLane, payloadJson(0));
    dao.enqueueAll(
        List.of(
            new PreparedEvent(blockedLane, payloadJson(1)),
            new PreparedEvent(readyLane, payloadJson(2))));
    setHeadAvailableAt(blockedLane, Instant.parse("2100-01-01T00:00:00Z"));

    List<OpenLineageQueueRow> claimed =
        jdbi.inTransaction(
            TransactionIsolationLevel.READ_COMMITTED,
            handle -> handle.attach(OpenLineageQueueDao.class).lockNextDueBatch(8));

    assertThat(claimed).hasSize(1);
    assertThat(claimed.get(0).orderingKey()).isEqualTo(readyLane);
    assertThat(claimed.get(0).admissionId()).isNotNull();
    assertThat(headId(blockedLane)).isEqualTo(predecessor);
    assertQueueIntegrity();
  }

  @Test
  void bulkAcknowledgementConsumesExactlyOneQ2CreditPerLane() {
    UUID lane = UUID.randomUUID();
    dao.enqueueAll(
        List.of(
            new PreparedEvent(lane, payloadJson(1)),
            new PreparedEvent(lane, payloadJson(2)),
            new PreparedEvent(lane, payloadJson(3))));
    List<Long> ids = queueIds();
    setHeadAvailableAt(lane, Instant.parse("2000-01-01T00:00:00.321Z"));
    String originalSchedule = headAvailableAtBytes(lane);

    jdbi.useTransaction(
        TransactionIsolationLevel.READ_COMMITTED,
        handle -> {
          OpenLineageQueueDao transactional = handle.attach(OpenLineageQueueDao.class);
          List<OpenLineageQueueRow> claimed = transactional.lockNextDueBatch(8);
          assertThat(claimed)
              .extracting(OpenLineageQueueRow::id)
              .containsExactly(ids.get(0), ids.get(1));
          transactional.ackLockedAll(new LinkedList<>(claimed));
        });

    HeadState state = headState(lane);
    assertThat(state.eventId()).isEqualTo(ids.get(2));
    assertThat(state.refreshDueOnAdvance()).isFalse();
    assertThat(headAvailableAtBytes(lane)).isNotEqualTo(originalSchedule);
    assertThat(queueIds()).containsExactly(ids.get(2));
    assertQueueIntegrity();
  }

  @Test
  void batchAcknowledgementRejectsNonIncreasingDuplicateAndMixedAdmissionRowsBeforeSql() {
    OpenLineageQueueRow first = lockedRow(51, UUID.randomUUID(), 7L);
    OpenLineageQueueRow second = lockedRow(52, UUID.randomUUID(), 7L);
    OpenLineageQueueRow otherAdmission = lockedRow(53, UUID.randomUUID(), 8L);
    OpenLineageQueueDao transactional = transactionalQueueDaoMock();

    assertThatThrownBy(() -> transactional.ackLockedAll(List.of(second, first)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("locked rows must be strictly ordered by event ID");
    assertThatThrownBy(() -> transactional.ackLockedAll(List.of(first, first)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("locked rows must be strictly ordered by event ID");
    assertThatThrownBy(() -> transactional.ackLockedAll(List.of(first, otherAdmission)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("locked rows must belong to one admission");

    verify(transactional, never()).acquireOrderingKeyLocks(any(UUID[].class));
    verify(transactional, never())
        .acknowledgeLockedPrefixesAfterLaneLocks(any(UUID[].class), any(long[].class));
  }

  @Test
  void bulkAcknowledgementUsesAFreshSnapshotAfterWaitingForSameLaneEnqueue() throws Exception {
    UUID lane = UUID.randomUUID();
    dao.enqueueAll(List.of(new PreparedEvent(lane, payloadJson(1))));
    long acknowledgedId = queueIds().get(0);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Handle enqueueHandle = jdbi.open();
        Handle observer = jdbi.open()) {
      enqueueHandle.begin();
      OpenLineageQueueDao enqueueDao = enqueueHandle.attach(OpenLineageQueueDao.class);
      enqueueDao.acquireOrderingKeyLock(lane);
      long follower = enqueueDao.insertEventAndMaybeHeadAfterLock(lane, payloadJson(2));

      AtomicInteger backendPid = new AtomicInteger();
      CountDownLatch claimed = new CountDownLatch(1);
      Future<?> acknowledgement =
          executor.submit(
              () -> {
                try (Handle acknowledgeHandle = jdbi.open()) {
                  acknowledgeHandle.begin();
                  backendPid.set(
                      acknowledgeHandle
                          .createQuery("SELECT pg_backend_pid()")
                          .mapTo(Integer.class)
                          .one());
                  OpenLineageQueueDao transactional =
                      acknowledgeHandle.attach(OpenLineageQueueDao.class);
                  List<OpenLineageQueueRow> rows = transactional.lockNextDueBatch(8);
                  assertThat(rows)
                      .extracting(OpenLineageQueueRow::id)
                      .containsExactly(acknowledgedId);
                  claimed.countDown();
                  transactional.ackLockedAll(rows);
                  acknowledgeHandle.commit();
                }
              });

      assertThat(claimed.await(5, TimeUnit.SECONDS)).isTrue();
      awaitAdvisoryLockWait(observer, backendPid.get());
      enqueueHandle.commit();
      acknowledgement.get(5, TimeUnit.SECONDS);

      assertThat(queueIds(observer)).containsExactly(follower);
      assertThat(headId(observer, lane)).isEqualTo(follower);
      assertQueueIntegrity(observer);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void trueQ2HeadDoesNotClaimAnotherSameAdmissionFollower() {
    UUID lane = UUID.randomUUID();
    dao.enqueueAll(
        List.of(
            new PreparedEvent(lane, payloadJson(1)),
            new PreparedEvent(lane, payloadJson(2)),
            new PreparedEvent(lane, payloadJson(3))));
    OpenLineageQueueRow first = ackNext();

    List<OpenLineageQueueRow> claimed =
        jdbi.inTransaction(
            TransactionIsolationLevel.READ_COMMITTED,
            handle -> handle.attach(OpenLineageQueueDao.class).lockNextDueBatch(8));

    assertThat(claimed).hasSize(1);
    assertThat(claimed.get(0).id()).isNotEqualTo(first.id());
    assertThat(headState(lane).refreshDueOnAdvance()).isTrue();
    assertQueueIntegrity();
  }

  @Test
  void rowLockRollbackRestoresExactHeadPointerScheduleAttemptAndPayload() {
    UUID key = UUID.randomUUID();
    long first = dao.enqueue(key, payloadJson(1));
    long follower = dao.enqueue(key, payloadJson(2));
    setHeadAvailableAt(key, Instant.parse("2000-01-01T00:00:00.321Z"));
    HeadState before = headState(key);
    String beforeSchedule = headAvailableAtBytes(key);
    String firstPayload = eventJson(first);
    String followerPayload = eventJson(follower);

    try (Handle owner = jdbi.open();
        Handle replica = jdbi.open()) {
      owner.begin();
      OpenLineageQueueDao ownerDao = owner.attach(OpenLineageQueueDao.class);
      OpenLineageQueueRow locked = ownerDao.lockNextDue().orElseThrow();
      assertThat(locked.id()).isEqualTo(first);
      assertThat(locked.attemptCount()).isEqualTo(1);
      ownerDao.ackLocked(key, first);
      assertThat(headId(owner, key)).isEqualTo(follower);

      replica.begin();
      assertThat(replica.attach(OpenLineageQueueDao.class).lockNextDue()).isEmpty();
      replica.rollback();
      owner.rollback();
    }

    assertThat(headState(key)).isEqualTo(before);
    assertThat(headAvailableAtBytes(key)).isEqualTo(beforeSchedule);
    assertThat(queueIds()).containsExactly(first, follower);
    assertThat(eventJson(first)).isEqualTo(firstPayload);
    assertThat(eventJson(follower)).isEqualTo(followerPayload);
    assertQueueIntegrity();
  }

  @Test
  void savepointRollbackRetainsPreexistingHeadLockAndCaughtAttemptPersists() throws Exception {
    UUID key = UUID.randomUUID();
    long first = dao.enqueue(key, payloadJson(1));
    long follower = dao.enqueue(key, payloadJson(2));
    String originalPayload = eventJson(first);

    try (Handle owner = jdbi.open();
        Handle replica = jdbi.open()) {
      owner.begin();
      OpenLineageQueueDao ownerDao = owner.attach(OpenLineageQueueDao.class);
      OpenLineageQueueRow attempt = ownerDao.lockNextDue().orElseThrow();
      assertThat(attempt.id()).isEqualTo(first);
      assertThat(attempt.attemptCount()).isEqualTo(1);

      Savepoint projection = owner.getConnection().setSavepoint("queue_projection");
      owner
          .createUpdate(
              "UPDATE open_lineage_queue SET event = :event "
                  + "WHERE ordering_key = :key AND id = :id")
          .bind("event", "{\"partial\":true}")
          .bind("key", key)
          .bind("id", first)
          .execute();
      owner.getConnection().rollback(projection);
      owner.getConnection().releaseSavepoint(projection);
      assertThat(eventJson(owner, first)).isEqualTo(originalPayload);

      replica.begin();
      assertThat(replica.attach(OpenLineageQueueDao.class).lockNextDue()).isEmpty();
      replica.rollback();

      ownerDao.retryLocked(key, first, attempt.attemptCount(), "caught projection failure", 0);
      owner.commit();
    }

    HeadState retried = headState(key);
    assertThat(retried.eventId()).isEqualTo(first);
    assertThat(retried.attemptCount()).isEqualTo(1);
    assertThat(retried.refreshDueOnAdvance()).isFalse();
    assertThat(retried.lastError()).isEqualTo("caught projection failure");
    assertMillisecondAligned(retried.availableAt());
    makeScheduleDue(key);
    HeadState beforeRolledBackDeadLetter = headState(key);

    try (Handle owner = jdbi.open()) {
      owner.begin();
      OpenLineageQueueDao ownerDao = owner.attach(OpenLineageQueueDao.class);
      OpenLineageQueueRow secondAttempt = ownerDao.lockNextDue().orElseThrow();
      assertThat(secondAttempt.id()).isEqualTo(first);
      assertThat(secondAttempt.attemptCount()).isEqualTo(2);
      ownerDao.deadLetterLocked(key, first, secondAttempt.attemptCount(), "forced rollback");
      assertThat(deadIds(owner)).containsExactly(first);
      assertThat(queueIds(owner)).containsExactly(follower);
      owner.rollback();
    }

    assertThat(headState(key)).isEqualTo(beforeRolledBackDeadLetter);
    assertThat(deadIds()).isEmpty();
    assertThat(queueIds()).containsExactly(first, follower);
    assertThat(eventJson(first)).isEqualTo(originalPayload);

    OpenLineageQueueRow recovered =
        jdbi.inTransaction(
            TransactionIsolationLevel.READ_COMMITTED,
            handle -> handle.attach(OpenLineageQueueDao.class).lockNextDue().orElseThrow());
    assertThat(recovered.id()).isEqualTo(first);
    assertThat(recovered.attemptCount()).isEqualTo(2);
    assertThat(headAttempt(key)).isEqualTo(1);
    assertQueueIntegrity();
  }

  @ParameterizedTest(name = "{0}, enqueue holds lane lock first={1}")
  @CsvSource({"ACK,true", "ACK,false", "DEAD_LETTER,true", "DEAD_LETTER,false"})
  void terminalTransitionAndSameLaneEnqueueAreAtomicInBothLockOrders(
      String terminalName, boolean enqueueLocksFirst) throws Exception {
    Terminal terminal = Terminal.valueOf(terminalName);
    UUID key = UUID.randomUUID();
    long first = dao.enqueue(key, payloadJson(1));

    long follower =
        enqueueLocksFirst
            ? finishEnqueueBeforeTerminal(key, first, terminal)
            : finishTerminalBeforeEnqueue(key, first, terminal);

    assertThat(queueIds()).containsExactly(follower);
    assertThat(headId(key)).isEqualTo(follower);
    assertThat(headAttempt(key)).isZero();
    assertThat(headState(key).lastError()).isNull();
    if (terminal == Terminal.DEAD_LETTER) {
      assertThat(deadIds()).containsExactly(first);
    } else {
      assertThat(deadIds()).isEmpty();
    }
    assertQueueIntegrity();
  }

  @Test
  void afterLaneLockTransitionsUseOneMutationWithoutAcquiringTheAdvisoryLockAgain() {
    UUID key = UUID.randomUUID();
    long eventId = 43L;

    OpenLineageQueueDao acknowledged = transactionalQueueDaoMock();
    doReturn(1L).when(acknowledged).finishLockedHeadAfterLaneLock(key, eventId);
    acknowledged.ackLockedAfterLaneLock(key, eventId);

    verify(acknowledged, never()).acquireOrderingKeyLock(key);
    verify(acknowledged).finishLockedHeadAfterLaneLock(key, eventId);

    OpenLineageQueueDao dead = transactionalQueueDaoMock();
    doReturn(1).when(dead).insertDeadLetterLocked(key, eventId, 2, "poison");
    doReturn(1L).when(dead).finishLockedHeadAfterLaneLock(key, eventId);
    dead.deadLetterLockedAfterLaneLock(key, eventId, 2, "poison");

    verify(dead, never()).acquireOrderingKeyLock(key);
    InOrder deadCalls = inOrder(dead);
    deadCalls.verify(dead).insertDeadLetterLocked(key, eventId, 2, "poison");
    deadCalls.verify(dead).finishLockedHeadAfterLaneLock(key, eventId);
  }

  @Test
  void terminalMutationRejectsANonHeadAndSingleEventAckDeletesTheLane() {
    UUID key = UUID.randomUUID();
    long first = dao.enqueue(key, payloadJson(1));
    long second = dao.enqueue(key, payloadJson(2));

    assertThatThrownBy(
            () ->
                jdbi.useTransaction(
                    TransactionIsolationLevel.READ_COMMITTED,
                    handle -> handle.attach(OpenLineageQueueDao.class).ackLocked(key, second)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("acknowledge OpenLineage queue payload " + second);

    assertThat(queueIds()).containsExactly(first, second);
    assertThat(headId(key)).isEqualTo(first);
    ackNext();
    ackNext();

    assertThat(queueIds()).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(deadIds()).isEmpty();
    assertQueueIntegrity();
  }

  @Test
  void acknowledgementPreservesThenRefreshesTheQ2Schedule() {
    UUID key = UUID.randomUUID();
    long first = dao.enqueue(key, payloadJson(1));
    long second = dao.enqueue(key, payloadJson(2));
    long third = dao.enqueue(key, payloadJson(3));
    setHeadAvailableAt(key, Instant.parse("2000-01-01T00:00:00.321Z"));
    String originalSchedule = headAvailableAtBytes(key);

    OpenLineageQueueRow firstAttempt = ackNext();
    assertThat(firstAttempt.id()).isEqualTo(first);
    HeadState preserved = headState(key);
    assertThat(preserved.eventId()).isEqualTo(second);
    assertThat(preserved.refreshDueOnAdvance()).isTrue();
    assertThat(preserved.attemptCount()).isZero();
    assertThat(headAvailableAtBytes(key)).isEqualTo(originalSchedule);

    OpenLineageQueueRow secondAttempt = ackNext();
    assertThat(secondAttempt.id()).isEqualTo(second);
    HeadState refreshed = headState(key);
    assertThat(refreshed.eventId()).isEqualTo(third);
    assertThat(refreshed.refreshDueOnAdvance()).isFalse();
    assertThat(refreshed.attemptCount()).isZero();
    assertThat(headAvailableAtBytes(key)).isNotEqualTo(originalSchedule);
    assertMillisecondAligned(refreshed.availableAt());
    assertThat(queueIds()).containsExactly(third);
    assertQueueIntegrity();
  }

  @Test
  void deadLetterUsesQ2AndRetryRefreshesTheIndependentSchedule() {
    UUID key = UUID.randomUUID();
    dao.enqueueAll(List.of(new PreparedEvent(key, payloadJson(1))));
    long first = queueIds().get(0);
    Long firstAdmission = admissionId(first);
    long second = dao.enqueue(key, payloadJson(2));
    long third = dao.enqueue(key, payloadJson(3));
    long fourth = dao.enqueue(key, payloadJson(4));
    setHeadAvailableAt(key, Instant.parse("2000-01-01T00:00:00.654Z"));
    String originalSchedule = headAvailableAtBytes(key);

    deadLetterNext("first poison");
    HeadState preserved = headState(key);
    assertThat(preserved.eventId()).isEqualTo(second);
    assertThat(preserved.refreshDueOnAdvance()).isTrue();
    assertThat(headAvailableAtBytes(key)).isEqualTo(originalSchedule);

    deadLetterNext("second poison");
    HeadState refreshed = headState(key);
    assertThat(refreshed.eventId()).isEqualTo(third);
    assertThat(refreshed.refreshDueOnAdvance()).isFalse();
    assertThat(headAvailableAtBytes(key)).isNotEqualTo(originalSchedule);
    assertMillisecondAligned(refreshed.availableAt());

    OpenLineageQueueRow thirdAttempt = ackNext();
    assertThat(thirdAttempt.id()).isEqualTo(third);
    HeadState retryTarget = headState(key);
    assertThat(retryTarget.eventId()).isEqualTo(fourth);
    assertThat(retryTarget.refreshDueOnAdvance()).isTrue();
    String scheduleBeforeRetry = headAvailableAtBytes(key);

    jdbi.useTransaction(
        TransactionIsolationLevel.READ_COMMITTED,
        handle -> {
          OpenLineageQueueDao transactional = handle.attach(OpenLineageQueueDao.class);
          OpenLineageQueueRow row = transactional.lockNextDue().orElseThrow();
          assertThat(row.id()).isEqualTo(fourth);
          transactional.retryLocked(key, fourth, row.attemptCount(), "retry fourth", 60_000);
        });
    HeadState retried = headState(key);
    assertThat(retried.eventId()).isEqualTo(fourth);
    assertThat(retried.refreshDueOnAdvance()).isFalse();
    assertThat(retried.attemptCount()).isEqualTo(1);
    assertThat(retried.lastError()).isEqualTo("retry fourth");
    assertThat(headAvailableAtBytes(key)).isNotEqualTo(scheduleBeforeRetry);
    assertThat(retried.availableAt()).isAfter(retryTarget.availableAt());
    assertMillisecondAligned(retried.availableAt());
    assertThat(deadIds()).containsExactlyInAnyOrder(first, second);
    assertThat(deadAdmissionId(first)).isEqualTo(firstAdmission);
    assertThat(queueIds()).containsExactly(fourth);
    assertQueueIntegrity();
  }

  @Test
  void deadLetterPurgeIsBoundedAndSkipsLockedRows() {
    List<Long> created = new ArrayList<>();
    for (int index = 0; index < 4; index++) {
      created.add(dao.enqueue(runEvent("dead-" + index, "START")));
      deadLetterNext("retention test");
    }
    Instant old = Instant.now().minusSeconds(172_800);
    for (int index = 0; index < 3; index++) {
      setDeadAt(created.get(index), old.plusSeconds(index));
    }
    Instant cutoff = Instant.now().minusSeconds(86_400);
    assertThat(dao.countDeadBefore(cutoff, 2)).isEqualTo(2);

    try (Handle locked = jdbi.open()) {
      locked.begin();
      locked
          .createQuery("SELECT id FROM open_lineage_dead_letters WHERE id = :id FOR UPDATE")
          .bind("id", created.get(0))
          .mapTo(Long.class)
          .one();
      assertThat(dao.purgeDeadBefore(cutoff, 2)).isEqualTo(2);
      assertThat(deadIds()).containsExactly(created.get(0), created.get(3));
      locked.rollback();
    }

    assertThat(dao.purgeDeadBefore(cutoff, 2)).isEqualTo(1);
    assertThat(deadIds()).containsExactly(created.get(3));
  }

  @Test
  void batchClaimUsesTheNarrowAdmissionIndexAtUnrelatedBacklog() throws Exception {
    List<PreparedEvent> batch = new ArrayList<>();
    for (int index = 0; index < 32; index++) {
      batch.add(new PreparedEvent(UUID.randomUUID(), payloadJson(index)));
    }
    dao.enqueueAll(batch);
    jdbi.useHandle(
        handle -> {
          handle.execute(
              "UPDATE open_lineage_queue_heads "
                  + "SET available_at = TIMESTAMPTZ '2000-01-01 00:00:00+00'");
          handle.execute(
              """
              WITH source AS MATERIALIZED (
                SELECT md5('unrelated:' || value)::uuid AS ordering_key
                FROM generate_series(1, 4096) AS generated(value)
              ), inserted AS (
                INSERT INTO open_lineage_queue (ordering_key, event)
                SELECT ordering_key, '{"unrelated":true}'
                FROM source
                RETURNING ordering_key, id
              )
              INSERT INTO open_lineage_queue_heads (ordering_key, event_id, available_at)
              SELECT ordering_key, id, TIMESTAMPTZ '2100-01-01 00:00:00+00'
              FROM inserted
              """);
          handle.execute("ANALYZE open_lineage_queue");
          handle.execute("ANALYZE open_lineage_queue_heads");
        });

    JsonNode claimPlan;
    try (Handle handle = jdbi.open()) {
      handle.begin();
      claimPlan = explainAnalyzedLockNextDueBatch(handle, 32);
      handle.rollback();
    }

    List<JsonNode> nodes = planNodes(claimPlan);
    JsonNode admissionIndex =
        planNodeWithValue(nodes, "Index Name", "open_lineage_queue_admission_idx");
    List<JsonNode> payloadIndexes =
        nodes.stream()
            .filter(node -> "open_lineage_queue_pkey".equals(node.path("Index Name").asText()))
            .toList();
    assertThat(admissionIndex.path("Node Type").asText()).contains("Index");
    assertThat(admissionIndex.path("Actual Rows").asLong()).isBetween(1L, 32L);
    assertThat(payloadIndexes)
        .isNotEmpty()
        .allSatisfy(node -> assertThat(node.path("Actual Rows").asLong()).isBetween(0L, 2L));
    assertThat(planValues(nodes, "Node Type")).doesNotContain("Seq Scan");
    assertThat(claimPlan.path("Actual Rows").asLong()).isEqualTo(32);
    assertQueueIntegrity();
  }

  @Test
  void productionClaimAndFollowerPlansStayBoundedAtBacklog() throws Exception {
    int lockedHeadCount = 8;
    UUID hot = UUID.randomUUID();
    jdbi.useHandle(
        handle -> {
          handle
              .createUpdate(
                  """
                  INSERT INTO open_lineage_queue (ordering_key, event)
                  SELECT :hot, '{"hot":true}'
                  FROM generate_series(1, 10000)
                  """)
              .bind("hot", hot)
              .execute();
          handle
              .createUpdate(
                  """
                  INSERT INTO open_lineage_queue_heads (ordering_key, event_id, available_at)
                  SELECT :hot, min(id), TIMESTAMPTZ '2100-01-01 00:00:00+00'
                  FROM open_lineage_queue
                  WHERE ordering_key = :hot
                  """)
              .bind("hot", hot)
              .execute();
          handle
              .createUpdate(
                  """
                  WITH source AS MATERIALIZED (
                    SELECT md5('locked:' || value)::uuid AS ordering_key,
                           TIMESTAMPTZ '2000-01-01 00:00:00+00' AS available_at
                    FROM generate_series(1, :lockedHeadCount) AS generated(value)
                    UNION ALL
                    SELECT md5('claimable')::uuid,
                           TIMESTAMPTZ '2001-01-01 00:00:00+00'
                    UNION ALL
                    SELECT md5('future:' || value)::uuid,
                           TIMESTAMPTZ '2100-01-01 00:00:00+00'
                    FROM generate_series(1, 2048) AS generated(value)
                  ), inserted AS (
                    INSERT INTO open_lineage_queue (ordering_key, event)
                    SELECT ordering_key, '{"plan":true}'
                    FROM source
                    RETURNING ordering_key, id
                  )
                  INSERT INTO open_lineage_queue_heads (ordering_key, event_id, available_at)
                  SELECT inserted.ordering_key, inserted.id, source.available_at
                  FROM inserted
                  JOIN source USING (ordering_key)
                  """)
              .bind("lockedHeadCount", lockedHeadCount)
              .execute();
          handle.execute("ANALYZE open_lineage_queue");
          handle.execute("ANALYZE open_lineage_queue_heads");
        });

    JsonNode claimPlan;
    String followerPlan;
    try (Handle locked = jdbi.open();
        Handle explaining = jdbi.open()) {
      locked.begin();
      assertThat(
              locked
                  .createQuery(
                      """
                      SELECT ordering_key
                      FROM open_lineage_queue_heads
                      WHERE available_at = TIMESTAMPTZ '2000-01-01 00:00:00+00'
                      FOR UPDATE
                      """)
                  .mapTo(UUID.class)
                  .list())
          .hasSize(lockedHeadCount);

      explaining.begin();
      claimPlan = explainAnalyzedLockNextDue(explaining);
      followerPlan = explainAnalyzedFollowerLookup(explaining, hot);
      explaining.rollback();
      locked.rollback();
    }

    List<JsonNode> claimNodes = planNodes(claimPlan);
    JsonNode dueIndex =
        planNodeWithValue(claimNodes, "Index Name", "open_lineage_queue_heads_due_idx");
    JsonNode payloadIndex = planNodeWithValue(claimNodes, "Index Name", "open_lineage_queue_pkey");
    JsonNode lockRows = planNodeWithValue(claimNodes, "Node Type", "LockRows");

    assertThat(claimPlan.path("Actual Rows").asLong()).isEqualTo(1);
    assertThat(lockRows.path("Actual Rows").asLong()).isEqualTo(1);
    assertThat(dueIndex.path("Node Type").asText()).isEqualTo("Index Scan");
    assertThat(dueIndex.path("Actual Loops").asLong()).isEqualTo(1);
    assertThat(dueIndex.path("Actual Rows").asLong())
        .as("one-row capacity examines only locked predecessors plus the winner")
        .isBetween(1L, lockedHeadCount + 1L);
    assertThat(payloadIndex.path("Actual Loops").asLong())
        .as("the locked candidate drives exactly one payload lookup")
        .isEqualTo(1);
    assertThat(payloadIndex.path("Actual Rows").asLong()).isEqualTo(1);
    assertThat(planValues(claimNodes, "Node Type"))
        .contains("Limit", "LockRows", "Nested Loop")
        .doesNotContain("Sort", "Incremental Sort", "Seq Scan");
    assertThat(planValues(claimNodes, "Index Name"))
        .contains("open_lineage_queue_heads_due_idx", "open_lineage_queue_pkey");
    assertThat(followerPlan).contains("open_lineage_queue_pkey");
    assertQueueIntegrity();
  }

  private static OpenLineageQueueRow ackNext() {
    return jdbi.inTransaction(
        TransactionIsolationLevel.READ_COMMITTED,
        handle -> {
          OpenLineageQueueDao transactional = handle.attach(OpenLineageQueueDao.class);
          OpenLineageQueueRow row = transactional.lockNextDue().orElseThrow();
          transactional.ackLocked(row.orderingKey(), row.id());
          return row;
        });
  }

  private static OpenLineageQueueDao transactionalQueueDaoMock() {
    Handle handle = mock(Handle.class);
    doReturn(TransactionIsolationLevel.READ_COMMITTED).when(handle).getTransactionIsolationLevel();
    OpenLineageQueueDao transactional = mock(OpenLineageQueueDao.class, CALLS_REAL_METHODS);
    doReturn(true).when(transactional).isInTransaction();
    doReturn(handle).when(transactional).getHandle();
    return transactional;
  }

  private static OpenLineageQueueRow lockedRow(long id, UUID orderingKey, Long admissionId) {
    return new OpenLineageQueueRow(id, orderingKey, "{}", 1, admissionId);
  }

  private static OpenLineageQueueRow deadLetterNext(String error) {
    return jdbi.inTransaction(
        TransactionIsolationLevel.READ_COMMITTED,
        handle -> {
          OpenLineageQueueDao transactional = handle.attach(OpenLineageQueueDao.class);
          OpenLineageQueueRow row = transactional.lockNextDue().orElseThrow();
          transactional.deadLetterLocked(row.orderingKey(), row.id(), row.attemptCount(), error);
          return row;
        });
  }

  private static long finishEnqueueBeforeTerminal(UUID key, long first, Terminal terminal)
      throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Handle enqueueHandle = jdbi.open();
        Handle observer = jdbi.open()) {
      enqueueHandle.begin();
      OpenLineageQueueDao enqueueDao = enqueueHandle.attach(OpenLineageQueueDao.class);
      enqueueDao.acquireOrderingKeyLock(key);
      long follower = enqueueDao.insertEventAndMaybeHeadAfterLock(key, payloadJson(2));

      AtomicInteger backendPid = new AtomicInteger();
      CountDownLatch lockedHead = new CountDownLatch(1);
      Future<?> transition =
          executor.submit(
              () -> {
                try (Handle terminalHandle = jdbi.open()) {
                  terminalHandle.begin();
                  backendPid.set(
                      terminalHandle
                          .createQuery("SELECT pg_backend_pid()")
                          .mapTo(Integer.class)
                          .one());
                  OpenLineageQueueDao terminalDao =
                      terminalHandle.attach(OpenLineageQueueDao.class);
                  OpenLineageQueueRow row = terminalDao.lockNextDue().orElseThrow();
                  assertThat(row.id()).isEqualTo(first);
                  lockedHead.countDown();
                  finishLocked(terminalDao, row, terminal);
                  terminalHandle.commit();
                }
              });

      assertThat(lockedHead.await(5, TimeUnit.SECONDS)).isTrue();
      awaitAdvisoryLockWait(observer, backendPid.get());
      assertThat(queueIds(observer)).containsExactly(first);
      enqueueHandle.commit();
      transition.get(5, TimeUnit.SECONDS);
      return follower;
    } finally {
      executor.shutdownNow();
    }
  }

  private static long finishTerminalBeforeEnqueue(UUID key, long first, Terminal terminal)
      throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Handle terminalHandle = jdbi.open();
        Handle observer = jdbi.open()) {
      terminalHandle.begin();
      OpenLineageQueueDao terminalDao = terminalHandle.attach(OpenLineageQueueDao.class);
      OpenLineageQueueRow row = terminalDao.lockNextDue().orElseThrow();
      assertThat(row.id()).isEqualTo(first);
      terminalDao.acquireOrderingKeyLock(key);

      AtomicInteger backendPid = new AtomicInteger();
      CountDownLatch ready = new CountDownLatch(1);
      Future<Long> enqueue =
          executor.submit(
              () -> {
                try (Handle enqueueHandle = jdbi.open()) {
                  backendPid.set(
                      enqueueHandle
                          .createQuery("SELECT pg_backend_pid()")
                          .mapTo(Integer.class)
                          .one());
                  ready.countDown();
                  return enqueueHandle
                      .attach(OpenLineageQueueDao.class)
                      .enqueue(key, payloadJson(2));
                }
              });

      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      awaitAdvisoryLockWait(observer, backendPid.get());
      assertThat(queueIds(observer)).containsExactly(first);
      finishLocked(terminalDao, row, terminal);
      terminalHandle.commit();
      return enqueue.get(5, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  private static void finishLocked(
      OpenLineageQueueDao transactional, OpenLineageQueueRow row, Terminal terminal) {
    if (terminal == Terminal.ACK) {
      transactional.ackLocked(row.orderingKey(), row.id());
    } else {
      transactional.deadLetterLocked(
          row.orderingKey(), row.id(), row.attemptCount(), "concurrent terminal");
    }
  }

  private static void assertConcurrentEnqueueAfterFirstTransaction(boolean commit)
      throws Exception {
    UUID key = UUID.randomUUID();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Handle firstHandle = jdbi.open();
        Handle observer = jdbi.open()) {
      firstHandle.begin();
      OpenLineageQueueDao firstDao = firstHandle.attach(OpenLineageQueueDao.class);
      firstDao.acquireOrderingKeyLock(key);
      long first = firstDao.insertEventAndMaybeHeadAfterLock(key, payloadJson(1));

      AtomicInteger backendPid = new AtomicInteger();
      CountDownLatch ready = new CountDownLatch(1);
      Future<Long> second =
          executor.submit(
              () -> {
                try (Handle secondHandle = jdbi.open()) {
                  backendPid.set(
                      secondHandle
                          .createQuery("SELECT pg_backend_pid()")
                          .mapTo(Integer.class)
                          .one());
                  ready.countDown();
                  return secondHandle
                      .attach(OpenLineageQueueDao.class)
                      .enqueue(key, payloadJson(2));
                }
              });

      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      awaitAdvisoryLockWait(observer, backendPid.get());
      assertThat(queueIds(observer)).isEmpty();

      if (commit) {
        firstHandle.commit();
      } else {
        firstHandle.rollback();
      }
      long secondId = second.get(5, TimeUnit.SECONDS);

      if (commit) {
        assertThat(queueIds(observer)).containsExactly(first, secondId);
        assertThat(headId(observer, key)).isEqualTo(first);
      } else {
        assertThat(queueIds(observer)).containsExactly(secondId);
        assertThat(headId(observer, key)).isEqualTo(secondId);
      }
      assertQueueIntegrity(observer);
    } finally {
      executor.shutdownNow();
    }
  }

  private static void assertConcurrentBulkEnqueueAfterFirstTransaction(boolean commit)
      throws Exception {
    UUID key = UUID.randomUUID();
    List<PreparedEvent> batch =
        List.of(new PreparedEvent(key, payloadJson(2)), new PreparedEvent(key, payloadJson(3)));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Handle firstHandle = jdbi.open();
        Handle observer = jdbi.open()) {
      firstHandle.begin();
      OpenLineageQueueDao firstDao = firstHandle.attach(OpenLineageQueueDao.class);
      firstDao.acquireOrderingKeyLock(key);
      firstDao.insertEventAndMaybeHeadAfterLock(key, payloadJson(1));

      AtomicInteger backendPid = new AtomicInteger();
      CountDownLatch ready = new CountDownLatch(1);
      Future<Integer> admitted =
          executor.submit(
              () -> {
                try (Handle bulkHandle = jdbi.open()) {
                  backendPid.set(
                      bulkHandle.createQuery("SELECT pg_backend_pid()").mapTo(Integer.class).one());
                  ready.countDown();
                  return bulkHandle.attach(OpenLineageQueueDao.class).enqueueAll(batch);
                }
              });

      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      awaitAdvisoryLockWait(observer, backendPid.get());
      assertThat(queueIds(observer)).isEmpty();

      if (commit) {
        firstHandle.commit();
      } else {
        firstHandle.rollback();
      }
      assertThat(admitted.get(5, TimeUnit.SECONDS)).isEqualTo(2);

      assertThat(lanePayloads(observer, key))
          .containsExactlyElementsOf(
              commit
                  ? List.of(payloadJson(1), payloadJson(2), payloadJson(3))
                  : List.of(payloadJson(2), payloadJson(3)));
      assertThat(headId(observer, key)).isEqualTo(laneIds(observer, key).get(0));
      assertQueueIntegrity(observer);
    } finally {
      executor.shutdownNow();
    }
  }

  private static int enqueueBulkAfterSignal(
      List<PreparedEvent> batch, CountDownLatch ready, CountDownLatch start) throws Exception {
    try (Handle handle = jdbi.open()) {
      OpenLineageQueueDao transactional = handle.attach(OpenLineageQueueDao.class);
      ready.countDown();
      if (!start.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("timed out waiting to start concurrent bulk enqueue");
      }
      return transactional.enqueueAll(batch);
    }
  }

  private static void awaitAdvisoryLockWait(Handle observer, int backendPid) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    boolean waiting;
    do {
      waiting =
          observer
              .createQuery(
                  """
                  SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE pid = :pid
                      AND wait_event_type = 'Lock'
                      AND wait_event = 'advisory'
                      AND cardinality(pg_blocking_pids(pid)) > 0)
                  """)
              .bind("pid", backendPid)
              .mapTo(Boolean.class)
              .one();
    } while (!waiting && System.nanoTime() < deadline);
    assertThat(waiting)
        .as("backend %s should wait for the lane advisory lock", backendPid)
        .isTrue();
  }

  private static void assertQueueIntegrity() {
    jdbi.useHandle(OpenLineageQueueDaoTest::assertQueueIntegrity);
  }

  private static void assertQueueIntegrity(Handle handle) {
    long invalidLaneCount =
        handle
            .createQuery(
                """
                SELECT count(*)
                FROM (
                  SELECT queued.ordering_key
                  FROM open_lineage_queue AS queued
                  LEFT JOIN open_lineage_queue_heads AS head
                    ON head.ordering_key = queued.ordering_key
                  GROUP BY queued.ordering_key
                  HAVING count(DISTINCT head.ordering_key) <> 1
                     OR min(queued.id) <> min(head.event_id)
                ) AS invalid
                """)
            .mapTo(Long.class)
            .one();
    long orphanOrInvalidHeads =
        handle
            .createQuery(
                """
                SELECT count(*)
                FROM open_lineage_queue_heads AS head
                LEFT JOIN open_lineage_queue AS queued
                  ON queued.ordering_key = head.ordering_key
                 AND queued.id = head.event_id
                WHERE queued.id IS NULL
                   OR head.attempt_count < 0
                """)
            .mapTo(Long.class)
            .one();

    assertThat(invalidLaneCount).isZero();
    assertThat(orphanOrInvalidHeads).isZero();
  }

  private static JsonNode explainAnalyzedLockNextDue(Handle handle) throws Exception {
    String planJson =
        handle
            .createQuery(
                "EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, TIMING OFF, SUMMARY OFF) "
                    + OpenLineageQueueDao.LOCK_NEXT_DUE_SQL)
            .mapTo(String.class)
            .one();
    return Utils.getMapper().readTree(planJson).path(0).path("Plan");
  }

  private static JsonNode explainAnalyzedLockNextDueBatch(Handle handle, int maxEvents)
      throws Exception {
    String planJson =
        handle
            .createQuery(
                "EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, TIMING OFF, SUMMARY OFF) "
                    + OpenLineageQueueDao.LOCK_NEXT_DUE_BATCH_SQL)
            .bind("maxEvents", maxEvents)
            .mapTo(String.class)
            .one();
    return Utils.getMapper().readTree(planJson).path(0).path("Plan");
  }

  private static String explainAnalyzedFollowerLookup(Handle handle, UUID key) {
    return String.join(
        "\n",
        handle
            .createQuery(
                """
                EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
                SELECT id
                FROM open_lineage_queue
                WHERE ordering_key = :key
                  AND id > 0
                ORDER BY id
                LIMIT 1
                """)
            .bind("key", key)
            .mapTo(String.class)
            .list());
  }

  private static List<JsonNode> planNodes(JsonNode root) {
    List<JsonNode> nodes = new ArrayList<>();
    collectPlanNodes(root, nodes);
    return nodes;
  }

  private static void collectPlanNodes(JsonNode node, List<JsonNode> nodes) {
    if (!node.isObject()) {
      return;
    }
    nodes.add(node);
    node.path("Plans").forEach(child -> collectPlanNodes(child, nodes));
  }

  private static JsonNode planNodeWithValue(
      List<JsonNode> nodes, String fieldName, String expectedValue) {
    return nodes.stream()
        .filter(node -> expectedValue.equals(node.path(fieldName).asText()))
        .findFirst()
        .orElseThrow(
            () ->
                new AssertionError("missing plan node with " + fieldName + " = " + expectedValue));
  }

  private static List<String> planValues(List<JsonNode> nodes, String fieldName) {
    List<String> values = new ArrayList<>();
    for (JsonNode node : nodes) {
      if (node.has(fieldName)) {
        values.add(node.path(fieldName).asText());
      }
    }
    return values;
  }

  private static UUID orderingKey(long id) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT ordering_key FROM open_lineage_queue WHERE id = :id")
                .bind("id", id)
                .mapTo(UUID.class)
                .one());
  }

  private static long headId(UUID orderingKey) {
    return jdbi.withHandle(handle -> headId(handle, orderingKey));
  }

  private static long headId(Handle handle, UUID orderingKey) {
    return handle
        .createQuery("SELECT event_id FROM open_lineage_queue_heads WHERE ordering_key = :key")
        .bind("key", orderingKey)
        .mapTo(Long.class)
        .one();
  }

  private static int headAttempt(UUID orderingKey) {
    return headState(orderingKey).attemptCount();
  }

  private static HeadState headState(UUID orderingKey) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT event_id,
                           available_at,
                           attempt_count,
                           refresh_due_on_advance,
                           last_error
                    FROM open_lineage_queue_heads
                    WHERE ordering_key = :key
                    """)
                .bind("key", orderingKey)
                .map(
                    (resultSet, context) ->
                        new HeadState(
                            resultSet.getLong("event_id"),
                            resultSet.getTimestamp("available_at").toInstant(),
                            resultSet.getInt("attempt_count"),
                            resultSet.getBoolean("refresh_due_on_advance"),
                            resultSet.getString("last_error")))
                .one());
  }

  private static String headAvailableAtBytes(UUID orderingKey) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT encode(timestamptz_send(available_at), 'hex')
                    FROM open_lineage_queue_heads
                    WHERE ordering_key = :key
                    """)
                .bind("key", orderingKey)
                .mapTo(String.class)
                .one());
  }

  private static void setHeadAvailableAt(UUID orderingKey, Instant availableAt) {
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    "UPDATE open_lineage_queue_heads "
                        + "SET available_at = :availableAt WHERE ordering_key = :key")
                .bind("availableAt", availableAt)
                .bind("key", orderingKey)
                .execute());
  }

  private static void setAllHeadsAvailableAt(Instant availableAt) {
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate("UPDATE open_lineage_queue_heads SET available_at = :availableAt")
                .bind("availableAt", availableAt)
                .execute());
  }

  private static void makeScheduleDue(UUID orderingKey) {
    setHeadAvailableAt(orderingKey, Instant.parse("2000-01-01T00:00:00Z"));
  }

  private static void assertMillisecondAligned(Instant value) {
    assertThat(value.getNano() % 1_000_000).isZero();
  }

  private static String payloadJson(long position) {
    return "{\"position\":" + position + "}";
  }

  private static String eventJson(Handle handle, long id) {
    return handle
        .createQuery("SELECT event FROM open_lineage_queue WHERE id = :id")
        .bind("id", id)
        .mapTo(String.class)
        .one();
  }

  private static String eventJson(long id) {
    return jdbi.withHandle(handle -> eventJson(handle, id));
  }

  private static Long admissionId(long id) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT admission_id FROM open_lineage_queue WHERE id = :id")
                .bind("id", id)
                .map((resultSet, context) -> (Long) resultSet.getObject("admission_id"))
                .one());
  }

  private static Long deadAdmissionId(long id) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT admission_id FROM open_lineage_dead_letters WHERE id = :id")
                .bind("id", id)
                .map((resultSet, context) -> (Long) resultSet.getObject("admission_id"))
                .one());
  }

  private static List<Long> laneIds(UUID orderingKey) {
    return jdbi.withHandle(handle -> laneIds(handle, orderingKey));
  }

  private static List<Long> laneIds(Handle handle, UUID orderingKey) {
    return handle
        .createQuery("SELECT id FROM open_lineage_queue WHERE ordering_key = :key ORDER BY id")
        .bind("key", orderingKey)
        .mapTo(Long.class)
        .list();
  }

  private static List<String> lanePayloads(UUID orderingKey) {
    return jdbi.withHandle(handle -> lanePayloads(handle, orderingKey));
  }

  private static List<String> lanePayloads(Handle handle, UUID orderingKey) {
    return handle
        .createQuery("SELECT event FROM open_lineage_queue WHERE ordering_key = :key ORDER BY id")
        .bind("key", orderingKey)
        .mapTo(String.class)
        .list();
  }

  private static void setDeadAt(long id, Instant deadAt) {
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    "UPDATE open_lineage_dead_letters SET dead_at = :deadAt WHERE id = :id")
                .bind("deadAt", deadAt)
                .bind("id", id)
                .execute());
  }

  private static List<Long> queueIds() {
    return jdbi.withHandle(OpenLineageQueueDaoTest::queueIds);
  }

  private static List<Long> queueIds(Handle handle) {
    return handle
        .createQuery("SELECT id FROM open_lineage_queue ORDER BY id")
        .mapTo(Long.class)
        .list();
  }

  private static long headCount() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT count(*) FROM open_lineage_queue_heads")
                .mapTo(Long.class)
                .one());
  }

  private static List<Long> deadIds() {
    return jdbi.withHandle(OpenLineageQueueDaoTest::deadIds);
  }

  private static List<Long> deadIds(Handle handle) {
    return handle
        .createQuery("SELECT id FROM open_lineage_dead_letters ORDER BY dead_at, id")
        .mapTo(Long.class)
        .list();
  }

  private record HeadState(
      long eventId,
      Instant availableAt,
      int attemptCount,
      boolean refreshDueOnAdvance,
      String lastError) {}

  private enum Terminal {
    ACK,
    DEAD_LETTER
  }

  private static LineageEvent runEvent(String runId, String eventType) {
    return runEvent(runId, eventType, job("namespace", "job"));
  }

  private static LineageEvent runEvent(
      String runId, String eventType, String namespace, String name) {
    return runEvent(runId, eventType, job(namespace, name));
  }

  private static LineageEvent runEvent(String runId, String eventType, LineageEvent.Job job) {
    return LineageEvent.builder()
        .eventType(eventType)
        .eventTime(EVENT_TIME.atZone(UTC))
        .run(new LineageEvent.Run(runId, null))
        .job(job)
        .producer(PRODUCER)
        .schemaURL(RUN_SCHEMA)
        .build();
  }

  private static JobEvent jobEvent(String namespace, String name) {
    return jobEvent(job(namespace, name));
  }

  private static JobEvent jobEvent(LineageEvent.Job job) {
    return JobEvent.builder()
        .eventTime(EVENT_TIME.atZone(UTC))
        .job(job)
        .producer(PRODUCER)
        .schemaURL(JOB_SCHEMA)
        .build();
  }

  private static LineageEvent.Job job(String namespace, String name) {
    return LineageEvent.Job.builder().namespace(namespace).name(name).build();
  }

  private static DatasetEvent datasetEvent(String namespace, String name) {
    return DatasetEvent.builder()
        .eventTime(EVENT_TIME.atZone(UTC))
        .dataset(LineageEvent.Dataset.builder().namespace(namespace).name(name).build())
        .producer(PRODUCER)
        .schemaURL(DATASET_SCHEMA)
        .build();
  }
}
