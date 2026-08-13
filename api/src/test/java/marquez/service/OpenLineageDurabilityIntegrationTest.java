/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static marquez.service.models.ServiceModelGenerator.newDbTableMetaWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;
import marquez.api.JdbiUtils;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.JobName;
import marquez.common.models.JobType;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunState;
import marquez.db.DatasetDao;
import marquez.db.JobDao;
import marquez.db.NamespaceDao;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageQueueDao;
import marquez.db.RunArgsDao;
import marquez.db.RunDao;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.OpenLineageQueueRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.UpdateLineageRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.BaseEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.jdbi.v3.jackson2.Jackson2Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

@org.junit.jupiter.api.Tag("IntegrationTests")
@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class OpenLineageDurabilityIntegrationTest {
  private static final Instant EVENT_TIME = Instant.parse("2026-08-11T00:00:00Z");
  private static final String PRODUCER = "https://example.com/producer";
  private static final String NAMESPACE = "durability";
  private static final URI RUN_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/RunEvent");
  private static final URI JOB_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/JobEvent");
  private static final URI FACET_SCHEMA =
      URI.create("https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json");
  private static final UUID LWW_RUN_A = UUID.fromString("10000000-0000-4000-8000-000000000001");
  private static final UUID LWW_RUN_B = UUID.fromString("10000000-0000-4000-8000-000000000002");
  private static final String LWW_JOB = "lww-job";
  private static final String LWW_SHARED_OUTPUT = "lww-shared-output";

  private final List<OpenLineageWorker> workers = new ArrayList<>();
  private Jdbi jdbi;
  private OpenLineageQueueDao queueDao;

  @BeforeEach
  void setUp(Jdbi configuredJdbi) {
    jdbi = configuredJdbi;
    JdbiUtils.cleanDatabase(jdbi);
    queueDao = jdbi.onDemand(OpenLineageQueueDao.class);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    InterruptedException interruption = null;
    for (OpenLineageWorker worker : workers) {
      try {
        worker.stop();
      } catch (InterruptedException interrupted) {
        interruption = interrupted;
      }
    }
    JdbiUtils.cleanDatabase(jdbi);
    if (interruption != null) {
      throw interruption;
    }
  }

  @Test
  void intakeCommitIsVisibleBeforeWakeOnIndependentConnection() {
    LineageEvent event =
        runEvent(UUID.randomUUID(), "START", EVENT_TIME, "committed-before-wake-job");
    OpenLineageQueueDao.PreparedEvent prepared = OpenLineageQueueDao.prepare(event);
    AtomicInteger wakeCount = new AtomicInteger();
    AtomicReference<AdmissionState> observedAtWake = new AtomicReference<>();

    // Holding this handle forces admission onto another physical connection. Its autocommit query
    // can observe the row during wake only if the admission transaction has already committed.
    try (Handle observer = jdbi.open()) {
      OpenLineageIntake intake =
          new OpenLineageIntake(
              jdbi.onDemand(OpenLineageQueueDao.class),
              () -> {
                wakeCount.incrementAndGet();
                observedAtWake.set(admissionState(observer, prepared.orderingKey()));
              });

      long eventId = intake.enqueue(prepared);

      assertThat(wakeCount).hasValue(1);
      assertThat(observedAtWake.get())
          .isEqualTo(new AdmissionState(eventId, eventId, prepared.eventJson(), 1));
    }
  }

  @Test
  void intakeLostCommitResponseLeavesDurableEventWithoutWake() {
    LineageEvent event =
        runEvent(UUID.randomUUID(), "START", EVENT_TIME, "ambiguous-intake-commit-job");
    OpenLineageQueueDao.PreparedEvent prepared = OpenLineageQueueDao.prepare(event);
    AtomicInteger wakeCount = new AtomicInteger();

    try (Handle connectionOwner = jdbi.open()) {
      Connection ambiguousConnection =
          connectionThatReportsLossAfterCommit(connectionOwner.getConnection());
      Jdbi ambiguousJdbi = Jdbi.create(ambiguousConnection).installPlugins();
      ambiguousJdbi.getConfig(Jackson2Config.class).setMapper(Utils.getMapper());
      OpenLineageQueueDao ambiguousQueueDao = ambiguousJdbi.onDemand(OpenLineageQueueDao.class);
      OpenLineageIntake intake =
          new OpenLineageIntake(ambiguousQueueDao, wakeCount::incrementAndGet);

      assertThatThrownBy(() -> intake.enqueue(prepared))
          .hasRootCauseMessage("simulated connection loss after PostgreSQL commit");
      assertThat(wakeCount).hasValue(0);
    }

    AdmissionState durable =
        jdbi.withHandle(handle -> admissionState(handle, prepared.orderingKey()));
    assertThat(durable.eventJson()).isEqualTo(prepared.eventJson());
    assertThat(durable.headEventId()).isEqualTo(durable.eventId());
    assertThat(durable.payloadCount()).isEqualTo(1);
  }

  @Test
  void intakePreCommitFailureRollsBackWithoutWakeOrQueueState() {
    LineageEvent event =
        runEvent(UUID.randomUUID(), "START", EVENT_TIME, "failed-intake-commit-job");
    OpenLineageQueueDao.PreparedEvent prepared = OpenLineageQueueDao.prepare(event);
    AtomicInteger wakeCount = new AtomicInteger();
    AtomicInteger commitCalls = new AtomicInteger();
    AtomicInteger rollbackCalls = new AtomicInteger();

    try (Handle connectionOwner = jdbi.open()) {
      Connection failingConnection =
          connectionThatLosesInsertResponseBeforeCommit(
              connectionOwner.getConnection(), commitCalls, rollbackCalls);
      Jdbi failingJdbi = Jdbi.create(failingConnection).installPlugins();
      failingJdbi.getConfig(Jackson2Config.class).setMapper(Utils.getMapper());
      OpenLineageQueueDao failingQueueDao = failingJdbi.onDemand(OpenLineageQueueDao.class);
      OpenLineageIntake intake = new OpenLineageIntake(failingQueueDao, wakeCount::incrementAndGet);

      assertThatThrownBy(() -> intake.enqueue(prepared))
          .hasRootCauseMessage("simulated insert response loss before PostgreSQL commit");
      assertThat(wakeCount).hasValue(0);
      assertThat(commitCalls).hasValue(0);
      assertThat(rollbackCalls).hasValue(1);
    }

    assertThat(liveEventIds()).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(deadLetterCount()).isZero();
  }

  @Test
  void backendTerminationAfterProjectionRollsBackWithoutCountingAttempt() throws Exception {
    UUID runId = UUID.randomUUID();
    LineageEvent event = runEvent(runId, "START", EVENT_TIME, "terminated-projection-job");
    UUID queueKey = OpenLineageQueueDao.orderingKeyFor(event);
    String eventJson = Utils.toJson(event);
    long eventId = queueDao.enqueue(event);

    long advisoryKey = 0x4f4c5f54584eL;
    CountDownLatch projected = new CountDownLatch(1);
    AtomicInteger processingPid = new AtomicInteger();
    AdvisoryBlockedAfterProjectionService service =
        new AdvisoryBlockedAfterProjectionService(
            baseDao(), runService(), projected, processingPid, advisoryKey);
    ExecutorService processingThread = Executors.newSingleThreadExecutor();
    CompletableFuture<TransactionResult> result;
    try (Handle blocker = jdbi.open()) {
      int blockerPid = blocker.createQuery("SELECT pg_backend_pid()").mapTo(Integer.class).one();
      blocker
          .createQuery("SELECT pg_advisory_lock(:advisoryKey)")
          .bind("advisoryKey", advisoryKey)
          .map((resultSet, context) -> true)
          .one();

      result = CompletableFuture.supplyAsync(() -> processNext(service, 3, 0), processingThread);
      awaitLatch(projected, "projection before backend termination");
      awaitBlockedBy(processingPid.get(), blockerPid);
      boolean terminated =
          jdbi.withHandle(
              handle ->
                  handle
                      .createQuery("SELECT pg_terminate_backend(:pid)")
                      .bind("pid", processingPid.get())
                      .mapTo(Boolean.class)
                      .one());
      assertThat(terminated).isTrue();
      assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
          .isInstanceOf(ExecutionException.class);
    } finally {
      processingThread.shutdownNow();
      assertThat(processingThread.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(liveEventIds()).containsExactly(eventId);
    assertThat(queuedEventJson(queueKey, eventId)).isEqualTo(eventJson);
    assertThat(head(queueKey)).isEqualTo(new HeadState(eventId, 0, null));
    assertThat(headClock(queueKey).scheduleDue()).isTrue();
    assertThat(rawEventCount(runId)).isZero();
    assertThat(runCount(runId)).isZero();
    assertThat(deadLetterCount()).isZero();

    TransactionResult recovered = processNext(newService(), 3, 0);
    assertThat(recovered.outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(recovered.row().id()).isEqualTo(eventId);
    assertThat(recovered.row().attemptCount()).isEqualTo(1);
    assertThat(liveEventIds()).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(rawEventCount(runId)).isEqualTo(1);
  }

  @Test
  void lostCommitResponseStillLeavesOneAtomicCommittedOutcome() throws Exception {
    UUID runId = UUID.randomUUID();
    LineageEvent event = runEvent(runId, "START", EVENT_TIME, "ambiguous-commit-job");
    String eventJson = Utils.toJson(event);
    long eventId = queueDao.enqueue(event);

    try (Handle connectionOwner = jdbi.open()) {
      Connection ambiguousConnection =
          connectionThatReportsLossAfterCommit(connectionOwner.getConnection());
      Jdbi ambiguousJdbi = Jdbi.create(ambiguousConnection).installPlugins();
      ambiguousJdbi.getConfig(Jackson2Config.class).setMapper(Utils.getMapper());
      MetricRegistry metrics = new MetricRegistry();
      OpenLineageService service = spy(newService());
      OpenLineageWorker ambiguousWorker =
          new OpenLineageWorker(
              ambiguousJdbi, queueDao, service, workerConfig(1, 3, 1_000), metrics);
      workers.add(ambiguousWorker);

      assertThatThrownBy(() -> ambiguousWorker.processTask(() -> true))
          .isInstanceOf(RuntimeException.class)
          .hasRootCauseMessage("simulated connection loss after PostgreSQL commit");

      assertThat(ambiguousWorker.healthStatus().message())
          .isEqualTo("OpenLineage worker task failed");
      assertThat(ambiguousWorker.healthStatus().failure())
          .hasRootCauseMessage("simulated connection loss after PostgreSQL commit");
      assertThat(ambiguousWorker.activeConnectionCount()).isZero();
      assertThat(metrics.meter(workerMetricName("selected")).getCount()).isEqualTo(1);
      assertThat(metrics.meter(workerMetricName("task_failed")).getCount()).isEqualTo(1);
      assertThat(metrics.meter(workerMetricName("succeeded")).getCount()).isZero();
      assertThat(metrics.meter(workerMetricName("retried")).getCount()).isZero();
      assertThat(metrics.meter(workerMetricName("dead_lettered")).getCount()).isZero();
      verify(service, never()).publishQueuedEventBestEffort(any(), any());
    }

    assertThat(liveEventIds()).doesNotContain(eventId).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(rawEventCount(runId)).isEqualTo(1);
    assertThat(runCount(runId)).isEqualTo(1);
    assertThat(rawEventPayloads(runId))
        .extracting(OpenLineageDurabilityIntegrationTest::jsonTree)
        .containsExactly(jsonTree(eventJson));
    assertThat(deadLetterCount()).isZero();
  }

  @Test
  void rawAndSanitizedDatasetNamespacesShareOneCanonicalDataset() throws Exception {
    String rawNamespace = "dataset://warehouse;tenant=one";
    String canonicalNamespace = Utils.sanitizeOpenLineageNamespace(rawNamespace);
    String datasetName = "canonical-output";
    UUID rawRunId = UUID.randomUUID();
    UUID canonicalRunId = UUID.randomUUID();

    queueDao.enqueue(
        outputEvent(rawRunId, EVENT_TIME, "canonical-dataset-job", rawNamespace, datasetName));
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);

    queueDao.enqueue(
        outputEvent(
            canonicalRunId,
            EVENT_TIME.plusSeconds(1),
            "canonical-dataset-job",
            canonicalNamespace,
            datasetName));
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);

    List<SymlinkState> symlinks =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT namespace.name AS namespace_name,
                               symlink.is_primary,
                               symlink.dataset_uuid
                        FROM dataset_symlinks AS symlink
                        JOIN namespaces AS namespace ON namespace.uuid = symlink.namespace_uuid
                        WHERE symlink.name = :datasetName
                          AND namespace.name IN (:rawNamespace, :canonicalNamespace)
                        ORDER BY symlink.is_primary, namespace.name
                        """)
                    .bind("datasetName", datasetName)
                    .bind("rawNamespace", rawNamespace)
                    .bind("canonicalNamespace", canonicalNamespace)
                    .map(
                        (resultSet, context) ->
                            new SymlinkState(
                                resultSet.getString("namespace_name"),
                                resultSet.getBoolean("is_primary"),
                                resultSet.getObject("dataset_uuid", UUID.class)))
                    .list());

    assertThat(symlinks).hasSize(2);
    UUID canonicalDatasetId = symlinks.get(0).datasetId();
    assertThat(symlinks)
        .containsExactly(
            new SymlinkState(rawNamespace, false, canonicalDatasetId),
            new SymlinkState(canonicalNamespace, true, canonicalDatasetId));
    long canonicalDatasetCount =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT count(*)
                        FROM datasets
                        WHERE namespace_name = :namespace AND name = :name
                        """)
                    .bind("namespace", canonicalNamespace)
                    .bind("name", datasetName)
                    .mapTo(Long.class)
                    .one());
    assertThat(canonicalDatasetCount).isEqualTo(1);
  }

  @Test
  void rawAliasDoesNotDisplaceAnotherDatasetsPrimaryIdentity() throws Exception {
    String rawNamespace = "dataset://warehouse;tenant=legacy";
    String canonicalNamespace = Utils.sanitizeOpenLineageNamespace(rawNamespace);
    String datasetName = "primary-collision";
    DatasetDao datasetDao = jdbi.onDemand(DatasetDao.class);
    datasetDao.upsertDatasetMeta(
        NamespaceName.of(rawNamespace),
        DatasetName.of(datasetName),
        newDbTableMetaWith(datasetName));
    UUID legacyDatasetUuid =
        datasetDao.findDatasetAsRow(rawNamespace, datasetName).orElseThrow().getUuid();

    queueDao.enqueue(
        outputEvent(
            UUID.randomUUID(), EVENT_TIME, "canonical-collision-job", rawNamespace, datasetName));
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);

    List<SymlinkState> symlinks =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT namespace.name AS namespace_name,
                               symlink.is_primary,
                               symlink.dataset_uuid
                        FROM dataset_symlinks AS symlink
                        JOIN namespaces AS namespace ON namespace.uuid = symlink.namespace_uuid
                        WHERE symlink.name = :datasetName
                          AND namespace.name IN (:rawNamespace, :canonicalNamespace)
                        ORDER BY namespace.name
                        """)
                    .bind("datasetName", datasetName)
                    .bind("rawNamespace", rawNamespace)
                    .bind("canonicalNamespace", canonicalNamespace)
                    .map(
                        (resultSet, context) ->
                            new SymlinkState(
                                resultSet.getString("namespace_name"),
                                resultSet.getBoolean("is_primary"),
                                resultSet.getObject("dataset_uuid", UUID.class)))
                    .list());

    assertThat(symlinks).hasSize(2);
    SymlinkState rawPrimary =
        symlinks.stream()
            .filter(symlink -> symlink.namespace().equals(rawNamespace))
            .findFirst()
            .orElseThrow();
    SymlinkState canonicalPrimary =
        symlinks.stream()
            .filter(symlink -> symlink.namespace().equals(canonicalNamespace))
            .findFirst()
            .orElseThrow();
    assertThat(rawPrimary).isEqualTo(new SymlinkState(rawNamespace, true, legacyDatasetUuid));
    assertThat(canonicalPrimary.primary()).isTrue();
    assertThat(canonicalPrimary.datasetId()).isNotEqualTo(legacyDatasetUuid);
  }

  @Test
  void orderedJobEventCurrentIoIsAnExactWinningSnapshot() throws Exception {
    String jobName = "job-event-io-" + UUID.randomUUID();

    queueDao.enqueue(jobEvent(EVENT_TIME, jobName, List.of("full-input"), List.of("full-output")));
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(currentJobIo(jobName))
        .containsExactlyInAnyOrder(
            new IoRef("INPUT", NAMESPACE, "full-input"),
            new IoRef("OUTPUT", NAMESPACE, "full-output"));

    queueDao.enqueue(jobEvent(EVENT_TIME.plusSeconds(2), jobName, List.of("new-input"), List.of()));
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(currentJobIo(jobName)).containsExactly(new IoRef("INPUT", NAMESPACE, "new-input"));

    queueDao.enqueue(jobEvent(EVENT_TIME.plusSeconds(3), jobName, List.of(), List.of()));
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(currentJobIo(jobName)).isEmpty();

    queueDao.enqueue(
        jobEvent(
            EVENT_TIME.plusSeconds(1), jobName, List.of("stale-input"), List.of("stale-output")));
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(currentJobIo(jobName)).isEmpty();
  }

  @Test
  void orderedAliasJobEventProjectsCanonicalSnapshotVersionAndIo() throws Exception {
    String suffix = UUID.randomUUID().toString();
    NamespaceRow namespace =
        jdbi.onDemand(NamespaceDao.class)
            .upsertNamespaceRow(UUID.randomUUID(), Instant.now(), NAMESPACE, getClass().getName());
    String primaryName = "durable-primary-" + suffix;
    String aliasName = "durable-alias-" + suffix;
    JobDao jobDao = jdbi.onDemand(JobDao.class);
    Instant seededAt = Instant.now();
    JobRow primary =
        jobDao.upsertJob(
            UUID.randomUUID(),
            JobType.BATCH,
            seededAt,
            namespace.getUuid(),
            namespace.getName(),
            primaryName,
            "canonical target",
            null,
            null,
            jobDao.toJson(Collections.emptySet(), Utils.getMapper()));
    jobDao.upsertJob(
        UUID.randomUUID(),
        JobType.BATCH,
        seededAt,
        namespace.getUuid(),
        namespace.getName(),
        aliasName,
        "alias row",
        null,
        primary.getUuid(),
        jobDao.toJson(Collections.emptySet(), Utils.getMapper()));
    Instant winnerTime = Instant.now().plusSeconds(10);

    queueDao.enqueue(jobEvent(winnerTime, aliasName, List.of("alias-input"), List.of()));
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(currentJobIo(primaryName))
        .containsExactly(new IoRef("INPUT", NAMESPACE, "alias-input"));

    queueDao.enqueue(
        jobEvent(winnerTime.minusSeconds(1), primaryName, List.of("stale-input"), List.of()));
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(currentJobIo(primaryName))
        .containsExactly(new IoRef("INPUT", NAMESPACE, "alias-input"));
  }

  @ParameterizedTest(name = "older JobEvent commits first: {0}")
  @ValueSource(booleans = {true, false})
  void orderedJobEventVersionsUseTheirOwnLocationsAcrossAliasLanes(boolean olderCommitsFirst)
      throws Exception {
    String suffix = UUID.randomUUID().toString();
    NamespaceRow namespace =
        jdbi.onDemand(NamespaceDao.class)
            .upsertNamespaceRow(
                UUID.randomUUID(), EVENT_TIME.minusSeconds(20), NAMESPACE, getClass().getName());
    String primaryName = "location-primary-" + suffix;
    String aliasName = "location-alias-" + suffix;
    JobDao jobDao = jdbi.onDemand(JobDao.class);
    JobRow primary =
        jobDao.upsertJob(
            UUID.randomUUID(),
            JobType.BATCH,
            EVENT_TIME.minusSeconds(10),
            namespace.getUuid(),
            namespace.getName(),
            primaryName,
            "seed",
            null,
            null,
            jobDao.toJson(Collections.emptySet(), Utils.getMapper()));
    jobDao.upsertJob(
        UUID.randomUUID(),
        JobType.BATCH,
        EVENT_TIME.minusSeconds(10),
        namespace.getUuid(),
        namespace.getName(),
        aliasName,
        "alias",
        null,
        primary.getUuid(),
        jobDao.toJson(Collections.emptySet(), Utils.getMapper()));
    String olderLocation = "https://example.com/job-event/older";
    String newerLocation = "https://example.com/job-event/newer";
    JobEvent older =
        jobEvent(
            EVENT_TIME, primaryName, List.of("job-event-input-older"), List.of(), olderLocation);
    JobEvent newer =
        jobEvent(
            EVENT_TIME.plusSeconds(1),
            aliasName,
            List.of("job-event-input-newer"),
            List.of(),
            newerLocation);
    queueDao.enqueue(older);
    queueDao.enqueue(newer);
    UUID olderKey = OpenLineageQueueDao.orderingKeyFor(older);
    UUID newerKey = OpenLineageQueueDao.orderingKeyFor(newer);
    Predicate<BaseEvent> olderEvent =
        event -> ((JobEvent) event).getJob().getName().equals(primaryName);
    Predicate<BaseEvent> newerEvent =
        event -> ((JobEvent) event).getJob().getName().equals(aliasName);

    if (olderCommitsFirst) {
      projectCrossLane(olderKey, olderEvent, newerKey, newerEvent);
    } else {
      projectCrossLane(newerKey, newerEvent, olderKey, olderEvent);
    }

    String currentLocation =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery("SELECT current_location FROM jobs WHERE uuid = :jobUuid")
                    .bind("jobUuid", primary.getUuid())
                    .mapTo(String.class)
                    .one());
    assertThat(currentLocation).isEqualTo(newerLocation);
    assertThat(currentJobIo(primaryName))
        .containsExactly(new IoRef("INPUT", NAMESPACE, "job-event-input-newer"));
    assertThat(jobEventVersions(primary.getUuid()))
        .containsExactlyInAnyOrder(
            expectedJobEventVersion(primaryName, "job-event-input-older", olderLocation),
            expectedJobEventVersion(primaryName, "job-event-input-newer", newerLocation));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("lwwCases")
  void differentRunTerminalProjectionsConvergeByUtcTimeThenExactJsonDigest(LwwCase testCase)
      throws Exception {
    ZonedDateTime eventTimeA = EVENT_TIME.atZone(UTC);
    ZonedDateTime eventTimeB =
        testCase.equalUtcTime()
            ? EVENT_TIME.atZone(ZoneOffset.ofHours(12))
            : EVENT_TIME.plusSeconds(1).atZone(UTC);
    LwwPayload payloadA = lwwPayload(LWW_RUN_A, eventTimeA, "a");
    LwwPayload payloadB = lwwPayload(LWW_RUN_B, eventTimeB, "b");
    long eventIdA = queueDao.enqueue(payloadA.runId(), payloadA.eventJson());
    long eventIdB = queueDao.enqueue(payloadB.runId(), payloadB.eventJson());

    OpenLineageQueueRow rowA = queuedHead(payloadA.runId());
    OpenLineageQueueRow rowB = queuedHead(payloadB.runId());
    LwwActor actorA = queuedActor(payloadA, rowA, rowB);
    LwwActor actorB = queuedActor(payloadB, rowA, rowB);
    assertThat(actorA.row().id()).isEqualTo(eventIdA);
    assertThat(actorB.row().id()).isEqualTo(eventIdB);
    assertThat(actorA.row().eventJson()).isEqualTo(payloadA.eventJson());
    assertThat(actorB.row().eventJson()).isEqualTo(payloadB.eventJson());

    // The production helper orders the exact durable text. Independently pin its SHA-256 contract.
    assertThat(actorA.eventKey()).containsExactly(jdkSha256(actorA.row().eventJson()));
    assertThat(Arrays.compareUnsigned(actorA.eventKey(), actorB.eventKey())).isNotZero();
    if (testCase.equalUtcTime()) {
      assertThat(actorA.eventTime()).isEqualTo(actorB.eventTime());
    } else {
      assertThat(actorA.eventTime()).isBefore(actorB.eventTime());
    }

    LwwActor winner = compareProjectionOrder(actorA, actorB) > 0 ? actorA : actorB;
    LwwActor loser = winner.runId().equals(actorA.runId()) ? actorB : actorA;
    LwwActor first = testCase.winnerCommitsFirst() ? winner : loser;
    LwwActor second = testCase.winnerCommitsFirst() ? loser : winner;

    projectCrossLane(
        first.runId(),
        event -> lineageRunId(event).equals(first.runId()),
        second.runId(),
        event -> lineageRunId(event).equals(second.runId()));

    assertLwwWinner(winner);
    assertEventScopedJobVersion(actorA);
    assertEventScopedJobVersion(actorB);
    assertThat(rawEventCount(actorA.runId())).isEqualTo(1);
    assertThat(rawEventCount(actorB.runId())).isEqualTo(1);
    assertThat(runCount(actorA.runId())).isEqualTo(1);
    assertThat(runCount(actorB.runId())).isEqualTo(1);
    assertThat(liveEventIds()).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(deadLetterCount()).isZero();
  }

  @ParameterizedTest(name = "real parent commits first: {0}")
  @ValueSource(booleans = {true, false})
  void legacyChildAndRealParentConvergeAcrossCommitOrders(boolean parentCommitsFirst)
      throws Exception {
    String externalParentRunId = "legacy-parent-run";
    UUID childRunId = UUID.fromString("20000000-0000-4000-8000-00000000000a");
    String parentJobName = "ordered-parent";
    String legacyChildJobName = parentJobName + ".task";
    Instant parentEventTime = EVENT_TIME;
    Instant childEventTime = EVENT_TIME.plusSeconds(30);
    String parentDescription = "observed parent snapshot";
    String parentLocation = "https://example.com/parents/observed";
    String parentInput = "observed-parent-input";
    String parentOutput = "observed-parent-output";
    LineageEvent realParent =
        observedRunEvent(
            externalParentRunId,
            parentEventTime,
            parentJobName,
            parentDescription,
            parentLocation,
            parentInput,
            parentOutput);
    LineageEvent child =
        parentedEvent(
            childRunId,
            childEventTime,
            legacyChildJobName,
            "https://example.com/children/later",
            externalParentRunId,
            legacyChildJobName);
    queueDao.enqueue(realParent);
    queueDao.enqueue(child);
    UUID parentKey = OpenLineageQueueDao.orderingKeyFor(realParent);
    UUID childKey = OpenLineageQueueDao.orderingKeyFor(child);
    OpenLineageQueueRow parentRow = queuedHead(parentKey);
    OpenLineageQueueRow childRow = queuedHead(childKey);

    if (parentCommitsFirst) {
      projectCrossLane(
          parentKey,
          event -> lineageExternalRunId(event).equals(externalParentRunId),
          childKey,
          event -> lineageExternalRunId(event).equals(childRunId.toString()));
    } else {
      projectCrossLane(
          childKey,
          event -> lineageExternalRunId(event).equals(childRunId.toString()),
          parentKey,
          event -> lineageExternalRunId(event).equals(externalParentRunId));
    }

    UUID effectiveParentRunId = parentRunUuid(childRunId);
    ParentProjectionState parent = parentProjectionState(parentJobName, effectiveParentRunId);
    assertThat(parent.jobType()).isEqualTo("BATCH");
    assertThat(parent.jobUpdatedAt()).isEqualTo(childEventTime);
    assertThat(parent.description()).isEqualTo(parentDescription);
    assertThat(parent.jobLocation()).isEqualTo(parentLocation);
    assertThat(parent.currentInputs()).contains(parentInput);
    assertThat(parent.currentRunId()).isEqualTo(effectiveParentRunId);
    assertThat(parent.snapshotSentinel()).isFalse();
    assertThat(parent.snapshotTime()).isEqualTo(parentEventTime);
    assertThat(parent.snapshotKey()).containsExactly(Utils.sha256Utf8(parentRow.eventJson()));
    assertThat(parent.currentRunTime()).isEqualTo(childEventTime);
    assertThat(parent.currentRunKey()).containsExactly(Utils.sha256Utf8(childRow.eventJson()));
    assertThat(parent.runCreatedAt()).isEqualTo(parentEventTime);
    assertThat(parent.runUpdatedAt()).isEqualTo(parentEventTime);
    assertThat(parent.currentRunState()).isEqualTo("COMPLETED");
    assertThat(parent.endedAt()).isEqualTo(parentEventTime);
    assertThat(parent.nominalStartTime()).isEqualTo(parentEventTime.minusSeconds(60));
    assertThat(parent.nominalEndTime()).isEqualTo(parentEventTime.plusSeconds(60));
    assertThat(parent.runLocation()).isEqualTo(parentLocation);
    assertThat(parent.parentPlaceholder()).isNull();
    assertThat(parentRunCount(parentJobName)).isEqualTo(1);
    assertThat(rawEventPayloads(effectiveParentRunId))
        .singleElement()
        .satisfies(
            payload -> assertThat(jsonTree(payload)).isEqualTo(jsonTree(parentRow.eventJson())));
    assertThat(liveEventIds()).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(deadLetterCount()).isZero();
  }

  @Test
  void failedRepairedParentPromotionRollsBackThenPublishesEffectiveRun() throws Exception {
    UUID requestedParentRunId = UUID.fromString("21000000-0000-4000-8000-000000000001");
    String reportedByChild = requestedParentRunId.toString().toUpperCase(java.util.Locale.ROOT);
    String reportedByParent = requestedParentRunId.toString();
    UUID childRunId = UUID.fromString("21000000-0000-4000-8000-00000000000a");
    String parentJobName = "repaired-parent";
    String childJobName = "repaired-parent-child";
    Instant parentEventTime = EVENT_TIME.plusSeconds(10);
    Instant childEventTime = EVENT_TIME.plusSeconds(20);
    String parentDescription = "repaired observed parent";
    String parentLocation = "https://example.com/parents/repaired";
    String parentInput = "repaired-parent-input";
    String parentOutput = "repaired-parent-output";
    ForeignIdentitySnapshot foreignBefore = seedForeignRun(requestedParentRunId);
    LineageEvent child =
        parentedEvent(
            childRunId,
            childEventTime,
            childJobName,
            "https://example.com/children/repaired",
            reportedByChild,
            parentJobName);
    queueDao.enqueue(child);
    OpenLineageQueueRow childRow = queuedHead(OpenLineageQueueDao.orderingKeyFor(child));

    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.SUCCESS);

    UUID effectiveParentRunId = parentRunUuid(childRunId);
    assertThat(effectiveParentRunId).isNotEqualTo(requestedParentRunId);
    ParentProjectionState placeholder = parentProjectionState(parentJobName, effectiveParentRunId);
    assertThat(placeholder.parentPlaceholder()).isTrue();
    assertThat(placeholder.snapshotSentinel()).isTrue();
    assertThat(placeholder.snapshotTime()).isNull();
    assertThat(placeholder.snapshotKey()).containsOnly((byte) 0);
    assertThat(placeholder.description()).isNull();
    assertThat(placeholder.jobLocation()).isNull();
    assertThat(placeholder.currentInputs()).isEqualTo("[]");
    assertThat(placeholder.currentRunId()).isEqualTo(effectiveParentRunId);
    assertThat(placeholder.currentRunTime()).isEqualTo(childEventTime);
    assertThat(placeholder.currentRunKey()).containsExactly(Utils.sha256Utf8(childRow.eventJson()));
    assertThat(placeholder.runCreatedAt()).isEqualTo(Instant.EPOCH);
    assertThat(placeholder.runUpdatedAt()).isEqualTo(Instant.EPOCH);
    assertThat(placeholder.currentRunState()).isNull();
    assertThat(dependentProjectionCount(effectiveParentRunId)).isZero();
    String placeholderRowsBeforeFailure = parentRowsJson(parentJobName, effectiveParentRunId);
    assertThat(foreignIdentitySnapshot(requestedParentRunId)).isEqualTo(foreignBefore);

    LineageEvent observedParent =
        observedRunEvent(
            reportedByParent,
            parentEventTime,
            parentJobName,
            parentDescription,
            parentLocation,
            parentInput,
            parentOutput);
    UUID parentQueueKey = OpenLineageQueueDao.orderingKeyFor(observedParent);
    long parentEventId = queueDao.enqueue(observedParent);
    OpenLineageService failingService =
        new FailAfterProjectionService(
            baseDao(),
            runService(),
            requestedParentRunId,
            "COMPLETE",
            "failure after repaired parent promotion");

    TransactionResult failed = processNext(failingService, 3, 60_000);

    assertThat(failed.outcome()).isEqualTo(TransactionOutcome.RETRY);
    assertThat(failed.row().id()).isEqualTo(parentEventId);
    assertThat(parentRowsJson(parentJobName, effectiveParentRunId))
        .isEqualTo(placeholderRowsBeforeFailure);
    assertThat(dependentProjectionCount(effectiveParentRunId)).isZero();
    assertThat(rawEventCount(effectiveParentRunId)).isZero();
    assertThat(foreignIdentitySnapshot(requestedParentRunId)).isEqualTo(foreignBefore);
    assertThat(head(parentQueueKey).attemptCount()).isEqualTo(1);
    assertThat(head(parentQueueKey).lastError())
        .contains("failure after repaired parent promotion");

    makeScheduleDue(parentQueueKey);
    SearchService searchService = mock(SearchService.class);
    when(searchService.indexEvent(any(LineageEvent.class), any(UUID.class))).thenReturn(true);
    OpenLineageService recoveryService =
        new OpenLineageService(baseDao(), runService(), searchService, Runnable::run);
    OpenLineageWorker worker = newWorker(recoveryService, 1);

    assertThat(worker.processTask(() -> true))
        .isEqualTo(new OpenLineageWorker.TaskResult(1, OpenLineageWorker.EventOutcome.IDLE));

    ParentProjectionState promoted = parentProjectionState(parentJobName, effectiveParentRunId);
    assertThat(promoted.parentPlaceholder()).isNull();
    assertThat(promoted.snapshotSentinel()).isFalse();
    assertThat(promoted.snapshotTime()).isEqualTo(parentEventTime);
    assertThat(promoted.jobUpdatedAt()).isEqualTo(childEventTime);
    assertThat(promoted.description()).isEqualTo(parentDescription);
    assertThat(promoted.jobLocation()).isEqualTo(parentLocation);
    assertThat(promoted.currentRunTime()).isEqualTo(childEventTime);
    assertThat(promoted.currentRunKey()).containsExactly(Utils.sha256Utf8(childRow.eventJson()));
    assertThat(promoted.runCreatedAt()).isEqualTo(parentEventTime);
    assertThat(promoted.runUpdatedAt()).isEqualTo(parentEventTime);
    assertThat(promoted.currentRunState()).isEqualTo("COMPLETED");
    assertThat(promoted.runLocation()).isEqualTo(parentLocation);
    assertThat(parentRunCount(parentJobName)).isEqualTo(1);
    assertThat(foreignIdentitySnapshot(requestedParentRunId)).isEqualTo(foreignBefore);

    List<RunReference> references =
        effectiveRunReferences(
            parentJobName, reportedByParent, parentInput, parentOutput, parentEventTime);
    assertThat(references)
        .extracting(RunReference::source)
        .contains(
            "runs",
            "lineage_events",
            "run_states",
            "run_facets",
            "job_facets",
            "dataset_facets",
            "dataset_versions",
            "runs_input_mapping",
            "jobs.current_run_uuid",
            "job_versions.latest_run_uuid");
    assertThat(references)
        .allSatisfy(reference -> assertThat(reference.runId()).isEqualTo(effectiveParentRunId));
    assertThat(rawEventPayloads(effectiveParentRunId))
        .singleElement()
        .satisfies(
            payload ->
                assertThat(jsonTree(payload)).isEqualTo(jsonTree(Utils.toJson(observedParent))));

    ArgumentCaptor<LineageEvent> indexedEvent = ArgumentCaptor.forClass(LineageEvent.class);
    ArgumentCaptor<UUID> indexedRunId = ArgumentCaptor.forClass(UUID.class);
    verify(searchService).indexEvent(indexedEvent.capture(), indexedRunId.capture());
    assertThat(indexedEvent.getValue().getRun().getRunId()).isEqualTo(reportedByParent);
    assertThat(indexedRunId.getValue()).isEqualTo(effectiveParentRunId);
    assertThat(liveEventIds()).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(deadLetterCount()).isZero();
  }

  @ParameterizedTest(name = "first dataset consumer commits first: {0}")
  @ValueSource(booleans = {true, false})
  void inputDatasetVersionIdentityIsSharedWhileOutputIdentityRemainsRunSpecific(
      boolean runACommitsFirst) throws Exception {
    UUID runA = UUID.fromString("30000000-0000-4000-8000-00000000000a");
    UUID runB = UUID.fromString("30000000-0000-4000-8000-00000000000b");
    String inputName = "shared-logical-input";
    String outputName = "shared-run-output";
    LineageEvent eventA =
        datasetIdentityEvent(runA, EVENT_TIME, "identity-job-a", inputName, outputName);
    LineageEvent eventB =
        datasetIdentityEvent(
            runB, EVENT_TIME.plusSeconds(1), "identity-job-b", inputName, outputName);
    queueDao.enqueue(eventA);
    queueDao.enqueue(eventB);
    UUID keyA = OpenLineageQueueDao.orderingKeyFor(eventA);
    UUID keyB = OpenLineageQueueDao.orderingKeyFor(eventB);

    if (runACommitsFirst) {
      projectCrossLane(
          keyA,
          event -> lineageRunId(event).equals(runA),
          keyB,
          event -> lineageRunId(event).equals(runB));
    } else {
      projectCrossLane(
          keyB,
          event -> lineageRunId(event).equals(runB),
          keyA,
          event -> lineageRunId(event).equals(runA));
    }

    List<DatasetVersionIdentity> inputVersions = datasetVersions(inputName);
    UUID expectedInputVersion =
        Utils.newDatasetVersionFor(
                NAMESPACE,
                OpenLineageDao.DEFAULT_SOURCE_NAME,
                inputName,
                inputName,
                "",
                List.of(),
                null)
            .getValue();
    assertThat(inputVersions).hasSize(1);
    assertThat(inputVersions.get(0).version()).isEqualTo(expectedInputVersion);
    assertThat(inputVersions.get(0).runId()).isNull();

    List<RunDatasetVersionMapping> inputMappings =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT mapping.run_uuid, mapping.dataset_version_uuid
                        FROM runs_input_mapping AS mapping
                        JOIN dataset_versions AS version
                          ON version.uuid = mapping.dataset_version_uuid
                        JOIN datasets AS dataset ON dataset.uuid = version.dataset_uuid
                        WHERE dataset.namespace_name = :namespace AND dataset.name = :name
                        ORDER BY mapping.run_uuid
                        """)
                    .bind("namespace", NAMESPACE)
                    .bind("name", inputName)
                    .map(
                        (resultSet, context) ->
                            new RunDatasetVersionMapping(
                                resultSet.getObject("run_uuid", UUID.class),
                                resultSet.getObject("dataset_version_uuid", UUID.class)))
                    .list());
    assertThat(inputMappings)
        .containsExactlyInAnyOrder(
            new RunDatasetVersionMapping(runA, inputVersions.get(0).uuid()),
            new RunDatasetVersionMapping(runB, inputVersions.get(0).uuid()));

    List<DatasetVersionIdentity> outputVersions = datasetVersions(outputName);
    assertThat(outputVersions)
        .extracting(DatasetVersionIdentity::version)
        .containsExactlyInAnyOrder(
            expectedDatasetVersion(outputName, runA), expectedDatasetVersion(outputName, runB));
    assertThat(outputVersions)
        .extracting(DatasetVersionIdentity::runId)
        .containsExactlyInAnyOrder(runA, runB);

    assertThat(jobVersionEdges(runA, runB))
        .containsExactlyInAnyOrder(
            new JobVersionEdge(runA, "INPUT", inputName),
            new JobVersionEdge(runA, "OUTPUT", outputName),
            new JobVersionEdge(runB, "INPUT", inputName),
            new JobVersionEdge(runB, "OUTPUT", outputName));
  }

  @Test
  void sameRunIsSerializedWhileDifferentRunOfSameJobProgresses() throws Exception {
    UUID blockedRunId = UUID.randomUUID();
    UUID independentRunId = UUID.randomUUID();
    String jobName = "shared-job";
    LineageEvent blockedStart = runEvent(blockedRunId, "START", EVENT_TIME, jobName);
    LineageEvent independentStart =
        runEvent(independentRunId, "START", EVENT_TIME.plusSeconds(2), jobName);
    UUID blockedKey = OpenLineageQueueDao.orderingKeyFor(blockedStart);
    UUID independentKey = OpenLineageQueueDao.orderingKeyFor(independentStart);
    long blockedStartId = queueDao.enqueue(blockedStart);
    long blockedCompleteId =
        queueDao.enqueue(runEvent(blockedRunId, "COMPLETE", EVENT_TIME.plusSeconds(1), jobName));
    long independentId = queueDao.enqueue(independentStart);
    setScheduleAt(blockedKey, Instant.parse("2000-01-01T00:00:00Z"));
    setScheduleAt(independentKey, Instant.parse("2001-01-01T00:00:00Z"));

    CountDownLatch blockedEntered = new CountDownLatch(1);
    CountDownLatch releaseBlocked = new CountDownLatch(1);
    OpenLineageService blockedService =
        new BlockingBeforeProjectionService(
            baseDao(), runService(), blockedRunId, blockedEntered, releaseBlocked);

    ExecutorService projectionThread = Executors.newSingleThreadExecutor();
    CompletableFuture<TransactionResult> blockedResult =
        CompletableFuture.supplyAsync(() -> processNext(blockedService, 3, 0), projectionThread);
    try {
      awaitLatch(blockedEntered, "same-run head lock before projection");

      TransactionResult independent = processNext(newService(), 3, 0);
      assertThat(independent.outcome()).isEqualTo(TransactionOutcome.SUCCESS);
      assertThat(independent.row().orderingKey()).isEqualTo(independentKey);
      assertThat(independent.row().id()).isEqualTo(independentId);
      assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.EMPTY);
      assertThat(rawEventCount(independentRunId)).isEqualTo(1);
      assertThat(rawEventCount(blockedRunId)).isZero();
      assertThat(head(blockedKey).eventId()).isEqualTo(blockedStartId);
      assertThat(liveEventIds()).contains(blockedStartId, blockedCompleteId);

      releaseBlocked.countDown();
      assertThat(blockedResult.get(10, TimeUnit.SECONDS).outcome())
          .isEqualTo(TransactionOutcome.SUCCESS);
    } finally {
      releaseBlocked.countDown();
      if (!blockedResult.isDone()) {
        blockedResult.cancel(true);
      }
      projectionThread.shutdownNow();
      assertThat(projectionThread.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(head(blockedKey).eventId()).isEqualTo(blockedCompleteId);
    TransactionResult follower = processNext(newService(), 3, 0);
    assertThat(follower.outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(follower.row().id()).isEqualTo(blockedCompleteId);
    assertThat(rawEventTypes(blockedRunId)).containsExactly("START", "COMPLETE");
    assertThat(liveEventIds()).isEmpty();
    assertThat(headCount()).isZero();
  }

  @Test
  void longRunLaneAlternatesQ2AndPreservesExactJson() throws Exception {
    UUID runId = UUID.randomUUID();
    String jobName = "scheduling-quantum-job";
    List<LineageEvent> events =
        List.of(
            runEvent(runId, "START", EVENT_TIME, jobName),
            runEvent(runId, "OTHER", EVENT_TIME.plusSeconds(1), jobName),
            runEvent(runId, "OTHER", EVENT_TIME.plusSeconds(2), jobName),
            runEvent(runId, "OTHER", EVENT_TIME.plusSeconds(3), jobName),
            runEvent(runId, "OTHER", EVENT_TIME.plusSeconds(4), jobName),
            runEvent(runId, "OTHER", EVENT_TIME.plusSeconds(5), jobName),
            runEvent(runId, "COMPLETE", EVENT_TIME.plusSeconds(6), jobName));
    List<OpenLineageQueueDao.PreparedEvent> preparedEvents =
        events.stream().map(OpenLineageQueueDao::prepare).toList();
    UUID queueKey = preparedEvents.get(0).orderingKey();
    assertThat(preparedEvents)
        .allSatisfy(event -> assertThat(event.orderingKey()).isEqualTo(queueKey));
    List<String> payloads =
        preparedEvents.stream().map(OpenLineageQueueDao.PreparedEvent::eventJson).toList();
    List<Long> eventIds = preparedEvents.stream().map(event -> queueDao.enqueue(event)).toList();
    Instant firstQuantumDue = Instant.parse("2000-01-01T00:00:00Z");
    setScheduleAt(queueKey, firstQuantumDue);

    assertThat(liveEventIds()).containsExactlyElementsOf(eventIds);
    assertThat(queueSchedule(queueKey))
        .isEqualTo(new QueueScheduleState(eventIds.get(0), firstQuantumDue, false, 0, null));

    for (int index = 0; index < events.size(); index++) {
      QueueScheduleState before = queueSchedule(queueKey);
      Instant refreshLowerBound = databaseNowMillis();
      TransactionResult result = processNext(newService(), 3, 0);
      Instant refreshUpperBound = databaseNowMillis();

      assertThat(result.outcome()).isEqualTo(TransactionOutcome.SUCCESS);
      assertThat(result.row().id()).isEqualTo(eventIds.get(index));
      assertThat(result.row().eventJson()).isEqualTo(payloads.get(index));
      assertThat(result.row().attemptCount()).isEqualTo(1);

      if (index == events.size() - 1) {
        assertThat(headCount()).isZero();
        continue;
      }

      QueueScheduleState after = queueSchedule(queueKey);
      assertThat(after.eventId()).isEqualTo(eventIds.get(index + 1));
      assertThat(after.attemptCount()).isZero();
      assertThat(after.lastError()).isNull();
      if ((index & 1) == 0) {
        assertThat(after.availableAt()).isEqualTo(before.availableAt());
        assertThat(after.refreshDueOnAdvance()).isTrue();
      } else {
        assertThat(after.availableAt()).isBetween(refreshLowerBound, refreshUpperBound);
        assertThat(after.refreshDueOnAdvance()).isFalse();
      }
    }

    assertThat(rawEventTypes(runId))
        .containsExactlyElementsOf(events.stream().map(LineageEvent::getEventType).toList());
    assertThat(rawEventPayloads(runId))
        .extracting(OpenLineageDurabilityIntegrationTest::jsonTree)
        .containsExactlyElementsOf(
            payloads.stream().map(OpenLineageDurabilityIntegrationTest::jsonTree).toList());
    assertThat(runCount(runId)).isEqualTo(1);
    assertThat(runProjectionState(runId)).isEqualTo("COMPLETED");
    assertThat(liveEventIds()).isEmpty();
    assertThat(deadLetterCount()).isZero();
  }

  @Test
  void savepointRollbackRemovesPartialProjectionAndCommitsRetry() throws Exception {
    UUID runId = UUID.randomUUID();
    LineageEvent event = runEvent(runId, "START", EVENT_TIME, "savepoint-retry-job");
    UUID queueKey = OpenLineageQueueDao.orderingKeyFor(event);
    String eventJson = Utils.toJson(event);
    long eventId = queueDao.enqueue(event);
    OpenLineageService failingService =
        new FailAfterProjectionService(
            baseDao(), runService(), runId, "START", "failure after partial projection");

    TransactionResult failed = processNext(failingService, 3, 60_000);
    assertThat(failed.outcome()).isEqualTo(TransactionOutcome.RETRY);
    assertThat(failed.row().id()).isEqualTo(eventId);
    assertThat(failed.row().eventJson()).isEqualTo(eventJson);
    assertThat(failed.row().attemptCount()).isEqualTo(1);

    assertThat(rawEventCount(runId)).isZero();
    assertThat(runCount(runId)).isZero();
    assertThat(liveEventIds()).containsExactly(eventId);
    assertThat(head(queueKey).eventId()).isEqualTo(eventId);
    assertThat(head(queueKey).attemptCount()).isEqualTo(1);
    assertThat(head(queueKey).lastError()).contains("failure after partial projection");
    assertThat(headClock(queueKey).scheduleDue()).isFalse();
    assertThat(queueSchedule(queueKey).refreshDueOnAdvance()).isFalse();
    assertThat(processNext(newService(), 3, 0).outcome()).isEqualTo(TransactionOutcome.EMPTY);

    makeScheduleDue(queueKey);
    TransactionResult recovered = processNext(newService(), 3, 0);
    assertThat(recovered.outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(recovered.row().id()).isEqualTo(eventId);
    assertThat(recovered.row().eventJson()).isEqualTo(eventJson);
    assertThat(recovered.row().attemptCount()).isEqualTo(2);
    assertThat(recovered.row().lastError()).contains("failure after partial projection");

    assertThat(rawEventTypes(runId)).containsExactly("START");
    assertThat(runCount(runId)).isEqualTo(1);
    assertThat(liveEventIds()).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(deadLetterCount()).isZero();
  }

  @Test
  void exhaustedPoisonIsDeadLetteredAndFollowerProgresses() throws Exception {
    UUID runId = UUID.randomUUID();
    LineageEvent poisonEvent = runEvent(runId, "START", EVENT_TIME, "poison-job");
    String poisonJson = Utils.toJson(poisonEvent);
    long poisonId = queueDao.enqueue(runId, poisonJson);
    LineageEvent followerEvent =
        runEvent(runId, "COMPLETE", EVENT_TIME.plusSeconds(1), "poison-job");
    String followerJson = Utils.toJson(followerEvent);
    long followerId = queueDao.enqueue(runId, followerJson);

    OpenLineageService failingService =
        new FailAfterProjectionService(
            baseDao(), runService(), runId, "START", "poison failure after projection");
    TransactionResult firstAttempt = processNext(failingService, 2, 60_000);
    assertThat(firstAttempt.outcome()).isEqualTo(TransactionOutcome.RETRY);
    assertThat(firstAttempt.row().id()).isEqualTo(poisonId);
    assertThat(firstAttempt.row().eventJson()).isEqualTo(poisonJson);
    assertThat(firstAttempt.row().attemptCount()).isEqualTo(1);
    assertThat(rawEventCount(runId)).isZero();
    assertThat(runCount(runId)).isZero();
    assertThat(head(runId).attemptCount()).isEqualTo(1);

    makeScheduleDue(runId);
    TransactionResult finalAttempt = processNext(failingService, 2, 0);
    assertThat(finalAttempt.outcome()).isEqualTo(TransactionOutcome.DEAD_LETTER);
    assertThat(finalAttempt.row().id()).isEqualTo(poisonId);
    assertThat(finalAttempt.row().eventJson()).isEqualTo(poisonJson);
    assertThat(finalAttempt.row().attemptCount()).isEqualTo(2);

    assertThat(rawEventCount(runId)).isZero();
    assertThat(runCount(runId)).isZero();
    assertThat(liveEventIds()).containsExactly(followerId).doesNotContain(poisonId);
    assertThat(head(runId)).isEqualTo(new HeadState(followerId, 0, null));
    assertThat(deadLetter(runId, poisonId))
        .satisfies(
            dead -> {
              assertThat(dead.eventJson()).isEqualTo(poisonJson);
              assertThat(dead.attemptCount()).isEqualTo(2);
              assertThat(dead.lastError()).contains("poison failure after projection");
            });

    TransactionResult follower = processNext(newService(), 2, 0);
    assertThat(follower.outcome()).isEqualTo(TransactionOutcome.SUCCESS);
    assertThat(follower.row().id()).isEqualTo(followerId);
    assertThat(follower.row().eventJson()).isEqualTo(followerJson);
    assertThat(follower.row().attemptCount()).isEqualTo(1);

    assertThat(rawEventTypes(runId)).containsExactly("COMPLETE");
    assertThat(runCount(runId)).isEqualTo(1);
    assertThat(liveEventIds()).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(deadLetterCount()).isEqualTo(1);
  }

  @Test
  void forcedShutdownAbortsConnectionAndReplacementRecovers() throws Exception {
    UUID runId = UUID.randomUUID();
    LineageEvent event = runEvent(runId, "START", EVENT_TIME, "shutdown-recovery-job");
    UUID queueKey = OpenLineageQueueDao.orderingKeyFor(event);
    String eventJson = Utils.toJson(event);
    long eventId = queueDao.enqueue(event);
    long advisoryKey = 0x4f4c5f534854L;
    CountDownLatch projected = new CountDownLatch(1);
    AtomicInteger processingPid = new AtomicInteger();
    OpenLineageWorker blockedWorker =
        newWorker(
            new AdvisoryBlockedAfterProjectionService(
                baseDao(), runService(), projected, processingPid, advisoryKey),
            1,
            3,
            500);
    ExecutorService shutdownThread = Executors.newSingleThreadExecutor();
    try (Handle blocker = jdbi.open()) {
      int blockerPid = blocker.createQuery("SELECT pg_backend_pid()").mapTo(Integer.class).one();
      blocker
          .createQuery("SELECT pg_advisory_lock(:advisoryKey)")
          .bind("advisoryKey", advisoryKey)
          .map((resultSet, context) -> true)
          .one();

      blockedWorker.start();
      awaitLatch(projected, "projection before forced shutdown");
      awaitBlockedBy(processingPid.get(), blockerPid);
      assertThat(blockedWorker.activeConnectionCount()).isEqualTo(1);

      long shutdownStarted = System.nanoTime();
      CompletableFuture<Void> stopped =
          CompletableFuture.runAsync(() -> stopWorker(blockedWorker), shutdownThread);
      stopped.get(5, TimeUnit.SECONDS);
      assertThat(System.nanoTime() - shutdownStarted).isLessThan(TimeUnit.SECONDS.toNanos(5));
      assertThat(blockedWorker.activeConnectionCount()).isZero();
      assertThat(blockedWorker.availableTaskCapacity()).isEqualTo(1);
    } finally {
      shutdownThread.shutdownNow();
      assertThat(shutdownThread.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(rawEventCount(runId)).isZero();
    assertThat(runCount(runId)).isZero();
    assertThat(liveEventIds()).containsExactly(eventId);
    assertThat(head(queueKey)).isEqualTo(new HeadState(eventId, 0, null));
    assertThat(headClock(queueKey).scheduleDue()).isTrue();
    assertThat(queuedEventJson(queueKey, eventId)).isEqualTo(eventJson);
    assertThat(deadLetterCount()).isZero();

    OpenLineageWorker replacement = newWorker(newService(), 1);
    assertThat(replacement.processTask(() -> true))
        .isEqualTo(new OpenLineageWorker.TaskResult(1, OpenLineageWorker.EventOutcome.IDLE));

    assertThat(rawEventCount(runId)).isEqualTo(1);
    assertThat(runCount(runId)).isEqualTo(1);
    assertThat(liveEventIds()).isEmpty();
    assertThat(headCount()).isZero();
    assertThat(deadLetterCount()).isZero();
  }

  private OpenLineageWorker newWorker(OpenLineageService service, int workerThreads)
      throws IOException {
    return newWorker(service, workerThreads, 3, 1_000);
  }

  private OpenLineageWorker newWorker(
      OpenLineageService service,
      int workerThreads,
      int maxAttempts,
      long shutdownGracePeriodMillis)
      throws IOException {
    OpenLineageWorker worker =
        new OpenLineageWorker(
            jdbi,
            queueDao,
            service,
            workerConfig(workerThreads, maxAttempts, shutdownGracePeriodMillis),
            new MetricRegistry());
    workers.add(worker);
    return worker;
  }

  private OpenLineageService newService() {
    return new OpenLineageService(baseDao(), runService(), Runnable::run);
  }

  /**
   * Mirrors the landed transaction contract. The selected row contains a proposed attempt ordinal;
   * the savepoint preserves only the locked head, and retry/dead-letter persists that ordinal after
   * projection rollback. A failure of the outer transaction persists neither projection nor
   * attempt.
   */
  private TransactionResult processNext(
      OpenLineageService service, int maxAttempts, long retryDelayMillis) {
    return processNext(jdbi, service, maxAttempts, retryDelayMillis);
  }

  private TransactionResult processNext(
      Jdbi transactionJdbi, OpenLineageService service, int maxAttempts, long retryDelayMillis) {
    return transactionJdbi.inTransaction(
        TransactionIsolationLevel.READ_COMMITTED,
        handle -> {
          OpenLineageQueueDao transactionalQueue = handle.attach(OpenLineageQueueDao.class);
          OpenLineageQueueRow row = transactionalQueue.lockNextDue().orElse(null);
          if (row == null) {
            return new TransactionResult(TransactionOutcome.EMPTY, null);
          }

          Savepoint projection = projectionSavepoint(handle.getConnection());
          try {
            if (row.attemptCount() > maxAttempts) {
              throw new IllegalArgumentException("maximum processing attempts exceeded");
            }
            BaseEvent event = Utils.getMapper().readValue(row.eventJson(), BaseEvent.class);
            service.processQueuedInTransaction(
                event, row.eventJson(), handle.attach(OpenLineageDao.class));
          } catch (Exception failure) {
            rollbackProjection(handle.getConnection(), projection);
            releaseProjectionSavepoint(handle.getConnection(), projection);
            String error = failureMessage(failure);
            if (causedBy(failure, JsonProcessingException.class)
                || causedBy(failure, IllegalArgumentException.class)
                || row.attemptCount() >= maxAttempts) {
              transactionalQueue.deadLetterLocked(
                  row.orderingKey(), row.id(), row.attemptCount(), error);
              return new TransactionResult(TransactionOutcome.DEAD_LETTER, row);
            }
            transactionalQueue.retryLocked(
                row.orderingKey(), row.id(), row.attemptCount(), error, retryDelayMillis);
            return new TransactionResult(TransactionOutcome.RETRY, row);
          }

          releaseProjectionSavepoint(handle.getConnection(), projection);
          transactionalQueue.ackLocked(row.orderingKey(), row.id());
          return new TransactionResult(TransactionOutcome.SUCCESS, row);
        });
  }

  private static Savepoint projectionSavepoint(Connection connection) {
    try {
      return connection.setSavepoint("open_lineage_projection");
    } catch (SQLException failure) {
      throw new IllegalStateException("Could not create projection savepoint", failure);
    }
  }

  private static void rollbackProjection(Connection connection, Savepoint savepoint) {
    try {
      connection.rollback(savepoint);
    } catch (SQLException failure) {
      throw new IllegalStateException("Could not roll back projection savepoint", failure);
    }
  }

  private static void releaseProjectionSavepoint(Connection connection, Savepoint savepoint) {
    try {
      connection.releaseSavepoint(savepoint);
    } catch (SQLException failure) {
      throw new IllegalStateException("Could not release projection savepoint", failure);
    }
  }

  private static Connection connectionThatReportsLossAfterCommit(Connection connection) {
    AtomicBoolean commitReportedLost = new AtomicBoolean();
    return (Connection)
        Proxy.newProxyInstance(
            OpenLineageDurabilityIntegrationTest.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, arguments) -> {
              Object result;
              try {
                result = method.invoke(connection, arguments);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }
              if (method.getName().equals("commit")
                  && method.getParameterCount() == 0
                  && commitReportedLost.compareAndSet(false, true)) {
                throw new SQLException("simulated connection loss after PostgreSQL commit");
              }
              return result;
            });
  }

  private static Connection connectionThatLosesInsertResponseBeforeCommit(
      Connection connection, AtomicInteger commitCalls, AtomicInteger rollbackCalls) {
    AtomicBoolean insertResponseLost = new AtomicBoolean();
    return (Connection)
        Proxy.newProxyInstance(
            OpenLineageDurabilityIntegrationTest.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, arguments) -> {
              if (method.getName().equals("commit") && method.getParameterCount() == 0) {
                commitCalls.incrementAndGet();
              } else if (method.getName().equals("rollback") && method.getParameterCount() == 0) {
                rollbackCalls.incrementAndGet();
              }

              Object result;
              try {
                result = method.invoke(connection, arguments);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }

              if (method.getName().equals("prepareStatement")
                  && arguments != null
                  && arguments.length > 0
                  && arguments[0] instanceof String sql
                  && sql.contains("INSERT INTO open_lineage_queue")
                  && result instanceof PreparedStatement statement) {
                return statementThatLosesExecutionResponse(statement, insertResponseLost);
              }
              return result;
            });
  }

  private static PreparedStatement statementThatLosesExecutionResponse(
      PreparedStatement statement, AtomicBoolean responseLost) {
    return (PreparedStatement)
        Proxy.newProxyInstance(
            OpenLineageDurabilityIntegrationTest.class.getClassLoader(),
            new Class<?>[] {PreparedStatement.class},
            (proxy, method, arguments) -> {
              Object result;
              try {
                result = method.invoke(statement, arguments);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }

              if ((method.getName().equals("execute")
                      || method.getName().equals("executeQuery")
                      || method.getName().equals("executeUpdate")
                      || method.getName().equals("executeLargeUpdate"))
                  && method.getParameterCount() == 0
                  && responseLost.compareAndSet(false, true)) {
                throw new SQLException("simulated insert response loss before PostgreSQL commit");
              }
              return result;
            });
  }

  private static String failureMessage(Exception failure) {
    return failure.getMessage() == null ? failure.toString() : failure.getMessage();
  }

  private static boolean causedBy(Throwable failure, Class<? extends Throwable> type) {
    List<Throwable> visited = new ArrayList<>();
    Throwable current = failure;
    while (current != null && !visited.contains(current)) {
      if (type.isInstance(current)) {
        return true;
      }
      visited.add(current);
      current = current.getCause();
    }
    return false;
  }

  private void projectCrossLane(
      UUID firstKey,
      Predicate<BaseEvent> firstEvent,
      UUID secondKey,
      Predicate<BaseEvent> secondEvent)
      throws Exception {
    setScheduleAt(firstKey, Instant.parse("2000-01-01T00:00:00Z"));
    setScheduleAt(secondKey, Instant.parse("2001-01-01T00:00:00Z"));
    CountDownLatch firstProjected = new CountDownLatch(1);
    CountDownLatch secondEntered = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    AtomicInteger firstBackendPid = new AtomicInteger();
    AtomicInteger secondBackendPid = new AtomicInteger();
    CrossLaneProjectionGateService service =
        new CrossLaneProjectionGateService(
            baseDao(),
            runService(),
            firstEvent,
            secondEvent,
            firstProjected,
            secondEntered,
            releaseFirst,
            firstBackendPid,
            secondBackendPid);
    ExecutorService projectionThreads = Executors.newFixedThreadPool(2);
    CompletableFuture<TransactionResult> firstResult =
        CompletableFuture.supplyAsync(() -> processNext(service, 3, 0), projectionThreads);
    CompletableFuture<TransactionResult> secondResult = null;
    try {
      awaitLatch(firstProjected, "first cross-lane projection");
      secondResult =
          CompletableFuture.supplyAsync(() -> processNext(service, 3, 0), projectionThreads);
      awaitLatch(secondEntered, "second cross-lane projection");
      awaitBlockedBy(secondBackendPid.get(), firstBackendPid.get());
      releaseFirst.countDown();
      TransactionResult first = firstResult.get(10, TimeUnit.SECONDS);
      TransactionResult second = secondResult.get(10, TimeUnit.SECONDS);
      assertThat(first.outcome()).isEqualTo(TransactionOutcome.SUCCESS);
      assertThat(first.row().orderingKey()).isEqualTo(firstKey);
      assertThat(second.outcome()).isEqualTo(TransactionOutcome.SUCCESS);
      assertThat(second.row().orderingKey()).isEqualTo(secondKey);
    } finally {
      releaseFirst.countDown();
      if (secondResult != null && !secondResult.isDone()) {
        secondResult.cancel(true);
      }
      if (!firstResult.isDone()) {
        firstResult.cancel(true);
      }
      projectionThreads.shutdownNow();
      assertThat(projectionThreads.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  private static UUID lineageRunId(BaseEvent event) {
    return UUID.fromString(((LineageEvent) event).getRun().getRunId());
  }

  private static String lineageExternalRunId(BaseEvent event) {
    return ((LineageEvent) event).getRun().getRunId();
  }

  private UUID parentRunUuid(UUID childRunId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT parent_run_uuid FROM runs WHERE uuid = :childRunId")
                .bind("childRunId", childRunId)
                .mapTo(UUID.class)
                .one());
  }

  private long parentRunCount(String parentJobName) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT count(*)
                    FROM runs AS run
                    JOIN jobs AS job ON job.uuid = run.job_uuid
                    WHERE job.namespace_name = :namespace AND job.name = :jobName
                    """)
                .bind("namespace", NAMESPACE)
                .bind("jobName", parentJobName)
                .mapTo(Long.class)
                .one());
  }

  private ParentProjectionState parentProjectionState(String parentJobName, UUID parentRunId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT job.uuid AS job_uuid,
                           job.type AS job_type,
                           job.created_at AS job_created_at,
                           job.updated_at AS job_updated_at,
                           job.description,
                           job.current_location AS job_location,
                           job.current_inputs::text AS current_inputs,
                           job.current_run_uuid,
                           job.open_lineage_snapshot_time = '-infinity'::timestamptz
                             AS snapshot_sentinel,
                           CASE
                             WHEN job.open_lineage_snapshot_time = '-infinity'::timestamptz
                               THEN NULL
                             ELSE job.open_lineage_snapshot_time
                           END AS finite_snapshot_time,
                           job.open_lineage_snapshot_key,
                           job.open_lineage_current_run_time,
                           job.open_lineage_current_run_key,
                           run.created_at AS run_created_at,
                           run.updated_at AS run_updated_at,
                           run.current_run_state,
                           run.started_at,
                           run.ended_at,
                           run.nominal_start_time,
                           run.nominal_end_time,
                           run.location AS run_location,
                           run.open_lineage_parent_placeholder
                    FROM jobs AS job
                    JOIN runs AS run ON run.job_uuid = job.uuid
                    WHERE job.namespace_name = :namespace
                      AND job.name = :jobName
                      AND run.uuid = :runId
                    """)
                .bind("namespace", NAMESPACE)
                .bind("jobName", parentJobName)
                .bind("runId", parentRunId)
                .map(
                    (resultSet, context) ->
                        new ParentProjectionState(
                            resultSet.getObject("job_uuid", UUID.class),
                            resultSet.getString("job_type"),
                            requiredInstant(resultSet, "job_created_at"),
                            requiredInstant(resultSet, "job_updated_at"),
                            resultSet.getString("description"),
                            resultSet.getString("job_location"),
                            resultSet.getString("current_inputs"),
                            resultSet.getObject("current_run_uuid", UUID.class),
                            resultSet.getBoolean("snapshot_sentinel"),
                            nullableInstant(resultSet, "finite_snapshot_time"),
                            resultSet.getBytes("open_lineage_snapshot_key"),
                            requiredInstant(resultSet, "open_lineage_current_run_time"),
                            resultSet.getBytes("open_lineage_current_run_key"),
                            requiredInstant(resultSet, "run_created_at"),
                            requiredInstant(resultSet, "run_updated_at"),
                            resultSet.getString("current_run_state"),
                            nullableInstant(resultSet, "started_at"),
                            nullableInstant(resultSet, "ended_at"),
                            nullableInstant(resultSet, "nominal_start_time"),
                            nullableInstant(resultSet, "nominal_end_time"),
                            resultSet.getString("run_location"),
                            resultSet.getObject("open_lineage_parent_placeholder", Boolean.class)))
                .one());
  }

  private ForeignIdentitySnapshot seedForeignRun(UUID requestedRunId) {
    String foreignNamespaceName = "durability-foreign";
    String foreignJobName = "foreign-run-owner";
    Instant seededAt = EVENT_TIME.minusSeconds(3600);
    NamespaceRow namespace =
        jdbi.onDemand(NamespaceDao.class)
            .upsertNamespaceRow(
                UUID.randomUUID(), seededAt, foreignNamespaceName, getClass().getName());
    JobDao jobDao = jdbi.onDemand(JobDao.class);
    JobRow job =
        jobDao.upsertJob(
            UUID.randomUUID(),
            JobType.BATCH,
            seededAt,
            namespace.getUuid(),
            namespace.getName(),
            foreignJobName,
            "foreign run owner",
            "https://example.com/foreign",
            null,
            jobDao.toJson(Collections.emptySet(), Utils.getMapper()));
    RunArgsRow args =
        jdbi.onDemand(RunArgsDao.class)
            .upsertRunArgs(
                UUID.randomUUID(),
                seededAt,
                "{\"foreign\":true}",
                Utils.checksumFor(Collections.singletonMap("foreign", requestedRunId.toString())));
    jdbi.onDemand(RunDao.class)
        .upsert(
            requestedRunId,
            null,
            "foreign-external-id",
            seededAt,
            job.getUuid(),
            null,
            args.getUuid(),
            seededAt.minusSeconds(30),
            seededAt.plusSeconds(30),
            RunState.COMPLETED,
            seededAt,
            namespace.getName(),
            job.getName(),
            "https://example.com/foreign/run");
    return foreignIdentitySnapshot(requestedRunId);
  }

  private ForeignIdentitySnapshot foreignIdentitySnapshot(UUID runId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT to_jsonb(run)::text AS run_json,
                           to_jsonb(job)::text AS job_json
                    FROM runs AS run
                    JOIN jobs AS job ON job.uuid = run.job_uuid
                    WHERE run.uuid = :runId
                    """)
                .bind("runId", runId)
                .map(
                    (resultSet, context) ->
                        new ForeignIdentitySnapshot(
                            resultSet.getString("run_json"), resultSet.getString("job_json")))
                .one());
  }

  private String parentRowsJson(String parentJobName, UUID parentRunId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT jsonb_build_object('job', to_jsonb(job), 'run', to_jsonb(run))::text
                    FROM jobs AS job
                    JOIN runs AS run ON run.job_uuid = job.uuid
                    WHERE job.namespace_name = :namespace
                      AND job.name = :jobName
                      AND run.uuid = :runId
                    """)
                .bind("namespace", NAMESPACE)
                .bind("jobName", parentJobName)
                .bind("runId", parentRunId)
                .mapTo(String.class)
                .one());
  }

  private long dependentProjectionCount(UUID runId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT
                      (SELECT count(*) FROM lineage_events WHERE run_uuid = :runId)
                      + (SELECT count(*) FROM run_states WHERE run_uuid = :runId)
                      + (SELECT count(*) FROM run_facets WHERE run_uuid = :runId)
                      + (SELECT count(*) FROM job_facets WHERE run_uuid = :runId)
                      + (SELECT count(*) FROM dataset_facets WHERE run_uuid = :runId)
                      + (SELECT count(*) FROM dataset_versions WHERE run_uuid = :runId)
                      + (SELECT count(*) FROM runs_input_mapping WHERE run_uuid = :runId)
                      + (SELECT count(*) FROM job_versions WHERE latest_run_uuid = :runId)
                    """)
                .bind("runId", runId)
                .mapTo(Long.class)
                .one());
  }

  private List<RunReference> effectiveRunReferences(
      String jobName,
      String reportedRunId,
      String inputName,
      String outputName,
      Instant eventTime) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    WITH target_job AS (
                      SELECT uuid
                      FROM jobs
                      WHERE namespace_name = :namespace AND name = :jobName
                    ), target_run AS (
                      SELECT run.uuid
                      FROM runs AS run
                      JOIN target_job AS job ON job.uuid = run.job_uuid
                    )
                    SELECT 'runs' AS source, run.uuid AS run_uuid
                    FROM runs AS run
                    JOIN target_job AS job ON job.uuid = run.job_uuid
                    UNION ALL
                    SELECT 'lineage_events', event.run_uuid
                    FROM lineage_events AS event
                    WHERE event.event -> 'run' ->> 'runId' = :reportedRunId
                      AND event.event -> 'job' ->> 'namespace' = :namespace
                      AND event.event -> 'job' ->> 'name' = :jobName
                    UNION ALL
                    SELECT 'run_states', state.run_uuid
                    FROM run_states AS state
                    JOIN target_run AS run ON run.uuid = state.run_uuid
                    UNION ALL
                    SELECT 'run_facets', facet.run_uuid
                    FROM run_facets AS facet
                    JOIN target_run AS run ON run.uuid = facet.run_uuid
                    WHERE facet.lineage_event_time = :eventTime
                    UNION ALL
                    SELECT 'job_facets', facet.run_uuid
                    FROM job_facets AS facet
                    JOIN target_job AS job ON job.uuid = facet.job_uuid
                    WHERE facet.lineage_event_time = :eventTime AND facet.run_uuid IS NOT NULL
                    UNION ALL
                    SELECT 'dataset_facets', facet.run_uuid
                    FROM dataset_facets AS facet
                    JOIN datasets AS dataset ON dataset.uuid = facet.dataset_uuid
                    WHERE dataset.namespace_name = :namespace
                      AND dataset.name IN (:inputName, :outputName)
                      AND facet.lineage_event_time = :eventTime
                      AND facet.run_uuid IS NOT NULL
                    UNION ALL
                    SELECT 'dataset_versions', version.run_uuid
                    FROM dataset_versions AS version
                    JOIN datasets AS dataset ON dataset.uuid = version.dataset_uuid
                    WHERE dataset.namespace_name = :namespace
                      AND dataset.name = :outputName
                      AND version.run_uuid IS NOT NULL
                    UNION ALL
                    SELECT 'runs_input_mapping', mapping.run_uuid
                    FROM runs_input_mapping AS mapping
                    JOIN dataset_versions AS version ON version.uuid = mapping.dataset_version_uuid
                    JOIN datasets AS dataset ON dataset.uuid = version.dataset_uuid
                    JOIN target_run AS run ON run.uuid = mapping.run_uuid
                    WHERE dataset.namespace_name = :namespace AND dataset.name = :inputName
                    UNION ALL
                    SELECT 'jobs.current_run_uuid', job.current_run_uuid
                    FROM jobs AS job
                    JOIN target_job AS target ON target.uuid = job.uuid
                    WHERE job.current_run_uuid IS NOT NULL
                    UNION ALL
                    SELECT 'job_versions.latest_run_uuid', version.latest_run_uuid
                    FROM job_versions AS version
                    JOIN target_job AS job ON job.uuid = version.job_uuid
                    WHERE version.latest_run_uuid IS NOT NULL
                    """)
                .bind("namespace", NAMESPACE)
                .bind("jobName", jobName)
                .bind("reportedRunId", reportedRunId)
                .bind("inputName", inputName)
                .bind("outputName", outputName)
                .bind("eventTime", eventTime)
                .map(
                    (resultSet, context) ->
                        new RunReference(
                            resultSet.getString("source"),
                            resultSet.getObject("run_uuid", UUID.class)))
                .list());
  }

  private OpenLineageDao baseDao() {
    return jdbi.onDemand(OpenLineageDao.class);
  }

  private RunService runService() {
    return new RunService(baseDao(), List.of());
  }

  private static OpenLineageConfig workerConfig(
      int workerThreads, int maxAttempts, long shutdownGracePeriodMillis) throws IOException {
    return Utils.newObjectMapper()
        .readValue(
            """
            {
              "workerThreads": %d,
              "pollIntervalMillis": 1000,
              "maxAttempts": %d,
              "retryInitialDelayMillis": 60000,
              "retryMaxDelayMillis": 60000,
              "shutdownGracePeriodMillis": %d
            }
            """
                .formatted(workerThreads, maxAttempts, shutdownGracePeriodMillis),
            OpenLineageConfig.class);
  }

  private static String workerMetricName(String suffix) {
    return MetricRegistry.name(OpenLineageWorker.class, suffix);
  }

  private static Stream<LwwCase> lwwCases() {
    return Stream.of(
        new LwwCase("newer event commits first", false, true),
        new LwwCase("newer event commits last", false, false),
        new LwwCase("equal UTC time digest winner commits first", true, true),
        new LwwCase("equal UTC time digest winner commits last", true, false));
  }

  private static LwwPayload lwwPayload(UUID runId, ZonedDateTime eventTime, String suffix) {
    String inputName = "lww-input-" + suffix;
    String privateOutputName = "lww-private-output-" + suffix;
    String schemaFieldName = "field_" + suffix;
    String jobDescription = "job snapshot " + suffix;
    String jobLocation = "https://example.com/jobs/" + suffix;
    String datasetDescription = "dataset snapshot " + suffix;
    String lifecycleState = "a".equals(suffix) ? "CREATE" : "TRUNCATE";
    LineageEvent.Dataset input =
        LineageEvent.Dataset.builder().namespace(NAMESPACE).name(inputName).build();
    LineageEvent.Dataset sharedOutput =
        LineageEvent.Dataset.builder()
            .namespace(NAMESPACE)
            .name(LWW_SHARED_OUTPUT)
            .facets(
                LineageEvent.DatasetFacets.builder()
                    .documentation(
                        new LineageEvent.DocumentationDatasetFacet(
                            URI.create(PRODUCER), FACET_SCHEMA, datasetDescription))
                    .schema(
                        new LineageEvent.SchemaDatasetFacet(
                            URI.create(PRODUCER),
                            FACET_SCHEMA,
                            List.of(
                                new LineageEvent.SchemaField(
                                    schemaFieldName, "STRING", "schema " + suffix))))
                    .lifecycleStateChange(
                        new LineageEvent.LifecycleStateChangeFacet(
                            URI.create(PRODUCER), FACET_SCHEMA, lifecycleState))
                    .build())
            .build();
    LineageEvent.Dataset privateOutput =
        LineageEvent.Dataset.builder().namespace(NAMESPACE).name(privateOutputName).build();
    LineageEvent event =
        LineageEvent.builder()
            .eventType("COMPLETE")
            .eventTime(eventTime)
            .run(new LineageEvent.Run(runId.toString(), null))
            .job(
                LineageEvent.Job.builder()
                    .namespace(NAMESPACE)
                    .name(LWW_JOB)
                    .facets(
                        LineageEvent.JobFacet.builder()
                            .documentation(
                                new LineageEvent.DocumentationJobFacet(
                                    URI.create(PRODUCER), FACET_SCHEMA, jobDescription))
                            .sourceCodeLocation(
                                LineageEvent.SourceCodeLocationJobFacet.builder()
                                    ._producer(URI.create(PRODUCER))
                                    ._schemaURL(FACET_SCHEMA)
                                    .type("git")
                                    .url(jobLocation)
                                    .build())
                            .build())
                    .build())
            .inputs(List.of(input))
            .outputs(List.of(sharedOutput, privateOutput))
            .producer(PRODUCER)
            .schemaURL(RUN_SCHEMA)
            .build();
    return new LwwPayload(
        runId,
        Utils.toJson(event),
        inputName,
        privateOutputName,
        schemaFieldName,
        jobDescription,
        jobLocation,
        datasetDescription,
        lifecycleState);
  }

  private static LwwActor queuedActor(
      LwwPayload payload, OpenLineageQueueRow first, OpenLineageQueueRow second)
      throws IOException {
    OpenLineageQueueRow row =
        payload.runId().equals(first.orderingKey())
            ? first
            : payload.runId().equals(second.orderingKey()) ? second : null;
    assertThat(row).as("queued head for run %s", payload.runId()).isNotNull();
    LineageEvent persistedEvent = Utils.getMapper().readValue(row.eventJson(), LineageEvent.class);
    return new LwwActor(
        payload, row, persistedEvent.getEventTime().toInstant(), Utils.sha256Utf8(row.eventJson()));
  }

  private static int compareProjectionOrder(LwwActor left, LwwActor right) {
    int timeComparison = left.eventTime().compareTo(right.eventTime());
    return timeComparison != 0
        ? timeComparison
        : Arrays.compareUnsigned(left.eventKey(), right.eventKey());
  }

  private static byte[] jdkSha256(String value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value.getBytes(UTF_8));
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError("SHA-256 must be available", impossible);
    }
  }

  private static LineageEvent runEvent(
      UUID runId, String eventType, Instant eventTime, String jobName) {
    return LineageEvent.builder()
        .eventType(eventType)
        .eventTime(eventTime.atZone(UTC))
        .run(new LineageEvent.Run(runId.toString(), null))
        .job(LineageEvent.Job.builder().namespace(NAMESPACE).name(jobName).build())
        .inputs(List.of())
        .outputs(List.of())
        .producer(PRODUCER)
        .schemaURL(RUN_SCHEMA)
        .build();
  }

  private static LineageEvent observedRunEvent(
      String runId,
      Instant eventTime,
      String jobName,
      String description,
      String jobLocation,
      String inputName,
      String outputName) {
    LineageEvent.RunFacet runFacets =
        LineageEvent.RunFacet.builder()
            .nominalTime(
                LineageEvent.NominalTimeRunFacet.builder()
                    ._producer(URI.create(PRODUCER))
                    ._schemaURL(FACET_SCHEMA)
                    .nominalStartTime(eventTime.minusSeconds(60).atZone(UTC))
                    .nominalEndTime(eventTime.plusSeconds(60).atZone(UTC))
                    .build())
            .build();
    LineageEvent.JobFacet jobFacets =
        LineageEvent.JobFacet.builder()
            .documentation(
                new LineageEvent.DocumentationJobFacet(
                    URI.create(PRODUCER), FACET_SCHEMA, description))
            .sourceCodeLocation(
                LineageEvent.SourceCodeLocationJobFacet.builder()
                    ._producer(URI.create(PRODUCER))
                    ._schemaURL(FACET_SCHEMA)
                    .type("git")
                    .url(jobLocation)
                    .build())
            .jobType(
                LineageEvent.JobTypeJobFacet.builder()
                    ._producer(URI.create(PRODUCER))
                    ._schemaURL(FACET_SCHEMA)
                    .processingType("BATCH")
                    .integration("durability-test")
                    .jobType("DAG")
                    .build())
            .build();
    LineageEvent.Dataset input = observedDataset(inputName, "observed parent input");
    LineageEvent.Dataset output = observedDataset(outputName, "observed parent output");
    return LineageEvent.builder()
        .eventType("COMPLETE")
        .eventTime(eventTime.atZone(UTC))
        .run(new LineageEvent.Run(runId, runFacets))
        .job(
            LineageEvent.Job.builder().namespace(NAMESPACE).name(jobName).facets(jobFacets).build())
        .inputs(List.of(input))
        .outputs(List.of(output))
        .producer(PRODUCER)
        .schemaURL(RUN_SCHEMA)
        .build();
  }

  private static LineageEvent.Dataset observedDataset(String name, String description) {
    return LineageEvent.Dataset.builder()
        .namespace(NAMESPACE)
        .name(name)
        .facets(
            LineageEvent.DatasetFacets.builder()
                .documentation(
                    new LineageEvent.DocumentationDatasetFacet(
                        URI.create(PRODUCER), FACET_SCHEMA, description))
                .schema(
                    new LineageEvent.SchemaDatasetFacet(
                        URI.create(PRODUCER),
                        FACET_SCHEMA,
                        List.of(new LineageEvent.SchemaField("value", "STRING", description))))
                .build())
        .build();
  }

  private static LineageEvent parentedEvent(
      UUID runId,
      Instant eventTime,
      String jobName,
      String jobLocation,
      String parentRunId,
      String parentJobName) {
    LineageEvent.ParentRunFacet parent =
        LineageEvent.ParentRunFacet.builder()
            ._producer(URI.create(PRODUCER))
            ._schemaURL(FACET_SCHEMA)
            .run(LineageEvent.RunLink.builder().runId(parentRunId).build())
            .job(LineageEvent.JobLink.builder().namespace(NAMESPACE).name(parentJobName).build())
            .build();
    LineageEvent.RunFacet runFacets =
        LineageEvent.RunFacet.builder()
            .parent(parent)
            .nominalTime(
                LineageEvent.NominalTimeRunFacet.builder()
                    ._producer(URI.create(PRODUCER))
                    ._schemaURL(FACET_SCHEMA)
                    .nominalStartTime(eventTime.minusSeconds(60).atZone(UTC))
                    .nominalEndTime(eventTime.plusSeconds(60).atZone(UTC))
                    .build())
            .build();
    LineageEvent.JobFacet jobFacets =
        LineageEvent.JobFacet.builder()
            .sourceCodeLocation(
                LineageEvent.SourceCodeLocationJobFacet.builder()
                    ._producer(URI.create(PRODUCER))
                    ._schemaURL(FACET_SCHEMA)
                    .type("git")
                    .url(jobLocation)
                    .build())
            .jobType(
                LineageEvent.JobTypeJobFacet.builder()
                    ._producer(URI.create(PRODUCER))
                    ._schemaURL(FACET_SCHEMA)
                    .processingType("STREAMING")
                    .integration("durability-test")
                    .jobType("TASK")
                    .build())
            .build();
    return LineageEvent.builder()
        .eventType("COMPLETE")
        .eventTime(eventTime.atZone(UTC))
        .run(new LineageEvent.Run(runId.toString(), runFacets))
        .job(
            LineageEvent.Job.builder().namespace(NAMESPACE).name(jobName).facets(jobFacets).build())
        .inputs(List.of())
        .outputs(List.of())
        .producer(PRODUCER)
        .schemaURL(RUN_SCHEMA)
        .build();
  }

  private static JobEvent jobEvent(
      Instant eventTime, String jobName, List<String> inputNames, List<String> outputNames) {
    return jobEvent(eventTime, jobName, inputNames, outputNames, null);
  }

  private static JobEvent jobEvent(
      Instant eventTime,
      String jobName,
      List<String> inputNames,
      List<String> outputNames,
      String jobLocation) {
    return JobEvent.builder()
        .eventTime(eventTime.atZone(UTC))
        .job(
            LineageEvent.Job.builder()
                .namespace(NAMESPACE)
                .name(jobName)
                .facets(
                    jobLocation == null
                        ? null
                        : LineageEvent.JobFacet.builder()
                            .sourceCodeLocation(
                                LineageEvent.SourceCodeLocationJobFacet.builder()
                                    ._producer(URI.create(PRODUCER))
                                    ._schemaURL(FACET_SCHEMA)
                                    .type("git")
                                    .url(jobLocation)
                                    .build())
                            .build())
                .build())
        .inputs(
            inputNames.stream()
                .map(name -> LineageEvent.Dataset.builder().namespace(NAMESPACE).name(name).build())
                .toList())
        .outputs(
            outputNames.stream()
                .map(name -> LineageEvent.Dataset.builder().namespace(NAMESPACE).name(name).build())
                .toList())
        .producer(PRODUCER)
        .schemaURL(JOB_SCHEMA)
        .build();
  }

  private static LineageEvent outputEvent(
      UUID runId, Instant eventTime, String jobName, String datasetNamespace, String datasetName) {
    return LineageEvent.builder()
        .eventType("COMPLETE")
        .eventTime(eventTime.atZone(UTC))
        .run(new LineageEvent.Run(runId.toString(), null))
        .job(LineageEvent.Job.builder().namespace(NAMESPACE).name(jobName).build())
        .inputs(List.of())
        .outputs(
            List.of(
                LineageEvent.Dataset.builder()
                    .namespace(datasetNamespace)
                    .name(datasetName)
                    .build()))
        .producer(PRODUCER)
        .schemaURL(RUN_SCHEMA)
        .build();
  }

  private static LineageEvent datasetIdentityEvent(
      UUID runId, Instant eventTime, String jobName, String inputName, String outputName) {
    return LineageEvent.builder()
        .eventType("COMPLETE")
        .eventTime(eventTime.atZone(UTC))
        .run(new LineageEvent.Run(runId.toString(), null))
        .job(LineageEvent.Job.builder().namespace(NAMESPACE).name(jobName).build())
        .inputs(
            List.of(LineageEvent.Dataset.builder().namespace(NAMESPACE).name(inputName).build()))
        .outputs(
            List.of(LineageEvent.Dataset.builder().namespace(NAMESPACE).name(outputName).build()))
        .producer(PRODUCER)
        .schemaURL(RUN_SCHEMA)
        .build();
  }

  private OpenLineageQueueRow queuedHead(UUID orderingKey) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT queued.id,
                           queued.ordering_key,
                           queued.event AS event_json,
                           queued.enqueued_at,
                           head.attempt_count,
                           head.last_error
                    FROM open_lineage_queue_heads AS head
                    JOIN open_lineage_queue AS queued
                      ON queued.ordering_key = head.ordering_key
                     AND queued.id = head.event_id
                    WHERE head.ordering_key = :orderingKey
                    """)
                .bind("orderingKey", orderingKey)
                .map(
                    (resultSet, context) ->
                        new OpenLineageQueueRow(
                            resultSet.getLong("id"),
                            resultSet.getObject("ordering_key", UUID.class),
                            resultSet.getString("event_json"),
                            resultSet.getTimestamp("enqueued_at").toInstant(),
                            resultSet.getInt("attempt_count"),
                            resultSet.getString("last_error")))
                .one());
  }

  private Instant databaseNowMillis() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT date_trunc('milliseconds', statement_timestamp())")
                .mapTo(Timestamp.class)
                .one()
                .toInstant());
  }

  private void setScheduleAt(UUID orderingKey, Instant availableAt) {
    int updated =
        jdbi.withHandle(
            handle ->
                handle
                    .createUpdate(
                        "UPDATE open_lineage_queue_heads "
                            + "SET available_at = :availableAt "
                            + "WHERE ordering_key = :orderingKey")
                    .bind("availableAt", Timestamp.from(availableAt))
                    .bind("orderingKey", orderingKey)
                    .execute());
    assertThat(updated).isEqualTo(1);
  }

  private void makeScheduleDue(UUID orderingKey) {
    int updated =
        jdbi.withHandle(
            handle ->
                handle
                    .createUpdate(
                        "UPDATE open_lineage_queue_heads "
                            + "SET available_at = '-infinity'::timestamptz "
                            + "WHERE ordering_key = :orderingKey")
                    .bind("orderingKey", orderingKey)
                    .execute());
    assertThat(updated).isEqualTo(1);
  }

  private List<Long> liveEventIds() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT id FROM open_lineage_queue ORDER BY id")
                .mapTo(Long.class)
                .list());
  }

  private long headCount() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT count(*) FROM open_lineage_queue_heads")
                .mapTo(Long.class)
                .one());
  }

  private HeadState head(UUID orderingKey) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT event_id, attempt_count, last_error "
                        + "FROM open_lineage_queue_heads WHERE ordering_key = :orderingKey")
                .bind("orderingKey", orderingKey)
                .map(
                    (resultSet, context) ->
                        new HeadState(
                            resultSet.getLong("event_id"),
                            resultSet.getInt("attempt_count"),
                            resultSet.getString("last_error")))
                .one());
  }

  private HeadClockState headClock(UUID orderingKey) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT available_at,
                           available_at <= statement_timestamp() AS schedule_due
                    FROM open_lineage_queue_heads
                    WHERE ordering_key = :orderingKey
                    """)
                .bind("orderingKey", orderingKey)
                .map(
                    (resultSet, context) ->
                        new HeadClockState(
                            resultSet.getTimestamp("available_at").toInstant(),
                            resultSet.getBoolean("schedule_due")))
                .one());
  }

  private QueueScheduleState queueSchedule(UUID orderingKey) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT event_id,
                           available_at,
                           refresh_due_on_advance,
                           attempt_count,
                           last_error
                    FROM open_lineage_queue_heads
                    WHERE ordering_key = :orderingKey
                    """)
                .bind("orderingKey", orderingKey)
                .map(
                    (resultSet, context) ->
                        new QueueScheduleState(
                            resultSet.getLong("event_id"),
                            resultSet.getTimestamp("available_at").toInstant(),
                            resultSet.getBoolean("refresh_due_on_advance"),
                            resultSet.getInt("attempt_count"),
                            resultSet.getString("last_error")))
                .one());
  }

  private void assertLwwWinner(LwwActor winner) {
    JobProjectionState job =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT description,
                               current_location AS location,
                               current_run_uuid,
                               current_version_uuid,
                               open_lineage_snapshot_time,
                               open_lineage_snapshot_key,
                               open_lineage_current_run_time,
                               open_lineage_current_run_key,
                               open_lineage_current_version_time,
                               open_lineage_current_version_key
                        FROM jobs
                        WHERE namespace_name = :namespace AND name = :name
                        """)
                    .bind("namespace", NAMESPACE)
                    .bind("name", LWW_JOB)
                    .map(
                        (resultSet, context) ->
                            new JobProjectionState(
                                resultSet.getString("description"),
                                resultSet.getString("location"),
                                resultSet.getObject("current_run_uuid", UUID.class),
                                resultSet.getObject("current_version_uuid", UUID.class),
                                requiredInstant(resultSet, "open_lineage_snapshot_time"),
                                resultSet.getBytes("open_lineage_snapshot_key"),
                                requiredInstant(resultSet, "open_lineage_current_run_time"),
                                resultSet.getBytes("open_lineage_current_run_key"),
                                requiredInstant(resultSet, "open_lineage_current_version_time"),
                                resultSet.getBytes("open_lineage_current_version_key")))
                    .one());
    UUID winningJobVersion = runJobVersionUuid(winner.runId());
    assertThat(job.description()).isEqualTo(winner.payload().jobDescription());
    assertThat(job.location()).isEqualTo(winner.payload().jobLocation());
    assertThat(job.currentRunId()).isEqualTo(winner.runId());
    assertThat(job.currentVersionId()).isEqualTo(winningJobVersion);
    assertProjectionOrder(job.snapshotTime(), job.snapshotKey(), winner);
    assertProjectionOrder(job.currentRunTime(), job.currentRunKey(), winner);
    assertProjectionOrder(job.currentVersionTime(), job.currentVersionKey(), winner);

    assertThat(currentJobInputs())
        .containsExactly(new DatasetRef(NAMESPACE, winner.payload().inputName()));
    assertThat(currentJobIo())
        .containsExactlyInAnyOrder(
            new IoRef("INPUT", NAMESPACE, winner.payload().inputName()),
            new IoRef("OUTPUT", NAMESPACE, LWW_SHARED_OUTPUT),
            new IoRef("OUTPUT", NAMESPACE, winner.payload().privateOutputName()));

    DatasetProjectionState dataset = currentSharedDataset();
    assertThat(dataset.description()).isEqualTo(winner.payload().datasetDescription());
    assertProjectionOrder(dataset.snapshotTime(), dataset.snapshotKey(), winner);
    assertProjectionOrder(dataset.currentVersionTime(), dataset.currentVersionKey(), winner);
    assertThat(dataset.versionRunId()).isEqualTo(winner.runId());
    assertThat(dataset.lifecycleState()).isEqualToIgnoringCase(winner.payload().lifecycleState());
    assertThat(schemaFields(dataset.schemaVersionId()))
        .containsExactly(new SchemaRef(winner.payload().schemaFieldName(), "STRING"));
  }

  private void assertEventScopedJobVersion(LwwActor actor) {
    RunVersionLocation state =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT run.location AS run_location,
                               version.location AS version_location,
                               version.version
                        FROM runs AS run
                        JOIN job_versions AS version ON version.uuid = run.job_version_uuid
                        WHERE run.uuid = :runId
                        """)
                    .bind("runId", actor.runId())
                    .map(
                        (resultSet, context) ->
                            new RunVersionLocation(
                                resultSet.getString("run_location"),
                                resultSet.getString("version_location"),
                                resultSet.getObject("version", UUID.class)))
                    .one());
    ImmutableSet<DatasetId> inputs =
        ImmutableSet.of(
            new DatasetId(
                NamespaceName.of(NAMESPACE), DatasetName.of(actor.payload().inputName())));
    ImmutableSet<DatasetId> outputs =
        ImmutableSet.of(
            new DatasetId(NamespaceName.of(NAMESPACE), DatasetName.of(LWW_SHARED_OUTPUT)),
            new DatasetId(
                NamespaceName.of(NAMESPACE), DatasetName.of(actor.payload().privateOutputName())));
    UUID expectedVersion =
        Utils.newJobVersionFor(
                NamespaceName.of(NAMESPACE),
                JobName.of(LWW_JOB),
                inputs,
                outputs,
                actor.payload().jobLocation())
            .getValue();
    assertThat(state)
        .isEqualTo(
            new RunVersionLocation(
                actor.payload().jobLocation(), actor.payload().jobLocation(), expectedVersion));
  }

  private static void assertProjectionOrder(
      Instant actualTime, byte[] actualKey, LwwActor expected) {
    assertThat(actualTime).isEqualTo(expected.eventTime());
    assertThat(actualKey).containsExactly(expected.eventKey());
  }

  private static Instant requiredInstant(ResultSet resultSet, String column) throws SQLException {
    Timestamp timestamp = resultSet.getTimestamp(column);
    assertThat(timestamp).as(column).isNotNull();
    return timestamp.toInstant();
  }

  private static Instant nullableInstant(ResultSet resultSet, String column) throws SQLException {
    Timestamp timestamp = resultSet.getTimestamp(column);
    return timestamp == null ? null : timestamp.toInstant();
  }

  private UUID runJobVersionUuid(UUID runId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT job_version_uuid FROM runs WHERE uuid = :runId")
                .bind("runId", runId)
                .mapTo(UUID.class)
                .one());
  }

  private List<DatasetVersionIdentity> datasetVersions(String datasetName) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT version.uuid, version.version, version.run_uuid
                    FROM dataset_versions AS version
                    JOIN datasets AS dataset ON dataset.uuid = version.dataset_uuid
                    WHERE dataset.namespace_name = :namespace AND dataset.name = :name
                    ORDER BY version.version
                    """)
                .bind("namespace", NAMESPACE)
                .bind("name", datasetName)
                .map(
                    (resultSet, context) ->
                        new DatasetVersionIdentity(
                            resultSet.getObject("uuid", UUID.class),
                            resultSet.getObject("version", UUID.class),
                            resultSet.getObject("run_uuid", UUID.class)))
                .list());
  }

  private static UUID expectedDatasetVersion(String datasetName, UUID runId) {
    return Utils.newDatasetVersionFor(
            NAMESPACE,
            OpenLineageDao.DEFAULT_SOURCE_NAME,
            datasetName,
            datasetName,
            "",
            List.of(),
            runId)
        .getValue();
  }

  private List<JobVersionEdge> jobVersionEdges(UUID runA, UUID runB) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT run.uuid AS run_uuid, mapping.io_type, dataset.name AS dataset_name
                    FROM runs AS run
                    JOIN job_versions_io_mapping AS mapping
                      ON mapping.job_version_uuid = run.job_version_uuid
                    JOIN datasets AS dataset ON dataset.uuid = mapping.dataset_uuid
                    WHERE run.uuid IN (:runA, :runB)
                    ORDER BY run.uuid, mapping.io_type, dataset.name
                    """)
                .bind("runA", runA)
                .bind("runB", runB)
                .map(
                    (resultSet, context) ->
                        new JobVersionEdge(
                            resultSet.getObject("run_uuid", UUID.class),
                            resultSet.getString("io_type"),
                            resultSet.getString("dataset_name")))
                .list());
  }

  private List<JobEventVersion> jobEventVersions(UUID jobUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT location, version FROM job_versions WHERE job_uuid = :jobUuid")
                .bind("jobUuid", jobUuid)
                .map(
                    (resultSet, context) ->
                        new JobEventVersion(
                            resultSet.getString("location"),
                            resultSet.getObject("version", UUID.class)))
                .list());
  }

  private static JobEventVersion expectedJobEventVersion(
      String canonicalJobName, String inputName, String location) {
    UUID version =
        Utils.newJobVersionFor(
                NamespaceName.of(NAMESPACE),
                JobName.of(canonicalJobName),
                ImmutableSet.of(
                    new DatasetId(NamespaceName.of(NAMESPACE), DatasetName.of(inputName))),
                ImmutableSet.of(),
                location)
            .getValue();
    return new JobEventVersion(location, version);
  }

  private List<DatasetRef> currentJobInputs() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT input ->> 'namespace' AS namespace_name,
                           input ->> 'name' AS dataset_name
                    FROM jobs AS job
                    CROSS JOIN LATERAL jsonb_array_elements(job.current_inputs) AS input
                    WHERE job.namespace_name = :namespace AND job.name = :name
                    ORDER BY namespace_name, dataset_name
                    """)
                .bind("namespace", NAMESPACE)
                .bind("name", LWW_JOB)
                .map(
                    (resultSet, context) ->
                        new DatasetRef(
                            resultSet.getString("namespace_name"),
                            resultSet.getString("dataset_name")))
                .list());
  }

  private List<IoRef> currentJobIo() {
    return currentJobIo(LWW_JOB);
  }

  private List<IoRef> currentJobIo(String jobName) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT mapping.io_type, dataset.namespace_name, dataset.name AS dataset_name
                    FROM job_versions_io_mapping AS mapping
                    JOIN jobs AS job ON job.uuid = mapping.job_uuid
                    JOIN datasets AS dataset ON dataset.uuid = mapping.dataset_uuid
                    WHERE job.namespace_name = :namespace
                      AND job.name = :name
                      AND mapping.is_current_job_version IS TRUE
                    ORDER BY mapping.io_type, dataset.namespace_name, dataset.name
                    """)
                .bind("namespace", NAMESPACE)
                .bind("name", jobName)
                .map(
                    (resultSet, context) ->
                        new IoRef(
                            resultSet.getString("io_type"),
                            resultSet.getString("namespace_name"),
                            resultSet.getString("dataset_name")))
                .list());
  }

  private DatasetProjectionState currentSharedDataset() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT dataset.description,
                           dataset.open_lineage_snapshot_time,
                           dataset.open_lineage_snapshot_key,
                           dataset.open_lineage_current_version_time,
                           dataset.open_lineage_current_version_key,
                           version.run_uuid,
                           version.lifecycle_state,
                           version.dataset_schema_version_uuid
                    FROM datasets AS dataset
                    JOIN dataset_versions AS version
                      ON version.uuid = dataset.current_version_uuid
                    WHERE dataset.namespace_name = :namespace AND dataset.name = :name
                    """)
                .bind("namespace", NAMESPACE)
                .bind("name", LWW_SHARED_OUTPUT)
                .map(
                    (resultSet, context) ->
                        new DatasetProjectionState(
                            resultSet.getString("description"),
                            requiredInstant(resultSet, "open_lineage_snapshot_time"),
                            resultSet.getBytes("open_lineage_snapshot_key"),
                            requiredInstant(resultSet, "open_lineage_current_version_time"),
                            resultSet.getBytes("open_lineage_current_version_key"),
                            resultSet.getObject("run_uuid", UUID.class),
                            resultSet.getString("lifecycle_state"),
                            resultSet.getObject("dataset_schema_version_uuid", UUID.class)))
                .one());
  }

  private List<SchemaRef> schemaFields(UUID schemaVersionId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT field.name, field.type
                    FROM dataset_schema_versions_field_mapping AS mapping
                    JOIN dataset_fields AS field ON field.uuid = mapping.dataset_field_uuid
                    WHERE mapping.dataset_schema_version_uuid = :schemaVersionId
                    ORDER BY field.name
                    """)
                .bind("schemaVersionId", schemaVersionId)
                .map(
                    (resultSet, context) ->
                        new SchemaRef(resultSet.getString("name"), resultSet.getString("type")))
                .list());
  }

  private String queuedEventJson(UUID orderingKey, long eventId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT event FROM open_lineage_queue "
                        + "WHERE ordering_key = :orderingKey AND id = :eventId")
                .bind("orderingKey", orderingKey)
                .bind("eventId", eventId)
                .mapTo(String.class)
                .one());
  }

  private static AdmissionState admissionState(Handle handle, UUID orderingKey) {
    return handle
        .createQuery(
            """
            SELECT queued.id,
                   head.event_id,
                   queued.event AS event_json,
                   (SELECT count(*)
                      FROM open_lineage_queue AS payload
                     WHERE payload.ordering_key = :orderingKey) AS payload_count
            FROM open_lineage_queue_heads AS head
            JOIN open_lineage_queue AS queued
              ON queued.ordering_key = head.ordering_key
             AND queued.id = head.event_id
            WHERE head.ordering_key = :orderingKey
            """)
        .bind("orderingKey", orderingKey)
        .map(
            (resultSet, context) ->
                new AdmissionState(
                    resultSet.getLong("id"),
                    resultSet.getLong("event_id"),
                    resultSet.getString("event_json"),
                    resultSet.getLong("payload_count")))
        .one();
  }

  private long deadLetterCount() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT count(*) FROM open_lineage_dead_letters")
                .mapTo(Long.class)
                .one());
  }

  private DeadLetterState deadLetter(UUID orderingKey, long eventId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT event, attempt_count, last_error
                    FROM open_lineage_dead_letters
                    WHERE ordering_key = :orderingKey AND id = :eventId
                    """)
                .bind("orderingKey", orderingKey)
                .bind("eventId", eventId)
                .map(
                    (resultSet, context) ->
                        new DeadLetterState(
                            resultSet.getString("event"),
                            resultSet.getInt("attempt_count"),
                            resultSet.getString("last_error")))
                .one());
  }

  private static void awaitLatch(CountDownLatch latch, String description)
      throws InterruptedException {
    assertThat(latch.await(10, TimeUnit.SECONDS)).as(description).isTrue();
  }

  private void awaitBlockedBy(int waiterPid, int blockerPid) {
    assertThat(waiterPid).isPositive();
    assertThat(blockerPid).isPositive();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    boolean blocked;
    do {
      blocked =
          jdbi.withHandle(
              handle ->
                  handle
                      .createQuery("SELECT :blockerPid = ANY(pg_blocking_pids(:waiterPid))")
                      .bind("blockerPid", blockerPid)
                      .bind("waiterPid", waiterPid)
                      .mapTo(Boolean.class)
                      .one());
    } while (!blocked && System.nanoTime() < deadline);
    assertThat(blocked)
        .as("backend %s should be blocked by backend %s", waiterPid, blockerPid)
        .isTrue();
  }

  private static void stopWorker(OpenLineageWorker worker) {
    try {
      worker.stop();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError("forced worker shutdown was interrupted", interrupted);
    }
  }

  private long rawEventCount(UUID runId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT count(*) FROM lineage_events WHERE run_uuid = :runId")
                .bind("runId", runId)
                .mapTo(Long.class)
                .one());
  }

  private List<String> rawEventTypes(UUID runId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT event_type FROM lineage_events "
                        + "WHERE run_uuid = :runId ORDER BY event_time")
                .bind("runId", runId)
                .mapTo(String.class)
                .list());
  }

  private List<String> rawEventPayloads(UUID runId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT event::text
                    FROM lineage_events
                    WHERE run_uuid = :runId
                    ORDER BY event_time
                    """)
                .bind("runId", runId)
                .mapTo(String.class)
                .list());
  }

  private static JsonNode jsonTree(String eventJson) {
    try {
      return Utils.newObjectMapper().readTree(eventJson);
    } catch (IOException invalidJson) {
      throw new AssertionError("queued OpenLineage payload must be valid JSON", invalidJson);
    }
  }

  private long runCount(UUID runId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT count(*) FROM runs WHERE uuid = :runId")
                .bind("runId", runId)
                .mapTo(Long.class)
                .one());
  }

  private String runProjectionState(UUID runId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT current_run_state FROM runs WHERE uuid = :runId")
                .bind("runId", runId)
                .mapTo(String.class)
                .one());
  }

  private record HeadState(long eventId, int attemptCount, String lastError) {}

  private record AdmissionState(
      long eventId, long headEventId, String eventJson, long payloadCount) {}

  private record HeadClockState(Instant availableAt, boolean scheduleDue) {}

  private record QueueScheduleState(
      long eventId,
      Instant availableAt,
      boolean refreshDueOnAdvance,
      int attemptCount,
      String lastError) {}

  private enum TransactionOutcome {
    EMPTY,
    SUCCESS,
    RETRY,
    DEAD_LETTER
  }

  private record TransactionResult(TransactionOutcome outcome, OpenLineageQueueRow row) {}

  private record DeadLetterState(String eventJson, int attemptCount, String lastError) {}

  private record DatasetRef(String namespace, String name) {}

  private record IoRef(String ioType, String namespace, String name) {}

  private record SchemaRef(String name, String type) {}

  private record SymlinkState(String namespace, boolean primary, UUID datasetId) {}

  private record JobProjectionState(
      String description,
      String location,
      UUID currentRunId,
      UUID currentVersionId,
      Instant snapshotTime,
      byte[] snapshotKey,
      Instant currentRunTime,
      byte[] currentRunKey,
      Instant currentVersionTime,
      byte[] currentVersionKey) {}

  private record DatasetProjectionState(
      String description,
      Instant snapshotTime,
      byte[] snapshotKey,
      Instant currentVersionTime,
      byte[] currentVersionKey,
      UUID versionRunId,
      String lifecycleState,
      UUID schemaVersionId) {}

  private record LwwCase(String description, boolean equalUtcTime, boolean winnerCommitsFirst) {
    @Override
    public String toString() {
      return description;
    }
  }

  private record LwwPayload(
      UUID runId,
      String eventJson,
      String inputName,
      String privateOutputName,
      String schemaFieldName,
      String jobDescription,
      String jobLocation,
      String datasetDescription,
      String lifecycleState) {}

  private record RunVersionLocation(String runLocation, String versionLocation, UUID version) {}

  private record DatasetVersionIdentity(UUID uuid, UUID version, UUID runId) {}

  private record RunDatasetVersionMapping(UUID runId, UUID datasetVersionId) {}

  private record JobVersionEdge(UUID runId, String ioType, String datasetName) {}

  private record JobEventVersion(String location, UUID version) {}

  private record ForeignIdentitySnapshot(String runJson, String jobJson) {}

  private record RunReference(String source, UUID runId) {}

  private record ParentProjectionState(
      UUID jobUuid,
      String jobType,
      Instant jobCreatedAt,
      Instant jobUpdatedAt,
      String description,
      String jobLocation,
      String currentInputs,
      UUID currentRunId,
      boolean snapshotSentinel,
      Instant snapshotTime,
      byte[] snapshotKey,
      Instant currentRunTime,
      byte[] currentRunKey,
      Instant runCreatedAt,
      Instant runUpdatedAt,
      String currentRunState,
      Instant startedAt,
      Instant endedAt,
      Instant nominalStartTime,
      Instant nominalEndTime,
      String runLocation,
      Boolean parentPlaceholder) {}

  private record LwwActor(
      LwwPayload payload, OpenLineageQueueRow row, Instant eventTime, byte[] eventKey) {
    private UUID runId() {
      return payload.runId();
    }
  }

  private static final class FailAfterProjectionService extends OpenLineageService {
    private final UUID runId;
    private final String eventType;
    private final String failureMessage;

    private FailAfterProjectionService(
        OpenLineageDao baseDao,
        RunService runService,
        UUID runId,
        String eventType,
        String failureMessage) {
      super(baseDao, runService, Runnable::run);
      this.runId = runId;
      this.eventType = eventType;
      this.failureMessage = failureMessage;
    }

    @Override
    UpdateLineageRow processQueuedInTransaction(
        BaseEvent event, String eventJson, OpenLineageDao transactionalDao) {
      UpdateLineageRow update =
          super.processQueuedInTransaction(event, eventJson, transactionalDao);
      LineageEvent lineageEvent = (LineageEvent) event;
      if (runId.toString().equals(lineageEvent.getRun().getRunId())
          && eventType.equals(lineageEvent.getEventType())) {
        throw new IllegalStateException(failureMessage);
      }
      return update;
    }
  }

  private static final class AdvisoryBlockedAfterProjectionService extends OpenLineageService {
    private final CountDownLatch projected;
    private final AtomicInteger backendPid;
    private final long advisoryKey;

    private AdvisoryBlockedAfterProjectionService(
        OpenLineageDao baseDao,
        RunService runService,
        CountDownLatch projected,
        AtomicInteger backendPid,
        long advisoryKey) {
      super(baseDao, runService, Runnable::run);
      this.projected = projected;
      this.backendPid = backendPid;
      this.advisoryKey = advisoryKey;
    }

    @Override
    UpdateLineageRow processQueuedInTransaction(
        BaseEvent event, String eventJson, OpenLineageDao transactionalDao) {
      UpdateLineageRow update =
          super.processQueuedInTransaction(event, eventJson, transactionalDao);
      backendPid.set(
          transactionalDao
              .getHandle()
              .createQuery("SELECT pg_backend_pid()")
              .mapTo(Integer.class)
              .one());
      projected.countDown();
      transactionalDao
          .getHandle()
          .createQuery("SELECT pg_advisory_lock(:advisoryKey)")
          .bind("advisoryKey", advisoryKey)
          .map((resultSet, context) -> true)
          .one();
      return update;
    }
  }

  private static final class CrossLaneProjectionGateService extends OpenLineageService {
    private final Predicate<BaseEvent> firstEvent;
    private final Predicate<BaseEvent> secondEvent;
    private final CountDownLatch firstProjected;
    private final CountDownLatch secondEntered;
    private final CountDownLatch releaseFirst;
    private final AtomicInteger firstBackendPid;
    private final AtomicInteger secondBackendPid;

    private CrossLaneProjectionGateService(
        OpenLineageDao baseDao,
        RunService runService,
        Predicate<BaseEvent> firstEvent,
        Predicate<BaseEvent> secondEvent,
        CountDownLatch firstProjected,
        CountDownLatch secondEntered,
        CountDownLatch releaseFirst,
        AtomicInteger firstBackendPid,
        AtomicInteger secondBackendPid) {
      super(baseDao, runService, Runnable::run);
      this.firstEvent = firstEvent;
      this.secondEvent = secondEvent;
      this.firstProjected = firstProjected;
      this.secondEntered = secondEntered;
      this.releaseFirst = releaseFirst;
      this.firstBackendPid = firstBackendPid;
      this.secondBackendPid = secondBackendPid;
    }

    @Override
    UpdateLineageRow processQueuedInTransaction(
        BaseEvent event, String eventJson, OpenLineageDao transactionalDao) {
      if (firstEvent.test(event)) {
        firstBackendPid.set(backendPid(transactionalDao));
        UpdateLineageRow update =
            super.processQueuedInTransaction(event, eventJson, transactionalDao);
        firstProjected.countDown();
        awaitRelease(releaseFirst, "first cross-lane projection release");
        return update;
      }
      if (secondEvent.test(event)) {
        secondBackendPid.set(backendPid(transactionalDao));
        secondEntered.countDown();
      }
      return super.processQueuedInTransaction(event, eventJson, transactionalDao);
    }

    private static int backendPid(OpenLineageDao transactionalDao) {
      return transactionalDao
          .getHandle()
          .createQuery("SELECT pg_backend_pid()")
          .mapTo(Integer.class)
          .one();
    }
  }

  private static void awaitRelease(CountDownLatch release, String description) {
    try {
      if (!release.await(15, TimeUnit.SECONDS)) {
        throw new IllegalStateException("timed out waiting for " + description);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(description + " was interrupted", interrupted);
    }
  }

  private static final class BlockingBeforeProjectionService extends OpenLineageService {
    private final UUID blockedRunId;
    private final CountDownLatch entered;
    private final CountDownLatch release;

    private BlockingBeforeProjectionService(
        OpenLineageDao baseDao,
        RunService runService,
        UUID blockedRunId,
        CountDownLatch entered,
        CountDownLatch release) {
      super(baseDao, runService, Runnable::run);
      this.blockedRunId = blockedRunId;
      this.entered = entered;
      this.release = release;
    }

    @Override
    UpdateLineageRow processQueuedInTransaction(
        BaseEvent event, String eventJson, OpenLineageDao transactionalDao) {
      LineageEvent lineageEvent = (LineageEvent) event;
      if (blockedRunId.toString().equals(lineageEvent.getRun().getRunId())
          && "START".equals(lineageEvent.getEventType())) {
        entered.countDown();
        OpenLineageDurabilityIntegrationTest.awaitRelease(release, "blocked projection release");
      }
      return super.processQueuedInTransaction(event, eventJson, transactionalDao);
    }
  }
}
