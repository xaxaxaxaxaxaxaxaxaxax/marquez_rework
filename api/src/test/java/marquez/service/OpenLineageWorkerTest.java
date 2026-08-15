/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import java.sql.Connection;
import java.sql.Savepoint;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import marquez.common.Utils;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageQueueDao;
import marquez.db.models.OpenLineageQueueRow;
import marquez.db.models.UpdateLineageRow;
import marquez.service.OpenLineageService.ProjectedEvent;
import marquez.service.OpenLineageService.QueuedEvent;
import marquez.service.models.BaseEvent;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.slf4j.MDC;

class OpenLineageWorkerTest {
  private static final Instant NOW = Instant.parse("2026-08-13T00:00:00Z");
  private static final UUID KEY = UUID.fromString("7a93475f-cf51-416e-af32-a25c5c39d72b");
  private static final UUID OTHER_KEY = UUID.fromString("2799fb9d-4936-44aa-ab14-f67c007b45d5");
  private static final UUID THIRD_KEY = UUID.fromString("93381aa0-6d6e-4c8b-bd56-3539d4ac627a");

  private Jdbi jdbi;
  private Handle handle;
  private Connection connection;
  private Savepoint savepoint;
  private OpenLineageQueueDao queueDao;
  private OpenLineageQueueDao transactionalQueueDao;
  private OpenLineageDao transactionalOpenLineageDao;
  private OpenLineageService openLineageService;
  private OpenLineageConfig config;
  private MetricRegistry metricRegistry;
  private UpdateLineageRow update;
  private OpenLineageWorker worker;
  private List<String> transactionTrace;
  private final AtomicInteger activeConnectionsAtHandleReturn = new AtomicInteger(-1);

  @BeforeEach
  @SuppressWarnings({"rawtypes", "unchecked"})
  void setUp() throws Exception {
    jdbi = mock(Jdbi.class);
    handle = mock(Handle.class);
    connection = mock(Connection.class);
    savepoint = mock(Savepoint.class);
    queueDao = mock(OpenLineageQueueDao.class);
    transactionalQueueDao = mock(OpenLineageQueueDao.class);
    transactionalOpenLineageDao = mock(OpenLineageDao.class);
    openLineageService = mock(OpenLineageService.class);
    config = mock(OpenLineageConfig.class);
    metricRegistry = new MetricRegistry();
    update = mock(UpdateLineageRow.class);
    transactionTrace = Collections.synchronizedList(new ArrayList<>());

    when(config.getWorkerThreads()).thenReturn(2);
    when(config.getProjectionBatchSize()).thenReturn(8);
    when(config.getPollIntervalMillis()).thenReturn(10L);
    when(config.getMaxAttempts()).thenReturn(3);
    when(config.getRetryInitialDelayMillis()).thenReturn(1_000L);
    when(config.getRetryMaxDelayMillis()).thenReturn(8_000L);
    when(config.getShutdownGracePeriodMillis()).thenReturn(1_000L);
    when(handle.getTransactionIsolationLevel())
        .thenReturn(TransactionIsolationLevel.READ_COMMITTED);
    when(handle.getConnection()).thenReturn(connection);
    when(connection.setSavepoint()).thenReturn(savepoint);
    when(handle.attach(OpenLineageQueueDao.class)).thenReturn(transactionalQueueDao);
    when(handle.attach(OpenLineageDao.class)).thenReturn(transactionalOpenLineageDao);
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.empty());
    when(transactionalQueueDao.lockNextDueBatch(anyInt()))
        .thenAnswer(
            invocation -> transactionalQueueDao.lockNextDue().map(List::of).orElseGet(List::of));
    when(openLineageService.processQueuedInTransaction(
            any(BaseEvent.class), any(String.class), eq(transactionalOpenLineageDao)))
        .thenReturn(update);
    when(openLineageService.processQueuedBatchInTransaction(
            anyList(), eq(transactionalOpenLineageDao)))
        .thenAnswer(
            invocation -> {
              List<QueuedEvent> queuedEvents = invocation.getArgument(0);
              List<ProjectedEvent> projected = new ArrayList<>(queuedEvents.size());
              for (QueuedEvent queuedEvent : queuedEvents) {
                UpdateLineageRow queuedUpdate =
                    openLineageService.processQueuedInTransaction(
                        queuedEvent.event(), queuedEvent.eventJson(), transactionalOpenLineageDao);
                projected.add(
                    new ProjectedEvent(queuedEvent.queueId(), queuedEvent.event(), queuedUpdate));
              }
              return List.copyOf(projected);
            });
    when(openLineageService.publishQueuedEventBestEffort(any(BaseEvent.class), any()))
        .thenReturn(0);
    when(openLineageService.publishQueuedEventsBestEffort(anyList())).thenReturn(0);
    when(jdbi.withHandle(any(HandleCallback.class)))
        .thenAnswer(
            invocation -> {
              HandleCallback callback = invocation.getArgument(0);
              try {
                return callback.withHandle(handle);
              } finally {
                OpenLineageWorker currentWorker = worker;
                if (currentWorker != null) {
                  activeConnectionsAtHandleReturn.set(currentWorker.activeConnectionCount());
                }
              }
            });
    when(handle.inTransaction(any(HandleCallback.class)))
        .thenAnswer(
            invocation -> {
              HandleCallback callback = invocation.getArgument(0);
              transactionTrace.add("begin");
              try {
                Object result = callback.withHandle(handle);
                transactionTrace.add("commit");
                return result;
              } catch (Throwable failure) {
                transactionTrace.add("rollback");
                throw failure;
              }
            });

    worker = newWorker();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    MDC.clear();
    if (worker != null) {
      worker.stop();
    }
  }

  @Test
  void eachEventUsesOneReadCommittedTransactionAndTheTransactionHandle() throws Exception {
    OpenLineageQueueRow first = row(1, 1);
    OpenLineageQueueRow second = row(2, 1);
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(first), Optional.of(second));

    OpenLineageWorker.TaskResult result = worker.processTask(allowEvents(2));

    assertThat(result)
        .isEqualTo(new OpenLineageWorker.TaskResult(2, OpenLineageWorker.EventOutcome.COMPLETED));
    verify(jdbi, times(2)).withHandle(any(HandleCallback.class));
    verify(handle, times(2)).inTransaction(any(HandleCallback.class));
    verify(handle, never())
        .inTransaction(any(TransactionIsolationLevel.class), any(HandleCallback.class));
    verify(handle, times(2)).attach(OpenLineageQueueDao.class);
    verify(handle, times(2)).attach(OpenLineageDao.class);
    verify(transactionalQueueDao, times(2)).lockNextDue();
    assertThat(transactionTrace).containsExactly("begin", "commit", "begin", "commit");
    assertThat(activeConnectionsAtHandleReturn).hasValue(0);

    InOrder order = inOrder(connection, openLineageService, transactionalQueueDao);
    order.verify(connection).setSavepoint();
    order
        .verify(openLineageService)
        .processQueuedInTransaction(
            any(BaseEvent.class), eq(first.eventJson()), eq(transactionalOpenLineageDao));
    order.verify(connection).releaseSavepoint(savepoint);
    order.verify(transactionalQueueDao).ackLocked(KEY, first.id());
    order.verify(connection).setSavepoint();
    order
        .verify(openLineageService)
        .processQueuedInTransaction(
            any(BaseEvent.class), eq(second.eventJson()), eq(transactionalOpenLineageDao));
    order.verify(connection).releaseSavepoint(savepoint);
    order.verify(transactionalQueueDao).ackLocked(KEY, second.id());
  }

  @Test
  void oneAdmissionClaimProjectsAndAcknowledgesInOneTransactionBeforeBatchPublication()
      throws Exception {
    long admissionId = 71L;
    OpenLineageQueueRow first = batchRow(1, KEY, 1, false, admissionId);
    OpenLineageQueueRow second = batchRow(2, OTHER_KEY, 1, false, admissionId);
    doReturn(List.of(first, second)).when(transactionalQueueDao).lockNextDueBatch(anyInt());
    when(openLineageService.publishQueuedEventsBestEffort(anyList()))
        .thenAnswer(
            invocation -> {
              transactionTrace.add("publish-batch");
              return 0;
            });

    OpenLineageWorker.TaskResult result = worker.processTask(allowEvents(1));

    assertThat(result)
        .isEqualTo(new OpenLineageWorker.TaskResult(2, OpenLineageWorker.EventOutcome.COMPLETED));
    assertThat(transactionTrace).containsExactly("begin", "commit", "publish-batch");
    verify(jdbi).withHandle(any(HandleCallback.class));
    verify(handle).inTransaction(any(HandleCallback.class));
    verify(transactionalQueueDao).lockNextDueBatch(8);
    verify(connection).setSavepoint();
    verify(connection).releaseSavepoint(savepoint);
    verify(openLineageService)
        .processQueuedBatchInTransaction(
            argThat(
                events ->
                    events.size() == 2
                        && events.get(0).queueId() == first.id()
                        && events.get(0).eventJson().equals(first.eventJson())
                        && events.get(1).queueId() == second.id()
                        && events.get(1).eventJson().equals(second.eventJson())),
            eq(transactionalOpenLineageDao));
    verify(transactionalQueueDao).ackLockedAll(List.of(first, second));
    verify(transactionalQueueDao, never()).ackLocked(any(), anyLong());
    verify(openLineageService)
        .publishQueuedEventsBestEffort(
            argThat(
                events ->
                    events.size() == 2
                        && events.get(0).queueId() == first.id()
                        && events.get(1).queueId() == second.id()));
    assertThat(metricCount("selected")).isEqualTo(2);
    assertThat(metricCount("succeeded")).isEqualTo(2);
    assertThat(metricRegistry.histogram(metricName("claim_size")).getCount()).isEqualTo(1);
    assertThat(metricRegistry.histogram(metricName("claim_size")).getSnapshot().getMax())
        .isEqualTo(2);
  }

  @Test
  void failedBatchProjectionFallsBackInOrderAndRetryBlocksOnlyItsLane() throws Exception {
    long admissionId = 72L;
    OpenLineageQueueRow first = batchRow(1, KEY, 1, false, admissionId);
    OpenLineageQueueRow retry = batchRow(2, OTHER_KEY, 1, false, admissionId);
    OpenLineageQueueRow blockedFollower = batchRow(3, OTHER_KEY, 1, true, admissionId);
    OpenLineageQueueRow independent = batchRow(4, THIRD_KEY, 1, false, admissionId);
    doReturn(List.of(first, retry, blockedFollower, independent))
        .when(transactionalQueueDao)
        .lockNextDueBatch(anyInt());
    when(openLineageService.processQueuedBatchInTransaction(
            anyList(), eq(transactionalOpenLineageDao)))
        .thenThrow(new IllegalStateException("speculative batch failure"));
    when(openLineageService.processQueuedInTransaction(
            any(BaseEvent.class), any(String.class), eq(transactionalOpenLineageDao)))
        .thenAnswer(
            invocation -> {
              String eventJson = invocation.getArgument(1);
              if (eventJson.equals(retry.eventJson())) {
                throw new IllegalStateException("retry this lane");
              }
              return update;
            });

    OpenLineageWorker.TaskResult result = worker.processTask(allowEvents(1));

    assertThat(result)
        .isEqualTo(new OpenLineageWorker.TaskResult(3, OpenLineageWorker.EventOutcome.COMPLETED));
    assertThat(transactionTrace).containsExactly("begin", "commit");
    verify(transactionalQueueDao)
        .acquireOrderingKeyLocks(
            argThat(keys -> Arrays.equals(keys, new UUID[] {KEY, OTHER_KEY, THIRD_KEY})));
    verify(transactionalQueueDao)
        .ackLockedAfterLaneLock(KEY, first.id(), first.refreshDueOnAdvance());
    verify(transactionalQueueDao)
        .retryLocked(
            OTHER_KEY,
            retry.id(),
            retry.attemptCount(),
            "java.lang.IllegalStateException: retry this lane",
            500L);
    verify(transactionalQueueDao, never())
        .ackLockedAfterLaneLock(eq(OTHER_KEY), eq(blockedFollower.id()), anyBoolean());
    verify(transactionalQueueDao)
        .ackLockedAfterLaneLock(THIRD_KEY, independent.id(), independent.refreshDueOnAdvance());
    verify(transactionalQueueDao, never()).ackLockedAll(anyList());
    verify(openLineageService, times(3))
        .processQueuedInTransaction(
            any(BaseEvent.class), any(String.class), eq(transactionalOpenLineageDao));
    verify(openLineageService)
        .publishQueuedEventsBestEffort(
            argThat(
                events ->
                    events.size() == 2
                        && events.get(0).queueId() == first.id()
                        && events.get(1).queueId() == independent.id()));
    assertThat(metricCount("batch_fallback")).isEqualTo(1);
    assertThat(metricCount("selected")).isEqualTo(4);
    assertThat(metricRegistry.histogram(metricName("claim_size")).getCount()).isEqualTo(1);
    assertThat(metricRegistry.histogram(metricName("claim_size")).getSnapshot().getMax())
        .isEqualTo(4);
    assertThat(metricCount("succeeded")).isEqualTo(2);
    assertThat(metricCount("retried")).isEqualTo(1);
  }

  @Test
  void batchCancellationRollsBackTheWholeClaimWithoutFallingBack() throws Exception {
    long admissionId = 73L;
    OpenLineageQueueRow first = batchRow(1, KEY, 1, false, admissionId);
    OpenLineageQueueRow second = batchRow(2, OTHER_KEY, 1, false, admissionId);
    CancellationException cancelled = new CancellationException("batch cancelled");
    doReturn(List.of(first, second)).when(transactionalQueueDao).lockNextDueBatch(anyInt());
    when(openLineageService.processQueuedBatchInTransaction(
            anyList(), eq(transactionalOpenLineageDao)))
        .thenThrow(cancelled);

    assertThatThrownBy(() -> worker.processTask(allowEvents(1)))
        .isInstanceOf(RuntimeException.class)
        .hasRootCause(cancelled);

    assertThat(transactionTrace).containsExactly("begin", "rollback");
    verify(connection, never()).rollback(savepoint);
    verify(transactionalQueueDao, never()).acquireOrderingKeyLocks(any(UUID[].class));
    verify(transactionalQueueDao, never()).ackLockedAll(anyList());
    verify(openLineageService, never())
        .processQueuedInTransaction(any(BaseEvent.class), any(String.class), any());
    verifyNoPublication();
    assertThat(metricCount("batch_fallback")).isZero();
    assertThat(worker.healthStatus().failure()).isNotNull();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void ambiguousBatchCommitNeverPublishesTheProjectedEvents() throws Exception {
    long admissionId = 74L;
    OpenLineageQueueRow first = batchRow(1, KEY, 1, false, admissionId);
    OpenLineageQueueRow second = batchRow(2, OTHER_KEY, 1, false, admissionId);
    IllegalStateException commitFailure = new IllegalStateException("commit outcome unknown");
    doReturn(List.of(first, second)).when(transactionalQueueDao).lockNextDueBatch(anyInt());
    doAnswer(
            invocation -> {
              HandleCallback callback = invocation.getArgument(0);
              transactionTrace.add("begin");
              callback.withHandle(handle);
              transactionTrace.add("commit-attempt");
              throw commitFailure;
            })
        .when(handle)
        .inTransaction(any(HandleCallback.class));

    assertThatThrownBy(() -> worker.processTask(allowEvents(1))).isSameAs(commitFailure);

    assertThat(transactionTrace).containsExactly("begin", "commit-attempt");
    verify(transactionalQueueDao).ackLockedAll(List.of(first, second));
    verifyNoPublication();
    assertThat(metricCount("succeeded")).isZero();
    assertThat(metricCount("task_failed")).isEqualTo(1);
    assertThat(worker.healthStatus().failure()).isSameAs(commitFailure);
  }

  @Test
  void repeatableReadTransactionFailsPollAtDaoGuardBeforeStorageOrProjection() {
    when(handle.getTransactionIsolationLevel())
        .thenReturn(TransactionIsolationLevel.REPEATABLE_READ);
    when(transactionalQueueDao.isInTransaction()).thenReturn(true);
    when(transactionalQueueDao.getHandle()).thenReturn(handle);
    doCallRealMethod().when(transactionalQueueDao).lockNextDue();

    OpenLineageWorker.TaskResult result = worker.processTask(() -> true);

    assertThat(result)
        .isEqualTo(new OpenLineageWorker.TaskResult(0, OpenLineageWorker.EventOutcome.POLL_FAILED));
    assertThat(activeConnectionsAtHandleReturn).hasValue(0);
    assertThat(worker.activeConnectionCount()).isZero();
    assertThat(metricCount("poll_failed")).isEqualTo(1);
    assertThat(transactionTrace).containsExactly("begin", "rollback");
    verify(handle).getTransactionIsolationLevel();
    verify(handle).getConnection();
    verify(handle).inTransaction(any(HandleCallback.class));
    verify(handle, never())
        .inTransaction(any(TransactionIsolationLevel.class), any(HandleCallback.class));
    verify(handle).attach(OpenLineageQueueDao.class);
    verify(handle, never()).attach(OpenLineageDao.class);
    verify(transactionalQueueDao).lockNextDue();
    verify(transactionalQueueDao, never()).lockNextDueHead();
    verify(transactionalQueueDao, never()).ackLocked(any(), anyLong());
    verify(transactionalQueueDao, never())
        .retryLocked(any(), anyLong(), anyInt(), any(), anyLong());
    verify(transactionalQueueDao, never()).deadLetterLocked(any(), anyLong(), anyInt(), any());
    verifyNoInteractions(transactionalOpenLineageDao);
    verifyNoInteractions(openLineageService);
  }

  @Test
  void registersExactMetricNameAndTypeContract() {
    assertThat(metricRegistry.getGauges().keySet())
        .containsExactlyInAnyOrder(
            metricName("running"),
            metricName("coordinator_alive"),
            metricName("processor_capacity"),
            metricName("available_processor_capacity"));
    assertThat(metricRegistry.getCounters().keySet())
        .containsExactlyInAnyOrder(metricName("in_flight"), metricName("poll_empty"));
    assertThat(metricRegistry.getMeters().keySet())
        .containsExactlyInAnyOrder(
            metricName("selected"),
            metricName("succeeded"),
            metricName("retried"),
            metricName("dead_lettered"),
            metricName("poll_failed"),
            metricName("coordinator_failed"),
            metricName("task_failed"),
            metricName("state_transition_failed"),
            metricName("post_commit_failed"),
            metricName("batch_fallback"),
            metricName("forced_shutdown"),
            metricName("shutdown_incomplete"));
    assertThat(metricRegistry.getTimers().keySet())
        .containsExactlyInAnyOrder(
            metricName("poll_duration"),
            metricName("processing_duration"),
            metricName("post_commit_duration"));
    assertThat(metricRegistry.getHistograms().keySet()).containsExactly(metricName("claim_size"));
    assertThat(metricRegistry.getMetrics()).hasSize(22);
  }

  @Test
  void acknowledgementCommitsBeforeBestEffortPublication() {
    OpenLineageQueueRow row = row(1, 1);
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    doAnswer(
            invocation -> {
              transactionTrace.add("ack");
              return null;
            })
        .when(transactionalQueueDao)
        .ackLocked(KEY, row.id());
    when(openLineageService.publishQueuedEventBestEffort(any(), eq(update)))
        .thenAnswer(
            invocation -> {
              transactionTrace.add("publish");
              return 0;
            });

    worker.processTask(allowEvents(1));

    assertThat(transactionTrace).containsExactly("begin", "ack", "commit", "publish");
  }

  @Test
  void ordinaryProjectionFailureRollsBackSavepointAndCommitsRetry() throws Exception {
    OpenLineageQueueRow row = row(1, 1);
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    when(openLineageService.processQueuedInTransaction(any(), any(), any()))
        .thenThrow(new IllegalStateException("temporary\0\nfailure"));

    OpenLineageWorker.TaskResult result = worker.processTask(allowEvents(1));

    assertThat(result.outcome()).isEqualTo(OpenLineageWorker.EventOutcome.RETRIED);
    InOrder order = inOrder(connection, openLineageService, transactionalQueueDao);
    order.verify(connection).setSavepoint();
    order.verify(openLineageService).processQueuedInTransaction(any(), any(), any());
    order.verify(connection).rollback(savepoint);
    order
        .verify(transactionalQueueDao)
        .retryLocked(
            KEY, row.id(), 1, "java.lang.IllegalStateException: temporary\uFFFD failure", 500L);
    order.verify(connection).releaseSavepoint(savepoint);
    assertThat(transactionTrace).containsExactly("begin", "commit");
    verify(transactionalQueueDao, never()).ackLocked(any(), eq(row.id()));
    verify(transactionalQueueDao, never()).deadLetterLocked(any(), eq(row.id()), eq(1), any());
    verifyNoPublication();
    assertThat(metricCount("retried")).isEqualTo(1);
    assertThat(worker.healthStatus().failure()).isNull();
  }

  @Test
  void failureAtAttemptLimitCommitsDeadLetter() throws Exception {
    OpenLineageQueueRow row = row(1, 3);
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    when(openLineageService.processQueuedInTransaction(any(), any(), any()))
        .thenThrow(new IllegalStateException("still failing"));

    OpenLineageWorker.TaskResult result = worker.processTask(allowEvents(1));

    assertThat(result.outcome()).isEqualTo(OpenLineageWorker.EventOutcome.DEAD_LETTERED);
    verify(connection).rollback(savepoint);
    verify(transactionalQueueDao)
        .deadLetterLocked(KEY, row.id(), 3, "java.lang.IllegalStateException: still failing");
    verify(transactionalQueueDao, never())
        .retryLocked(any(), eq(row.id()), eq(3), any(), anyLong());
    assertThat(transactionTrace).containsExactly("begin", "commit");
    assertThat(metricCount("dead_lettered")).isEqualTo(1);
    assertThat(worker.healthStatus().failure()).isNull();
  }

  @Test
  void malformedPayloadDeadLettersWithinTheLockedTransaction() throws Exception {
    OpenLineageQueueRow row = row(1, 1, "{");
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));

    OpenLineageWorker.TaskResult result = worker.processTask(allowEvents(1));

    assertThat(result.outcome()).isEqualTo(OpenLineageWorker.EventOutcome.DEAD_LETTERED);
    verify(connection).rollback(savepoint);
    verify(transactionalQueueDao).deadLetterLocked(eq(KEY), eq(row.id()), eq(1), any());
    verifyNoInteractions(openLineageService);
    assertThat(transactionTrace).containsExactly("begin", "commit");
  }

  @Test
  void fatalProjectionErrorRollsBackWholeTransactionWithoutPersistingAttempt() throws Exception {
    OpenLineageQueueRow row = row(1, 1);
    AssertionError fatal = new AssertionError("fatal projection");
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    when(openLineageService.processQueuedInTransaction(any(), any(), any())).thenThrow(fatal);

    assertThatThrownBy(() -> worker.processTask(allowEvents(1))).isSameAs(fatal);

    assertThat(transactionTrace).containsExactly("begin", "rollback");
    verify(connection, never()).rollback(savepoint);
    verifyNoQueueTransition(row);
    assertThat(worker.healthStatus().failure()).isSameAs(fatal);
    assertThat(metricCount("task_failed")).isEqualTo(1);
  }

  @Test
  void cancellationRollsBackWholeTransactionWithoutPersistingAttempt() throws Exception {
    OpenLineageQueueRow row = row(1, 1);
    CancellationException cancelled = new CancellationException("cancelled");
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    when(openLineageService.processQueuedInTransaction(any(), any(), any())).thenThrow(cancelled);

    assertThatThrownBy(() -> worker.processTask(allowEvents(1)))
        .isInstanceOf(RuntimeException.class)
        .hasRootCause(cancelled);

    assertThat(transactionTrace).containsExactly("begin", "rollback");
    verify(connection, never()).rollback(savepoint);
    verifyNoQueueTransition(row);
    assertThat(worker.healthStatus().failure()).isNotNull();
  }

  @Test
  void shutdownCancellationDoesNotCountRolledBackEventAsProcessed() throws Exception {
    OpenLineageQueueRow row = row(1, 1);
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    when(openLineageService.processQueuedInTransaction(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              worker.stop();
              throw new CancellationException("cancelled during shutdown");
            });

    OpenLineageWorker.TaskResult result = worker.processTask(allowEvents(1));

    assertThat(result)
        .isEqualTo(new OpenLineageWorker.TaskResult(0, OpenLineageWorker.EventOutcome.CANCELLED));
    assertThat(transactionTrace).containsExactly("begin", "rollback");
    verifyNoQueueTransition(row);
    verifyNoPublication();
    assertThat(worker.activeConnectionCount()).isZero();
    assertThat(activeConnectionsAtHandleReturn).hasValue(0);
    assertThat(metricCount("task_failed")).isZero();
    assertThat(worker.healthStatus().failure()).isNull();
  }

  @Test
  void retrySqlTransitionFailureRollsBackWholeTransactionAndMarksFatal() throws Exception {
    OpenLineageQueueRow row = row(1, 1);
    IllegalStateException transitionFailure = new IllegalStateException("database unavailable");
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    when(openLineageService.processQueuedInTransaction(any(), any(), any()))
        .thenThrow(new IllegalStateException("projection failed"));
    doThrow(transitionFailure)
        .when(transactionalQueueDao)
        .retryLocked(eq(KEY), eq(row.id()), eq(1), any(), eq(500L));

    assertThatThrownBy(() -> worker.processTask(allowEvents(1)))
        .isInstanceOf(RuntimeException.class)
        .hasRootCause(transitionFailure);

    assertThat(transactionTrace).containsExactly("begin", "rollback");
    verify(connection).rollback(savepoint);
    verify(connection, never()).releaseSavepoint(savepoint);
    verify(transactionalQueueDao, never()).ackLocked(any(), eq(row.id()));
    verify(transactionalQueueDao, never()).deadLetterLocked(any(), eq(row.id()), eq(1), any());
    assertThat(metricCount("state_transition_failed")).isEqualTo(1);
    assertThat(worker.healthStatus().failure()).isNotNull();
  }

  @Test
  void postCommitRuntimeFailureDoesNotRetryCommittedEvent() {
    OpenLineageQueueRow row = row(1, 1);
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    when(openLineageService.publishQueuedEventBestEffort(any(), eq(update)))
        .thenThrow(new IllegalStateException("publisher failed"));

    OpenLineageWorker.TaskResult result = worker.processTask(allowEvents(1));

    assertThat(result.outcome()).isEqualTo(OpenLineageWorker.EventOutcome.COMPLETED);
    assertThat(transactionTrace).containsExactly("begin", "commit");
    verify(transactionalQueueDao).ackLocked(KEY, row.id());
    verifyNoRetryOrDeadLetter(row);
    assertThat(metricCount("post_commit_failed")).isEqualTo(1);
    assertThat(worker.healthStatus().failure()).isNull();
  }

  @Test
  void postCommitFatalErrorPreservesCommitAndPermanentlyFailsHealth() {
    OpenLineageQueueRow row = row(1, 1);
    AssertionError fatal = new AssertionError("fatal publisher");
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    when(openLineageService.publishQueuedEventBestEffort(any(), eq(update))).thenThrow(fatal);

    assertThatThrownBy(() -> worker.processTask(allowEvents(1))).isSameAs(fatal);

    assertThat(transactionTrace).containsExactly("begin", "commit");
    verify(transactionalQueueDao).ackLocked(KEY, row.id());
    verifyNoRetryOrDeadLetter(row);
    assertThat(worker.healthStatus().failure()).isSameAs(fatal);
    assertThat(metricCount("post_commit_failed")).isEqualTo(1);
    assertThat(metricCount("task_failed")).isEqualTo(1);
  }

  @Test
  void taskProcessesAtMostEightEventsInSeparateTransactions() {
    AtomicInteger ids = new AtomicInteger();
    when(transactionalQueueDao.lockNextDue())
        .thenAnswer(
            invocation -> {
              int id = ids.incrementAndGet();
              return Optional.of(row(id, 1));
            });

    OpenLineageWorker.TaskResult result = worker.processTask(() -> true);

    assertThat(result)
        .isEqualTo(new OpenLineageWorker.TaskResult(8, OpenLineageWorker.EventOutcome.COMPLETED));
    verify(jdbi, times(8)).withHandle(any(HandleCallback.class));
    verify(transactionalQueueDao, times(8)).lockNextDue();
    for (int remaining = 8; remaining >= 1; remaining--) {
      verify(transactionalQueueDao).lockNextDueBatch(remaining);
    }
    verify(transactionalQueueDao, times(8)).ackLocked(eq(KEY), anyLong());
    verify(openLineageService, times(8)).publishQueuedEventBestEffort(any(), eq(update));
    assertThat(transactionTrace).hasSize(16);
  }

  @Test
  void capacitySubmitsGenericTasksWithoutPrefetchingRows() throws Exception {
    ThreadPoolExecutor processors = mockProcessorWithQueue();
    doAnswer(
            invocation -> {
              processors.getQueue().add(invocation.getArgument(0));
              return null;
            })
        .when(processors)
        .execute(any(Runnable.class));
    replaceWorker(processors);

    assertThat(worker.runOneCycle()).isEqualTo(2);
    assertThat(worker.availableTaskCapacity()).isZero();
    assertThat(worker.queuedProcessorTaskCount()).isEqualTo(2);
    verifyNoInteractions(jdbi);
    verifyNoInteractions(transactionalQueueDao);

    List<Runnable> tasks = new ArrayList<>();
    processors.getQueue().drainTo(tasks);
    tasks.forEach(Runnable::run);
    assertThat(worker.availableTaskCapacity()).isEqualTo(2);
    verify(transactionalQueueDao, times(2)).lockNextDue();
  }

  @Test
  void emptyPollCommitsWithoutProjectionAndRecordsNeutralMetrics() {
    OpenLineageWorker.TaskResult result = worker.processTask(() -> true);

    assertThat(result)
        .isEqualTo(new OpenLineageWorker.TaskResult(0, OpenLineageWorker.EventOutcome.IDLE));
    assertThat(transactionTrace).containsExactly("begin", "commit");
    verify(transactionalQueueDao).lockNextDue();
    verifyNoInteractions(openLineageService);
    assertThat(metricRegistry.counter(metricName("poll_empty")).getCount()).isEqualTo(1);
    assertThat(metricRegistry.timer(metricName("poll_duration")).getCount()).isEqualTo(1);
    assertThat(metricCount("selected")).isZero();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void persistentPollFailureIsUnhealthyAndSuccessfulPollRecovers() throws Exception {
    IllegalStateException pollFailure = new IllegalStateException("database unavailable");
    when(jdbi.withHandle(any(HandleCallback.class))).thenThrow(pollFailure);

    assertThat(worker.processTask(() -> true).outcome())
        .isEqualTo(OpenLineageWorker.EventOutcome.POLL_FAILED);
    assertThat(worker.processTask(() -> true).outcome())
        .isEqualTo(OpenLineageWorker.EventOutcome.POLL_FAILED);
    assertThat(worker.processTask(() -> true).outcome())
        .isEqualTo(OpenLineageWorker.EventOutcome.POLL_FAILED);
    assertThat(worker.healthStatus().message())
        .isEqualTo("OpenLineage queue polling is persistently failing");
    assertThat(worker.healthStatus().failure()).isSameAs(pollFailure);
    assertThat(metricCount("poll_failed")).isEqualTo(3);

    doAnswer(
            invocation -> {
              HandleCallback callback = invocation.getArgument(0);
              return callback.withHandle(handle);
            })
        .when(jdbi)
        .withHandle(any(HandleCallback.class));
    assertThat(worker.processTask(() -> true).outcome())
        .isEqualTo(OpenLineageWorker.EventOutcome.IDLE);
    assertThat(worker.healthStatus().failure()).isNull();
    assertThat(worker.healthStatus().message()).isEqualTo("OpenLineage worker has not started");
  }

  @Test
  void coordinatorFatalErrorIsUnhealthyAndReleasesTaskReservation() throws Exception {
    AssertionError fatal = new AssertionError("fatal coordinator");
    ThreadPoolExecutor processors = mockProcessorWithQueue();
    doAnswer(
            invocation -> {
              throw fatal;
            })
        .when(processors)
        .execute(any(Runnable.class));
    replaceWorker(processors);

    worker.start();

    awaitFailure(fatal);
    assertThat(worker.healthStatus().message()).isEqualTo("OpenLineage worker coordinator failed");
    assertThat(worker.availableTaskCapacity()).isEqualTo(2);
    assertThat(metricCount("coordinator_failed")).isEqualTo(1);
    verifyNoInteractions(jdbi);
  }

  @Test
  void forcedShutdownAbortsActiveConnectionSynchronouslyAndRollsBack() throws Exception {
    when(config.getWorkerThreads()).thenReturn(1);
    when(config.getShutdownGracePeriodMillis()).thenReturn(25L);
    ThreadPoolExecutor processors =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
    replaceWorker(processors);
    OpenLineageQueueRow row = row(1, 1);
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row));
    CountDownLatch projectionEntered = new CountDownLatch(1);
    CountDownLatch abortRan = new CountDownLatch(1);
    AtomicReference<Thread> abortThread = new AtomicReference<>();
    when(openLineageService.processQueuedInTransaction(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              projectionEntered.countDown();
              while (abortRan.getCount() != 0) {
                try {
                  abortRan.await();
                } catch (InterruptedException ignored) {
                  // Forced shutdown interrupts first, then aborts the registered connection.
                }
              }
              throw new CancellationException("connection aborted");
            });
    doAnswer(
            invocation -> {
              Executor executor = invocation.getArgument(0);
              executor.execute(
                  () -> {
                    abortThread.set(Thread.currentThread());
                    abortRan.countDown();
                  });
              return null;
            })
        .when(connection)
        .abort(any(Executor.class));

    worker.start();
    assertThat(projectionEntered.await(2, TimeUnit.SECONDS)).isTrue();
    Thread stoppingThread = Thread.currentThread();
    worker.stop();

    verify(connection).abort(any(Executor.class));
    assertThat(abortThread).hasValue(stoppingThread);
    assertThat(transactionTrace).contains("rollback");
    verify(connection, never()).rollback(savepoint);
    verifyNoQueueTransition(row);
    assertThat(worker.activeConnectionCount()).isZero();
    assertThat(activeConnectionsAtHandleReturn).hasValue(0);
    assertThat(metricCount("forced_shutdown")).isEqualTo(1);
    assertThat(worker.healthStatus().failure()).isNull();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void uncheckedAbortFailuresAreReportedAndDoNotStopRemainingAborts() throws Exception {
    worker.stop();
    when(config.getWorkerThreads()).thenReturn(2);
    when(config.getShutdownGracePeriodMillis()).thenReturn(25L);

    Jdbi twoConnectionJdbi = mock(Jdbi.class);
    OpenLineageService blockingService = mock(OpenLineageService.class);
    CountDownLatch projectionsEntered = new CountDownLatch(2);
    CountDownLatch releaseProjections = new CountDownLatch(1);
    when(blockingService.processQueuedInTransaction(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              projectionsEntered.countDown();
              while (releaseProjections.getCount() != 0) {
                try {
                  releaseProjections.await();
                } catch (InterruptedException ignored) {
                  // Keep both registrations active until stop has attempted every abort.
                }
              }
              throw new CancellationException("released after failed abort");
            });

    Handle firstHandle = mock(Handle.class);
    Handle secondHandle = mock(Handle.class);
    Connection firstConnection = mock(Connection.class);
    Connection secondConnection = mock(Connection.class);
    configureTransactionHandle(firstHandle, firstConnection, row(1, 1));
    configureTransactionHandle(secondHandle, secondConnection, row(2, 1));
    List<Handle> handles = List.of(firstHandle, secondHandle);
    AtomicInteger nextHandle = new AtomicInteger();
    when(twoConnectionJdbi.withHandle(any(HandleCallback.class)))
        .thenAnswer(
            invocation -> {
              HandleCallback callback = invocation.getArgument(0);
              return callback.withHandle(handles.get(nextHandle.getAndIncrement()));
            });
    doThrow(new SecurityException("first abort rejected"))
        .when(firstConnection)
        .abort(any(Executor.class));
    doThrow(new SecurityException("second abort rejected"))
        .when(secondConnection)
        .abort(any(Executor.class));

    metricRegistry = new MetricRegistry();
    worker =
        new OpenLineageWorker(
            twoConnectionJdbi, queueDao, blockingService, config, metricRegistry, () -> 0.0);
    worker.start();
    try {
      assertThat(projectionsEntered.await(2, TimeUnit.SECONDS)).isTrue();

      worker.stop();

      verify(firstConnection).abort(any(Executor.class));
      verify(secondConnection).abort(any(Executor.class));
      assertThat(metricCount("shutdown_incomplete")).isEqualTo(1);
    } finally {
      releaseProjections.countDown();
    }
    awaitReleasedConnectionsAndCapacity(2);
    assertThat(worker.healthStatus().failure()).isNull();
  }

  @Test
  void retryBackoffIsBoundedAndJittered() {
    assertThat(worker.retryDelayMillis(1)).isEqualTo(500);
    assertThat(worker.retryDelayMillis(2)).isEqualTo(1_000);
    assertThat(worker.retryDelayMillis(4)).isEqualTo(4_000);
    assertThat(worker.retryDelayMillis(Integer.MAX_VALUE)).isEqualTo(4_000);
  }

  @Test
  void restoresPreviousMdcAfterProjection() {
    MDC.put("existing", "value");
    when(transactionalQueueDao.lockNextDue()).thenReturn(Optional.of(row(1, 1)));

    worker.processTask(allowEvents(1));

    assertThat(MDC.getCopyOfContextMap()).containsEntry("existing", "value").hasSize(1);
  }

  private OpenLineageWorker newWorker() {
    return new OpenLineageWorker(
        jdbi, queueDao, openLineageService, config, metricRegistry, () -> 0.0);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void configureTransactionHandle(
      Handle transactionHandle, Connection transactionConnection, OpenLineageQueueRow lockedRow)
      throws Exception {
    Savepoint transactionSavepoint = mock(Savepoint.class);
    OpenLineageQueueDao transactionQueue = mock(OpenLineageQueueDao.class);
    OpenLineageDao transactionOpenLineage = mock(OpenLineageDao.class);
    when(transactionHandle.getTransactionIsolationLevel())
        .thenReturn(TransactionIsolationLevel.READ_COMMITTED);
    when(transactionHandle.getConnection()).thenReturn(transactionConnection);
    when(transactionConnection.setSavepoint()).thenReturn(transactionSavepoint);
    when(transactionHandle.attach(OpenLineageQueueDao.class)).thenReturn(transactionQueue);
    when(transactionHandle.attach(OpenLineageDao.class)).thenReturn(transactionOpenLineage);
    when(transactionQueue.lockNextDue()).thenReturn(Optional.of(lockedRow));
    when(transactionQueue.lockNextDueBatch(anyInt())).thenReturn(List.of(lockedRow));
    when(transactionHandle.inTransaction(any(HandleCallback.class)))
        .thenAnswer(
            invocation -> {
              HandleCallback callback = invocation.getArgument(0);
              return callback.withHandle(transactionHandle);
            });
  }

  private void awaitReleasedConnectionsAndCapacity(int expectedCapacity) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while ((worker.activeConnectionCount() != 0
            || worker.availableTaskCapacity() != expectedCapacity)
        && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertThat(worker.activeConnectionCount()).isZero();
    assertThat(worker.availableTaskCapacity()).isEqualTo(expectedCapacity);
  }

  private void replaceWorker(ThreadPoolExecutor processors) throws InterruptedException {
    worker.stop();
    metricRegistry = new MetricRegistry();
    transactionTrace.clear();
    activeConnectionsAtHandleReturn.set(-1);
    worker =
        new OpenLineageWorker(
            jdbi,
            queueDao,
            openLineageService,
            config,
            metricRegistry,
            () -> 0.0,
            ignored -> processors);
  }

  private void verifyNoQueueTransition(OpenLineageQueueRow row) {
    verify(transactionalQueueDao, never()).ackLocked(any(), eq(row.id()));
    verifyNoRetryOrDeadLetter(row);
  }

  private void verifyNoRetryOrDeadLetter(OpenLineageQueueRow row) {
    verify(transactionalQueueDao, never())
        .retryLocked(any(), eq(row.id()), eq(row.attemptCount()), any(), anyLong());
    verify(transactionalQueueDao, never())
        .deadLetterLocked(any(), eq(row.id()), eq(row.attemptCount()), any());
  }

  private void verifyNoPublication() {
    verify(openLineageService, never()).publishQueuedEventBestEffort(any(), any());
    verify(openLineageService, never()).publishQueuedEventsBestEffort(anyList());
  }

  private void awaitFailure(Throwable expected) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (worker.healthStatus().failure() != expected && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertThat(worker.healthStatus().failure()).isSameAs(expected);
  }

  private long metricCount(String suffix) {
    return metricRegistry.meter(metricName(suffix)).getCount();
  }

  private static ThreadPoolExecutor mockProcessorWithQueue() {
    ThreadPoolExecutor processors = mock(ThreadPoolExecutor.class);
    when(processors.getQueue()).thenReturn(new ArrayBlockingQueue<>(2));
    when(processors.isTerminated()).thenReturn(true);
    return processors;
  }

  private static BooleanSupplier allowEvents(int count) {
    AtomicInteger remaining = new AtomicInteger(count);
    return () -> remaining.getAndDecrement() > 0;
  }

  private static String metricName(String suffix) {
    return MetricRegistry.name(OpenLineageWorker.class, suffix);
  }

  private static OpenLineageQueueRow row(long id, int attemptCount) {
    return row(id, attemptCount, eventJson(id));
  }

  private static OpenLineageQueueRow row(long id, int attemptCount, String eventJson) {
    return new OpenLineageQueueRow(id, KEY, eventJson, NOW.minusSeconds(1), attemptCount, null);
  }

  private static OpenLineageQueueRow batchRow(
      long id, UUID orderingKey, int attemptCount, boolean refreshDueOnAdvance, long admissionId) {
    return new OpenLineageQueueRow(
        id,
        orderingKey,
        eventJson(id),
        NOW.minusSeconds(1),
        attemptCount,
        null,
        refreshDueOnAdvance,
        admissionId);
  }

  private static String eventJson(long id) {
    return Utils.toJson(
        LineageEvent.builder()
            .eventType("START")
            .eventTime(NOW.atZone(ZoneOffset.UTC))
            .run(new LineageEvent.Run(new UUID(0, id).toString(), null))
            .job(LineageEvent.Job.builder().namespace("namespace").name("job").build())
            .inputs(List.of())
            .outputs(List.of())
            .producer("https://example.com/producer")
            .build());
  }
}
