/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.lifecycle.Managed;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.IntFunction;
import lombok.extern.slf4j.Slf4j;
import marquez.common.Utils;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageQueueDao;
import marquez.db.models.OpenLineageQueueRow;
import marquez.db.models.UpdateLineageRow;
import marquez.service.OpenLineageService.ProjectedEvent;
import marquez.service.OpenLineageService.QueuedEvent;
import marquez.service.models.BaseEvent;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.MDC;

/** Durable background processor for queued OpenLineage events. */
@Slf4j
public final class OpenLineageWorker implements Managed {
  private static final Object WAKE_SIGNAL = new Object();
  private static final String INTAKE_METHOD = "POST";
  private static final String INTAKE_PATH = "/api/v1/lineage";
  private static final int PERSISTENT_POLL_FAILURES = 3;

  private final Jdbi jdbi;
  private final OpenLineageService openLineageService;
  private final ObjectMapper mapper = Utils.newObjectMapper();
  private final DoubleSupplier randomDouble;
  private final int workerThreads;
  private final int projectionBatchSize;
  private final long pollIntervalMillis;
  private final int maxAttempts;
  private final long retryInitialDelayMillis;
  private final long retryMaxDelayMillis;
  private final long shutdownGracePeriodMillis;
  private final UUID workerId = UUID.randomUUID();

  private final Semaphore freeTaskSlots;
  private final ArrayBlockingQueue<Object> wakeSignals = new ArrayBlockingQueue<>(1);
  private final ThreadPoolExecutor processors;
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicBoolean stopping = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicInteger consecutivePollFailures = new AtomicInteger();
  private final AtomicReference<Throwable> taskFailure = new AtomicReference<>();
  private volatile Thread coordinator;
  private volatile Throwable coordinatorFailure;
  private volatile Throwable lastPollFailure;

  private final Object activeConnectionMonitor = new Object();
  private final Set<ConnectionRegistration> activeConnections = new HashSet<>();
  private boolean abortActiveConnections;
  private boolean connectionAbortFailed;

  private final Meter selections;
  private final Meter successes;
  private final Meter retries;
  private final Meter deadLetters;
  private final Meter pollFailures;
  private final Meter coordinatorFailures;
  private final Meter taskFailures;
  private final Meter stateTransitionFailures;
  private final Meter postCommitFailures;
  private final Meter batchFallbacks;
  private final Meter forcedShutdowns;
  private final Meter shutdownIncomplete;
  private final Timer pollDuration;
  private final Counter pollEmpty;
  private final Timer processingDuration;
  private final Timer postCommitDuration;
  private final Histogram claimSize;
  private final Counter inFlight;

  public OpenLineageWorker(
      Jdbi jdbi,
      OpenLineageQueueDao queueDao,
      OpenLineageService openLineageService,
      OpenLineageConfig config,
      MetricRegistry metricRegistry) {
    this(
        jdbi,
        queueDao,
        openLineageService,
        config,
        metricRegistry,
        () -> ThreadLocalRandom.current().nextDouble());
  }

  OpenLineageWorker(
      Jdbi jdbi,
      OpenLineageQueueDao queueDao,
      OpenLineageService openLineageService,
      OpenLineageConfig config,
      MetricRegistry metricRegistry,
      DoubleSupplier randomDouble) {
    this(
        jdbi,
        queueDao,
        openLineageService,
        config,
        metricRegistry,
        randomDouble,
        OpenLineageWorker::newProcessorPool);
  }

  OpenLineageWorker(
      Jdbi jdbi,
      OpenLineageQueueDao queueDao,
      OpenLineageService openLineageService,
      OpenLineageConfig config,
      MetricRegistry metricRegistry,
      DoubleSupplier randomDouble,
      IntFunction<ThreadPoolExecutor> processorFactory) {
    this.jdbi = Objects.requireNonNull(jdbi, "jdbi");
    Objects.requireNonNull(queueDao, "queueDao");
    this.openLineageService = Objects.requireNonNull(openLineageService, "openLineageService");
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(metricRegistry, "metricRegistry");
    this.randomDouble = Objects.requireNonNull(randomDouble, "randomDouble");

    workerThreads = requirePositive(config.getWorkerThreads(), "workerThreads");
    projectionBatchSize =
        requireInRange(
            config.getProjectionBatchSize(),
            1,
            OpenLineageConfig.MAX_PROJECTION_BATCH_SIZE,
            "projectionBatchSize");
    pollIntervalMillis = requirePositive(config.getPollIntervalMillis(), "pollIntervalMillis");
    maxAttempts = requirePositive(config.getMaxAttempts(), "maxAttempts");
    retryInitialDelayMillis =
        requirePositive(config.getRetryInitialDelayMillis(), "retryInitialDelayMillis");
    retryMaxDelayMillis = requirePositive(config.getRetryMaxDelayMillis(), "retryMaxDelayMillis");
    shutdownGracePeriodMillis =
        requireNonNegative(config.getShutdownGracePeriodMillis(), "shutdownGracePeriodMillis");
    if (retryInitialDelayMillis > retryMaxDelayMillis) {
      throw new IllegalArgumentException(
          "retryInitialDelayMillis must not exceed retryMaxDelayMillis");
    }

    freeTaskSlots = new Semaphore(workerThreads);
    processors = Objects.requireNonNull(processorFactory, "processorFactory").apply(workerThreads);
    Objects.requireNonNull(processors, "processorFactory result");

    selections = metricRegistry.meter(metricName("selected"));
    successes = metricRegistry.meter(metricName("succeeded"));
    retries = metricRegistry.meter(metricName("retried"));
    deadLetters = metricRegistry.meter(metricName("dead_lettered"));
    pollFailures = metricRegistry.meter(metricName("poll_failed"));
    coordinatorFailures = metricRegistry.meter(metricName("coordinator_failed"));
    taskFailures = metricRegistry.meter(metricName("task_failed"));
    stateTransitionFailures = metricRegistry.meter(metricName("state_transition_failed"));
    postCommitFailures = metricRegistry.meter(metricName("post_commit_failed"));
    batchFallbacks = metricRegistry.meter(metricName("batch_fallback"));
    forcedShutdowns = metricRegistry.meter(metricName("forced_shutdown"));
    shutdownIncomplete = metricRegistry.meter(metricName("shutdown_incomplete"));
    pollDuration = metricRegistry.timer(metricName("poll_duration"));
    pollEmpty = metricRegistry.counter(metricName("poll_empty"));
    processingDuration = metricRegistry.timer(metricName("processing_duration"));
    postCommitDuration = metricRegistry.timer(metricName("post_commit_duration"));
    claimSize = metricRegistry.histogram(metricName("claim_size"));
    inFlight = metricRegistry.counter(metricName("in_flight"));
    metricRegistry.register(metricName("running"), (Gauge<Integer>) () -> running.get() ? 1 : 0);
    metricRegistry.register(
        metricName("coordinator_alive"),
        (Gauge<Integer>)
            () -> {
              Thread coordinatorThread = coordinator;
              return coordinatorThread != null && coordinatorThread.isAlive() ? 1 : 0;
            });
    metricRegistry.register(metricName("processor_capacity"), (Gauge<Integer>) () -> workerThreads);
    metricRegistry.register(
        metricName("available_processor_capacity"),
        (Gauge<Integer>) () -> freeTaskSlots.availablePermits());
  }

  @Override
  public synchronized void start() {
    if (closed.get()) {
      throw new IllegalStateException("OpenLineage worker has already been stopped");
    }
    if (!started.compareAndSet(false, true)) {
      return;
    }

    running.set(true);
    coordinator = namedThreadFactory("open-lineage-coordinator-%d").newThread(this::coordinate);
    coordinator.start();
    wakeUp();
    log.info("Started OpenLineage worker {} with {} processor threads", workerId, workerThreads);
  }

  /** Wakes the local coordinator after an enqueue commits. */
  public void wakeUp() {
    wakeSignals.offer(WAKE_SIGNAL);
  }

  HealthStatus healthStatus() {
    Throwable fatalFailure = coordinatorFailure;
    if (fatalFailure != null) {
      return new HealthStatus(false, "OpenLineage worker coordinator failed", fatalFailure);
    }
    fatalFailure = taskFailure.get();
    if (fatalFailure != null) {
      return new HealthStatus(false, "OpenLineage worker task failed", fatalFailure);
    }
    Throwable pollFailure = lastPollFailure;
    if (pollFailure != null && consecutivePollFailures.get() >= PERSISTENT_POLL_FAILURES) {
      return new HealthStatus(
          false, "OpenLineage queue polling is persistently failing", pollFailure);
    }
    if (!started.get()) {
      return new HealthStatus(false, "OpenLineage worker has not started", null);
    }
    if (stopping.get() || closed.get()) {
      return new HealthStatus(false, "OpenLineage worker is stopping or stopped", null);
    }

    Thread coordinatorThread = coordinator;
    if (!running.get() || coordinatorThread == null || !coordinatorThread.isAlive()) {
      return new HealthStatus(false, "OpenLineage worker coordinator is not running", null);
    }

    fatalFailure = coordinatorFailure;
    if (fatalFailure != null) {
      return new HealthStatus(false, "OpenLineage worker coordinator failed", fatalFailure);
    }
    fatalFailure = taskFailure.get();
    if (fatalFailure != null) {
      return new HealthStatus(false, "OpenLineage worker task failed", fatalFailure);
    }
    pollFailure = lastPollFailure;
    if (pollFailure != null && consecutivePollFailures.get() >= PERSISTENT_POLL_FAILURES) {
      return new HealthStatus(
          false, "OpenLineage queue polling is persistently failing", pollFailure);
    }
    if (stopping.get() || closed.get() || !running.get()) {
      return new HealthStatus(false, "OpenLineage worker is stopping or stopped", null);
    }
    return new HealthStatus(true, "OpenLineage worker is running", null);
  }

  /** Submits one generic drain task per currently free processor slot. */
  int runOneCycle() {
    if (stopping.get() || closed.get()) {
      return 0;
    }

    int submitted = 0;
    int available = freeTaskSlots.availablePermits();
    for (int slot = 0;
        slot < available && !stopping.get() && !closed.get() && freeTaskSlots.tryAcquire();
        slot++) {
      DrainTask task = new DrainTask();
      try {
        processors.execute(task);
        submitted++;
      } catch (RejectedExecutionException failure) {
        task.cancelBeforeStart();
        if (!stopping.get() && !closed.get()) {
          throw failure;
        }
        break;
      } catch (RuntimeException | Error failure) {
        task.cancelBeforeStart();
        throw failure;
      }
    }
    return submitted;
  }

  /** Processes one bounded quantum, using request-aware claims where possible. */
  TaskResult processTask(BooleanSupplier runtimeRunning) {
    Objects.requireNonNull(runtimeRunning, "runtimeRunning");
    int processedEvents = 0;
    int claimedEvents = 0;
    EventOutcome lastOutcome = EventOutcome.IDLE;
    while (claimedEvents < projectionBatchSize
        && runtimeRunning.getAsBoolean()
        && !stopping.get()
        && !closed.get()
        && !Thread.currentThread().isInterrupted()) {
      ClaimResult result = processNextClaim(projectionBatchSize - claimedEvents);
      if (result.outcome() == EventOutcome.IDLE || result.outcome() == EventOutcome.POLL_FAILED) {
        return new TaskResult(processedEvents, result.outcome());
      }
      if (result.outcome() == EventOutcome.CANCELLED) {
        return new TaskResult(processedEvents, result.outcome());
      }
      claimedEvents += result.claimedEvents();
      processedEvents += result.processedEvents();
      lastOutcome = result.outcome();
    }
    return new TaskResult(processedEvents, lastOutcome);
  }

  private ClaimResult processNextClaim(int maxEvents) {
    AtomicReference<List<OpenLineageQueueRow>> lockedRows = new AtomicReference<>();
    TransactionBatch transactionBatch;
    try {
      transactionBatch =
          jdbi.withHandle(
              handle -> {
                ConnectionRegistration registration = registerConnection(handle.getConnection());
                try {
                  return handle.inTransaction(
                      transactionHandle -> {
                        OpenLineageQueueDao transactionalQueueDao =
                            transactionHandle.attach(OpenLineageQueueDao.class);
                        List<OpenLineageQueueRow> rows;
                        Timer.Context pollTimer = pollDuration.time();
                        try {
                          rows = transactionalQueueDao.lockNextDueBatch(maxEvents);
                        } finally {
                          pollTimer.stop();
                        }
                        if (rows == null) {
                          throw new IllegalStateException("OpenLineage queue claim returned null");
                        }
                        if (rows.isEmpty()) {
                          pollEmpty.inc();
                          return TransactionBatch.idle();
                        }

                        lockedRows.set(rows);
                        List<OpenLineageQueueRow> claimed = List.copyOf(rows);
                        lockedRows.set(claimed);
                        claimSize.update(claimed.size());
                        selections.mark(claimed.size());
                        validateClaim(claimed, maxEvents);
                        Timer.Context processingTimer = processingDuration.time();
                        try {
                          return processLockedClaim(
                              transactionHandle.getConnection(),
                              transactionHandle.attach(OpenLineageDao.class),
                              transactionalQueueDao,
                              claimed);
                        } finally {
                          processingTimer.stop();
                        }
                      });
                } finally {
                  // withHandle closes (and may pool) the connection only after this callback
                  // returns. Closing the registration here therefore follows commit/rollback and
                  // cannot race a forced abort against later pool reuse.
                  registration.close();
                }
              });
    } catch (Error failure) {
      recordTaskFailure(failure);
      throw failure;
    } catch (RuntimeException failure) {
      if (lockedRows.get() == null) {
        recordPollFailure(failure);
        return ClaimResult.special(EventOutcome.POLL_FAILED);
      }
      if (isCancellation(failure)) {
        if (stopping.get() || closed.get()) {
          return ClaimResult.special(EventOutcome.CANCELLED);
        }
        recordTaskFailure(failure);
        throw failure;
      }
      if (causedBy(failure, StateTransitionException.class)) {
        stateTransitionFailures.mark();
      }
      recordTaskFailure(failure);
      throw failure;
    }

    clearPollFailure();
    try {
      return publishCommittedBatch(transactionBatch);
    } catch (Error failure) {
      // Projection and acknowledgement have already committed. Preserve that durable result while
      // making the fatal post-commit failure visible to health checks.
      recordPostCommitFatalFailure(failure);
      throw failure;
    }
  }

  private TransactionBatch processLockedClaim(
      Connection connection,
      OpenLineageDao transactionalOpenLineageDao,
      OpenLineageQueueDao transactionalQueueDao,
      List<OpenLineageQueueRow> rows) {
    if (rows.size() == 1) {
      TransactionResult result =
          processLockedWithMdc(
              connection, transactionalOpenLineageDao, transactionalQueueDao, rows.get(0), false);
      return new TransactionBatch(rows, List.of(result));
    }

    Map<String, String> previousMdc = MDC.getCopyOfContextMap();
    installClaimMdc(rows);
    try {
      return processLockedBatch(
          connection, transactionalOpenLineageDao, transactionalQueueDao, rows);
    } finally {
      restoreMdc(previousMdc);
    }
  }

  private TransactionBatch processLockedBatch(
      Connection connection,
      OpenLineageDao transactionalOpenLineageDao,
      OpenLineageQueueDao transactionalQueueDao,
      List<OpenLineageQueueRow> rows) {
    Savepoint projection = setSavepoint(connection, rows.get(0).id());
    List<ProjectedEvent> projected;
    try {
      List<QueuedEvent> queuedEvents = new ArrayList<>(rows.size());
      for (OpenLineageQueueRow row : rows) {
        if (row.attemptCount() > maxAttempts) {
          throw new IllegalArgumentException("maximum processing attempts exceeded");
        }
        queuedEvents.add(
            new QueuedEvent(
                row.id(), mapper.readValue(row.eventJson(), BaseEvent.class), row.eventJson()));
      }

      projected =
          openLineageService.processQueuedBatchInTransaction(
              List.copyOf(queuedEvents), transactionalOpenLineageDao);
    } catch (Error failure) {
      throw failure;
    } catch (Exception failure) {
      if (isCancellation(failure)) {
        throw new ProcessingCancelledException(failure);
      }
      rollbackToSavepoint(connection, projection, rows.get(0).id());
      releaseSavepoint(connection, projection, rows.get(0).id());
      return processFallback(connection, transactionalOpenLineageDao, transactionalQueueDao, rows);
    }

    validateProjectedEvents(rows, projected);
    releaseSavepoint(connection, projection, rows.get(0).id());
    transitionBatch(
        "acknowledge an OpenLineage queue claim", () -> transactionalQueueDao.ackLockedAll(rows));

    List<TransactionResult> results = new ArrayList<>(rows.size());
    for (int index = 0; index < rows.size(); index++) {
      ProjectedEvent event = projected.get(index);
      results.add(TransactionResult.succeeded(rows.get(index), event.event(), event.update()));
    }
    return new TransactionBatch(rows, List.copyOf(results));
  }

  private TransactionBatch processFallback(
      Connection connection,
      OpenLineageDao transactionalOpenLineageDao,
      OpenLineageQueueDao transactionalQueueDao,
      List<OpenLineageQueueRow> rows) {
    batchFallbacks.mark();
    UUID[] orderingKeys =
        rows.stream().map(OpenLineageQueueRow::orderingKey).distinct().toArray(UUID[]::new);
    transitionBatch(
        "lock an OpenLineage queue claim for fallback",
        () -> transactionalQueueDao.acquireOrderingKeyLocks(orderingKeys));

    Set<UUID> blockedLanes = new HashSet<>();
    List<TransactionResult> results = new ArrayList<>(rows.size());
    for (OpenLineageQueueRow row : rows) {
      if (blockedLanes.contains(row.orderingKey())) {
        continue;
      }
      TransactionResult result =
          processLockedWithMdc(
              connection, transactionalOpenLineageDao, transactionalQueueDao, row, true);
      results.add(result);
      if (result.outcome() == EventOutcome.RETRIED) {
        blockedLanes.add(row.orderingKey());
      }
    }
    return new TransactionBatch(rows, List.copyOf(results));
  }

  private TransactionResult processLockedWithMdc(
      Connection connection,
      OpenLineageDao transactionalOpenLineageDao,
      OpenLineageQueueDao transactionalQueueDao,
      OpenLineageQueueRow row,
      boolean laneLockHeld) {
    Map<String, String> previousMdc = MDC.getCopyOfContextMap();
    installMdc(row);
    try {
      return processLocked(
          connection, transactionalOpenLineageDao, transactionalQueueDao, row, laneLockHeld);
    } finally {
      restoreMdc(previousMdc);
    }
  }

  private TransactionResult processLocked(
      Connection connection,
      OpenLineageDao transactionalOpenLineageDao,
      OpenLineageQueueDao transactionalQueueDao,
      OpenLineageQueueRow row,
      boolean laneLockHeld) {
    Savepoint projection = setSavepoint(connection, row.id());
    BaseEvent event;
    UpdateLineageRow update;
    try {
      if (row.attemptCount() > maxAttempts) {
        throw new IllegalArgumentException("maximum processing attempts exceeded");
      }
      event = mapper.readValue(row.eventJson(), BaseEvent.class);
      update =
          openLineageService.processQueuedInTransaction(
              event, row.eventJson(), transactionalOpenLineageDao);
    } catch (Error failure) {
      throw failure;
    } catch (Exception failure) {
      if (isCancellation(failure)) {
        throw new ProcessingCancelledException(failure);
      }
      rollbackToSavepoint(connection, projection, row.id());
      String error = errorSummary(failure);
      if (causedBy(failure, JsonProcessingException.class)
          || causedBy(failure, IllegalArgumentException.class)
          || row.attemptCount() >= maxAttempts) {
        transition(
            row,
            "dead-letter",
            () -> {
              if (laneLockHeld) {
                transactionalQueueDao.deadLetterLockedAfterLaneLock(
                    row.orderingKey(),
                    row.id(),
                    row.attemptCount(),
                    error,
                    row.refreshDueOnAdvance());
              } else {
                transactionalQueueDao.deadLetterLocked(
                    row.orderingKey(), row.id(), row.attemptCount(), error);
              }
            });
        releaseSavepoint(connection, projection, row.id());
        return TransactionResult.deadLettered(row, error);
      }

      long delayMillis = retryDelayMillis(row.attemptCount());
      transition(
          row,
          "retry",
          () ->
              transactionalQueueDao.retryLocked(
                  row.orderingKey(), row.id(), row.attemptCount(), error, delayMillis));
      releaseSavepoint(connection, projection, row.id());
      return TransactionResult.retried(row, error, delayMillis);
    }

    releaseSavepoint(connection, projection, row.id());
    transition(
        row,
        "acknowledge",
        () -> {
          if (laneLockHeld) {
            transactionalQueueDao.ackLockedAfterLaneLock(
                row.orderingKey(), row.id(), row.refreshDueOnAdvance());
          } else {
            transactionalQueueDao.ackLocked(row.orderingKey(), row.id());
          }
        });
    return TransactionResult.succeeded(row, event, update);
  }

  private ClaimResult publishCommittedBatch(TransactionBatch batch) {
    if (batch.claimedRows().isEmpty()) {
      return ClaimResult.special(EventOutcome.IDLE);
    }
    if (batch.claimedRows().size() == 1) {
      return new ClaimResult(1, 1, publishCommittedSingleton(batch.results().get(0)));
    }

    List<ProjectedEvent> committedEvents = new ArrayList<>();
    EventOutcome lastOutcome = EventOutcome.IDLE;
    for (TransactionResult result : batch.results()) {
      OpenLineageQueueRow row = result.row();
      Map<String, String> previousMdc = MDC.getCopyOfContextMap();
      installMdc(row);
      try {
        if (result.outcome() == EventOutcome.RETRIED) {
          retries.mark();
          log.warn(
              "Queued OpenLineage event {} failed on attempt {}; retrying in {} ms: {}",
              row.id(),
              row.attemptCount(),
              result.retryDelayMillis(),
              result.error());
        } else if (result.outcome() == EventOutcome.DEAD_LETTERED) {
          deadLetters.mark();
          log.error(
              "Dead-lettered queued OpenLineage event {} after attempt {}: {}",
              row.id(),
              row.attemptCount(),
              result.error());
        } else {
          successes.mark();
          committedEvents.add(new ProjectedEvent(row.id(), result.event(), result.update()));
        }
      } finally {
        restoreMdc(previousMdc);
      }
      lastOutcome = result.outcome();
    }

    if (!committedEvents.isEmpty()) {
      Map<String, String> previousMdc = MDC.getCopyOfContextMap();
      installClaimMdc(batch.claimedRows());
      Timer.Context postCommitTimer = postCommitDuration.time();
      try {
        try {
          int failures =
              openLineageService.publishQueuedEventsBestEffort(List.copyOf(committedEvents));
          if (failures > 0) {
            postCommitFailures.mark(failures);
          }
        } catch (RuntimeException failure) {
          postCommitFailures.mark();
          log.error(
              "Unexpected post-commit failure for OpenLineage queue claim containing {} event(s)",
              committedEvents.size(),
              failure);
        }
      } finally {
        postCommitTimer.stop();
        restoreMdc(previousMdc);
      }
    }

    return new ClaimResult(batch.claimedRows().size(), batch.results().size(), lastOutcome);
  }

  private EventOutcome publishCommittedSingleton(TransactionResult result) {
    OpenLineageQueueRow row = result.row();
    if (result.outcome() == EventOutcome.RETRIED) {
      retries.mark();
      log.warn(
          "Queued OpenLineage event {} failed on attempt {}; retrying in {} ms: {}",
          row.id(),
          row.attemptCount(),
          result.retryDelayMillis(),
          result.error());
      return EventOutcome.RETRIED;
    }
    if (result.outcome() == EventOutcome.DEAD_LETTERED) {
      deadLetters.mark();
      log.error(
          "Dead-lettered queued OpenLineage event {} after attempt {}: {}",
          row.id(),
          row.attemptCount(),
          result.error());
      return EventOutcome.DEAD_LETTERED;
    }

    successes.mark();
    Timer.Context postCommitTimer = postCommitDuration.time();
    try {
      try {
        int failures =
            openLineageService.publishQueuedEventBestEffort(result.event(), result.update());
        if (failures > 0) {
          postCommitFailures.mark(failures);
        }
      } catch (RuntimeException failure) {
        postCommitFailures.mark();
        log.error(
            "Unexpected post-commit failure for queued OpenLineage event {}", row.id(), failure);
      }
    } finally {
      postCommitTimer.stop();
    }
    return EventOutcome.COMPLETED;
  }

  @Override
  public synchronized void stop() throws InterruptedException {
    if (!stopping.compareAndSet(false, true)) {
      return;
    }

    long softDeadline = shutdownDeadline();
    running.set(false);
    processors.shutdown();
    wakeUp();
    Thread coordinatorThread = coordinator;
    if (coordinatorThread != null) {
      coordinatorThread.interrupt();
    }

    InterruptedException interruption = null;
    if (coordinatorThread != null) {
      try {
        joinUntil(coordinatorThread, softDeadline);
      } catch (InterruptedException interrupted) {
        interruption = interrupted;
      }
    }
    long drainNanos = remainingNanos(softDeadline);
    if (drainNanos > 0) {
      try {
        processors.awaitTermination(drainNanos, TimeUnit.NANOSECONDS);
      } catch (InterruptedException interrupted) {
        if (interruption == null) {
          interruption = interrupted;
        }
      }
    }

    boolean forcedShutdown = false;
    boolean coordinatorAlive = coordinatorThread != null && coordinatorThread.isAlive();
    if (interruption != null || coordinatorAlive || !processors.isTerminated()) {
      forcedShutdown = true;
      forcedShutdowns.mark();
      long hardDeadline = shutdownDeadline();
      List<Runnable> neverStarted = processors.shutdownNow();
      cancelUnstartedTasks(neverStarted);
      abortRegisteredConnections();
      if (coordinatorThread != null) {
        coordinatorThread.interrupt();
      }

      log.warn(
          "OpenLineage worker {} graceful shutdown did not complete; interrupted tasks and "
              + "aborted active database connections; coordinatorAlive={}, "
              + "processorsTerminated={}, shutdownInterrupted={}, cancelledTasks={}",
          workerId,
          coordinatorAlive,
          processors.isTerminated(),
          interruption != null,
          neverStarted.size());

      if (coordinatorThread != null) {
        try {
          joinUntil(coordinatorThread, hardDeadline);
        } catch (InterruptedException interrupted) {
          if (interruption == null) {
            interruption = interrupted;
          }
        }
      }
      long cancelNanos = remainingNanos(hardDeadline);
      if (cancelNanos > 0) {
        try {
          processors.awaitTermination(cancelNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
          if (interruption == null) {
            interruption = interrupted;
          }
        }
      }
    }

    closed.set(true);
    coordinatorAlive = coordinatorThread != null && coordinatorThread.isAlive();
    boolean processorsTerminated = processors.isTerminated();
    boolean connectionsRemain = activeConnectionCount() != 0;
    if (!coordinatorAlive && processorsTerminated && !connectionsRemain && !connectionAbortFailed) {
      log.info("Stopped OpenLineage worker {}; forcedShutdown={}", workerId, forcedShutdown);
    } else {
      shutdownIncomplete.mark();
      log.warn(
          "OpenLineage worker {} shutdown is incomplete; coordinatorAlive={}, "
              + "processorsTerminated={}, activeConnections={}, connectionAbortFailed={}, "
              + "shutdownInterrupted={}",
          workerId,
          coordinatorAlive,
          processorsTerminated,
          activeConnectionCount(),
          connectionAbortFailed,
          interruption != null);
    }
    if (interruption != null) {
      Thread.currentThread().interrupt();
      throw interruption;
    }
  }

  private void coordinate() {
    try {
      while (running.get()) {
        runOneCycle();
        if (running.get()) {
          try {
            wakeSignals.poll(pollIntervalMillis, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ignored) {
            if (!running.get()) {
              return;
            }
          }
        }
      }
    } catch (Error failure) {
      coordinatorFailure = failure;
      coordinatorFailures.mark();
      throw failure;
    } catch (RuntimeException failure) {
      coordinatorFailure = failure;
      coordinatorFailures.mark();
      log.error("OpenLineage worker coordinator failed", failure);
    } finally {
      running.set(false);
    }
  }

  private void recordPollFailure(Throwable failure) {
    pollFailures.mark();
    lastPollFailure = failure;
    int failures = consecutivePollFailures.incrementAndGet();
    log.error(
        "OpenLineage queue poll failed ({} consecutive failure{}); polling will continue",
        failures,
        failures == 1 ? "" : "s",
        failure);
  }

  private void clearPollFailure() {
    consecutivePollFailures.set(0);
    lastPollFailure = null;
  }

  private void recordTaskFailure(Throwable failure) {
    taskFailures.mark();
    taskFailure.compareAndSet(null, failure);
    log.error(
        "OpenLineage worker task failed; its transaction rolled back or its commit outcome is "
            + "indeterminate",
        failure);
  }

  private void recordPostCommitFatalFailure(Throwable failure) {
    postCommitFailures.mark();
    taskFailures.mark();
    taskFailure.compareAndSet(null, failure);
    log.error(
        "OpenLineage worker task failed after its durable transaction committed; "
            + "committed events will not be retried",
        failure);
  }

  private ConnectionRegistration registerConnection(Connection connection) {
    ConnectionRegistration registration = new ConnectionRegistration(connection);
    synchronized (activeConnectionMonitor) {
      activeConnections.add(registration);
      if (abortActiveConnections) {
        registration.abort();
      }
    }
    return registration;
  }

  private void abortRegisteredConnections() {
    synchronized (activeConnectionMonitor) {
      abortActiveConnections = true;
      for (ConnectionRegistration registration : new ArrayList<>(activeConnections)) {
        registration.abort();
      }
    }
  }

  int activeConnectionCount() {
    synchronized (activeConnectionMonitor) {
      return activeConnections.size();
    }
  }

  int availableTaskCapacity() {
    return freeTaskSlots.availablePermits();
  }

  int queuedProcessorTaskCount() {
    return processors.getQueue().size();
  }

  long retryDelayMillis(int attemptCount) {
    long cap = retryInitialDelayMillis;
    for (int attempt = 1; attempt < attemptCount && cap < retryMaxDelayMillis; attempt++) {
      cap = cap > retryMaxDelayMillis / 2 ? retryMaxDelayMillis : cap * 2;
    }
    cap = Math.min(cap, retryMaxDelayMillis);
    long floor = cap / 2;
    long range = cap - floor;
    double sample = Math.max(0.0, Math.min(Math.nextDown(1.0), randomDouble.getAsDouble()));
    return floor + (long) Math.floor(sample * (range + 1));
  }

  private static void validateClaim(List<OpenLineageQueueRow> rows, int maxEvents) {
    if (rows.isEmpty() || rows.size() > maxEvents) {
      throw new IllegalStateException(
          "Expected between 1 and "
              + maxEvents
              + " claimed OpenLineage events, but received "
              + rows.size());
    }

    Long admissionId = rows.get(0).admissionId();
    long previousId = 0;
    for (OpenLineageQueueRow row : rows) {
      if (row.id() <= previousId) {
        throw new IllegalStateException("OpenLineage queue claim is not ordered by event ID");
      }
      if (!Objects.equals(admissionId, row.admissionId())) {
        throw new IllegalStateException("OpenLineage queue claim crosses admission boundaries");
      }
      previousId = row.id();
    }
    if (admissionId == null && rows.size() != 1) {
      throw new IllegalStateException(
          "Legacy OpenLineage queue admissions must be claimed as singletons");
    }
  }

  private static void validateProjectedEvents(
      List<OpenLineageQueueRow> rows, List<ProjectedEvent> projected) {
    if (projected == null || projected.size() != rows.size()) {
      throw new BatchProjectionContractException(
          "Expected "
              + rows.size()
              + " projected OpenLineage events, but received "
              + (projected == null ? "null" : projected.size()));
    }
    for (int index = 0; index < rows.size(); index++) {
      ProjectedEvent event = projected.get(index);
      if (event == null || event.queueId() != rows.get(index).id()) {
        throw new BatchProjectionContractException(
            "OpenLineage batch projection result does not match queue event at index " + index);
      }
    }
  }

  private static void transition(OpenLineageQueueRow row, String description, Runnable transition) {
    try {
      transition.run();
    } catch (RuntimeException failure) {
      throw new StateTransitionException(
          "Failed to " + description + " queued OpenLineage event " + row.id(), failure);
    }
  }

  private static void transitionBatch(String description, Runnable transition) {
    try {
      transition.run();
    } catch (RuntimeException failure) {
      throw new StateTransitionException("Failed to " + description, failure);
    }
  }

  private static Savepoint setSavepoint(Connection connection, long eventId) {
    try {
      return connection.setSavepoint();
    } catch (SQLException failure) {
      throw new TransactionStepException(
          "Failed to create projection savepoint for queued OpenLineage event " + eventId, failure);
    }
  }

  private static void rollbackToSavepoint(
      Connection connection, Savepoint savepoint, long eventId) {
    try {
      connection.rollback(savepoint);
    } catch (SQLException failure) {
      throw new TransactionStepException(
          "Failed to roll back projection savepoint for queued OpenLineage event " + eventId,
          failure);
    }
  }

  private static void releaseSavepoint(Connection connection, Savepoint savepoint, long eventId) {
    try {
      connection.releaseSavepoint(savepoint);
    } catch (SQLException failure) {
      throw new TransactionStepException(
          "Failed to release projection savepoint for queued OpenLineage event " + eventId,
          failure);
    }
  }

  private void installMdc(OpenLineageQueueRow row) {
    Map<String, String> context = baseMdc();
    context.put("requestID", "open-lineage-queue-" + row.id());
    context.put("queueEventID", Long.toString(row.id()));
    context.put("queueAttempt", Integer.toString(row.attemptCount()));
    if (row.admissionId() != null) {
      context.put("queueAdmissionID", Long.toString(row.admissionId()));
    }
    MDC.setContextMap(context);
  }

  private void installClaimMdc(List<OpenLineageQueueRow> rows) {
    OpenLineageQueueRow first = rows.get(0);
    Map<String, String> context = baseMdc();
    Long admissionId = first.admissionId();
    context.put(
        "requestID",
        admissionId == null
            ? "open-lineage-queue-" + first.id()
            : "open-lineage-admission-" + admissionId);
    if (admissionId != null) {
      context.put("queueAdmissionID", Long.toString(admissionId));
    }
    context.put("queueClaimSize", Integer.toString(rows.size()));
    MDC.setContextMap(context);
  }

  private Map<String, String> baseMdc() {
    Map<String, String> context = new HashMap<>();
    context.put("queueWorkerID", workerId.toString());
    context.put("method", INTAKE_METHOD);
    context.put("path", INTAKE_PATH);
    context.put("pathWithParams", INTAKE_PATH);
    return context;
  }

  private static void restoreMdc(Map<String, String> previousMdc) {
    if (previousMdc == null) {
      MDC.clear();
    } else {
      MDC.setContextMap(previousMdc);
    }
  }

  private static boolean isCancellation(Throwable failure) {
    return Thread.currentThread().isInterrupted()
        || causedBy(failure, InterruptedException.class)
        || causedBy(failure, CancellationException.class);
  }

  private static boolean causedBy(Throwable failure, Class<? extends Throwable> type) {
    Set<Throwable> visited = new HashSet<>();
    Throwable current = failure;
    while (current != null && visited.add(current)) {
      if (type.isInstance(current)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static String errorSummary(Throwable failure) {
    Set<Throwable> visited = new HashSet<>();
    Throwable root = failure;
    while (root.getCause() != null && visited.add(root)) {
      root = root.getCause();
    }
    String message = root.getMessage();
    String summary =
        root.getClass().getName() + (message == null || message.isBlank() ? "" : ": " + message);
    return summary.replace('\0', '\uFFFD').replace('\n', ' ').replace('\r', ' ');
  }

  private static ThreadFactory namedThreadFactory(String format) {
    AtomicInteger threadNumber = new AtomicInteger();
    return runnable -> {
      Thread thread = new Thread(runnable, String.format(format, threadNumber.incrementAndGet()));
      thread.setUncaughtExceptionHandler(
          (failedThread, failure) ->
              log.error("Uncaught exception in {}", failedThread.getName(), failure));
      return thread;
    };
  }

  private static ThreadPoolExecutor newProcessorPool(int workerThreads) {
    return new ThreadPoolExecutor(
        workerThreads,
        workerThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(workerThreads),
        namedThreadFactory("open-lineage-worker-%d"),
        new ThreadPoolExecutor.AbortPolicy());
  }

  private static String metricName(String suffix) {
    return MetricRegistry.name(OpenLineageWorker.class, suffix);
  }

  private static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  private static int requireInRange(int value, int minimum, int maximum, String name) {
    if (value < minimum || value > maximum) {
      throw new IllegalArgumentException(name + " must be between " + minimum + " and " + maximum);
    }
    return value;
  }

  private static long requirePositive(long value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  private static long requireNonNegative(long value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
    return value;
  }

  private static long remainingNanos(long deadlineNanos) {
    return Math.max(0, deadlineNanos - System.nanoTime());
  }

  private void cancelUnstartedTasks(List<Runnable> neverStarted) {
    for (Runnable runnable : neverStarted) {
      if (runnable instanceof DrainTask task) {
        task.cancelBeforeStart();
      }
    }
  }

  private long shutdownDeadline() {
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(shutdownGracePeriodMillis);
  }

  private static void joinUntil(Thread thread, long deadlineNanos) throws InterruptedException {
    long remainingNanos = remainingNanos(deadlineNanos);
    if (remainingNanos == 0) {
      return;
    }
    long remainingMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
    int extraNanos = (int) (remainingNanos - TimeUnit.MILLISECONDS.toNanos(remainingMillis));
    thread.join(remainingMillis, extraNanos);
  }

  enum EventOutcome {
    IDLE,
    COMPLETED,
    RETRIED,
    DEAD_LETTERED,
    POLL_FAILED,
    CANCELLED
  }

  record TaskResult(int processedEvents, EventOutcome outcome) {}

  record HealthStatus(boolean healthy, String message, Throwable failure) {}

  private record ClaimResult(int claimedEvents, int processedEvents, EventOutcome outcome) {
    private static ClaimResult special(EventOutcome outcome) {
      return new ClaimResult(0, 0, outcome);
    }
  }

  private record TransactionBatch(
      List<OpenLineageQueueRow> claimedRows, List<TransactionResult> results) {
    private TransactionBatch {
      claimedRows = List.copyOf(claimedRows);
      results = List.copyOf(results);
    }

    private static TransactionBatch idle() {
      return new TransactionBatch(List.of(), List.of());
    }
  }

  private record TransactionResult(
      EventOutcome outcome,
      OpenLineageQueueRow row,
      BaseEvent event,
      UpdateLineageRow update,
      String error,
      long retryDelayMillis) {
    private static TransactionResult succeeded(
        OpenLineageQueueRow row, BaseEvent event, UpdateLineageRow update) {
      return new TransactionResult(EventOutcome.COMPLETED, row, event, update, null, 0);
    }

    private static TransactionResult retried(
        OpenLineageQueueRow row, String error, long retryDelayMillis) {
      return new TransactionResult(EventOutcome.RETRIED, row, null, null, error, retryDelayMillis);
    }

    private static TransactionResult deadLettered(OpenLineageQueueRow row, String error) {
      return new TransactionResult(EventOutcome.DEAD_LETTERED, row, null, null, error, 0);
    }
  }

  private final class DrainTask implements Runnable {
    private final AtomicBoolean started = new AtomicBoolean();

    @Override
    public void run() {
      if (!started.compareAndSet(false, true)) {
        return;
      }
      inFlight.inc();
      TaskResult result = new TaskResult(0, EventOutcome.IDLE);
      try {
        result = processTask(() -> !stopping.get() && !Thread.currentThread().isInterrupted());
      } finally {
        inFlight.dec();
        freeTaskSlots.release();
        if (result.processedEvents() > 0 && running.get()) {
          wakeUp();
        }
      }
    }

    private void cancelBeforeStart() {
      if (started.compareAndSet(false, true)) {
        freeTaskSlots.release();
      }
    }
  }

  private final class ConnectionRegistration implements AutoCloseable {
    private final Connection connection;
    private boolean closed;
    private boolean aborted;

    private ConnectionRegistration(Connection connection) {
      this.connection = Objects.requireNonNull(connection, "connection");
    }

    private void abort() {
      if (closed || aborted) {
        return;
      }
      aborted = true;
      try {
        // pgjdbc schedules its abort command on the supplied executor. Running it directly keeps
        // the active-transaction registration until the socket has actually been closed.
        connection.abort(Runnable::run);
      } catch (SQLException | RuntimeException failure) {
        connectionAbortFailed = true;
        log.error("Failed to abort an active OpenLineage worker database connection", failure);
      }
    }

    @Override
    public void close() {
      synchronized (activeConnectionMonitor) {
        if (closed) {
          return;
        }
        activeConnections.remove(this);
        closed = true;
      }
    }
  }

  private static final class ProcessingCancelledException extends RuntimeException {
    private ProcessingCancelledException(Throwable cause) {
      super("Queued OpenLineage projection was cancelled", cause);
    }
  }

  private static final class StateTransitionException extends RuntimeException {
    private StateTransitionException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final class TransactionStepException extends RuntimeException {
    private TransactionStepException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final class BatchProjectionContractException extends RuntimeException {
    private BatchProjectionContractException(String message) {
      super(message);
    }
  }
}
