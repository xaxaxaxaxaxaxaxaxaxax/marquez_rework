/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static marquez.logging.MdcPropagating.withMdc;
import static marquez.tracing.SentryPropagating.withSentry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import lombok.extern.slf4j.Slf4j;
import marquez.common.Utils;
import marquez.common.models.JobName;
import marquez.common.models.JobVersionId;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.BaseDao;
import marquez.db.OpenLineageDao;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.JobRow;
import marquez.db.models.ProjectionOrder;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunRow;
import marquez.db.models.RunStateRow;
import marquez.db.models.UpdateLineageRow;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import marquez.service.models.RunMeta;

@Slf4j
public class OpenLineageService extends DelegatingDaos.DelegatingOpenLineageDao {
  private final RunService runService;
  private final SearchService searchService;
  private final ObjectMapper mapper = Utils.newObjectMapper();
  private final Executor executor;

  public OpenLineageService(BaseDao baseDao, RunService runService) {
    this(baseDao, runService, null, ForkJoinPool.commonPool());
  }

  public OpenLineageService(BaseDao baseDao, RunService runService, Executor executor) {
    this(baseDao, runService, null, executor);
  }

  public OpenLineageService(BaseDao baseDao, RunService runService, SearchService searchService) {
    this(baseDao, runService, searchService, ForkJoinPool.commonPool());
  }

  public OpenLineageService(
      BaseDao baseDao, RunService runService, SearchService searchService, Executor executor) {
    super(baseDao.createOpenLineageDao());
    this.runService = runService;
    this.searchService = searchService;
    this.executor = executor;
  }

  /**
   * Persists a queued event through a transaction-attached DAO.
   *
   * <p>The caller owns the transaction and is responsible for acknowledging the durable queue row
   * on the same Jdbi handle. Post-commit side effects are deliberately excluded from this method.
   * The {@code event} must have been deserialized from {@code eventJson}. Queued lineage projection
   * and raw storage share that transaction, so either failure rolls back both writes.
   *
   * @return the relational update required by run-transition listeners, or {@code null} when the
   *     event has no listener notification
   */
  UpdateLineageRow processQueuedInTransaction(
      BaseEvent event, String eventJson, OpenLineageDao transactionalDao) {
    ProjectionOrder order =
        new ProjectionOrder(
            eventTime(event).withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
            Utils.sha256Utf8(eventJson));
    if (event instanceof DatasetEvent datasetEvent) {
      transactionalDao.createDatasetEvent(
          datasetEvent.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
          OpenLineageDao.createJsonObject(eventJson),
          datasetEvent.getProducer());
      transactionalDao.updateMarquezModel(datasetEvent, mapper, order);
      return null;
    }

    if (event instanceof JobEvent jobEvent) {
      transactionalDao.createJobEvent(
          jobEvent.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
          jobEvent.getJob().getName(),
          jobEvent.getJob().getNamespace(),
          OpenLineageDao.createJsonObject(eventJson),
          jobEvent.getProducer());
      transactionalDao.updateMarquezModel(jobEvent, mapper, order);
      return null;
    }

    if (event instanceof LineageEvent lineageEvent) {
      boolean listenerSnapshotRequired =
          lineageEvent.getEventType() != null && runService.hasRunTransitionListeners();
      UpdateLineageRow update =
          transactionalDao.updateMarquezModel(
              lineageEvent, mapper, listenerSnapshotRequired, order);
      UUID effectiveRunUuid = effectiveRunUuid(update);
      transactionalDao.createLineageEvent(
          lineageEvent.getEventType() == null ? "" : lineageEvent.getEventType(),
          lineageEvent.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
          effectiveRunUuid,
          lineageEvent.getJob().getName(),
          lineageEvent.getJob().getNamespace(),
          OpenLineageDao.createJsonObject(eventJson),
          lineageEvent.getProducer());
      return update;
    }

    throw new IllegalArgumentException(
        "Unsupported OpenLineage event type: " + event.getClass().getName());
  }

  private static java.time.ZonedDateTime eventTime(BaseEvent event) {
    if (event instanceof DatasetEvent datasetEvent) {
      return datasetEvent.getEventTime();
    }
    if (event instanceof JobEvent jobEvent) {
      return jobEvent.getEventTime();
    }
    if (event instanceof LineageEvent lineageEvent) {
      return lineageEvent.getEventTime();
    }
    throw new IllegalArgumentException(
        "Unsupported OpenLineage event type: " + event.getClass().getName());
  }

  /** Publishes non-transactional projections after the queue ACK has committed. */
  int publishQueuedEventBestEffort(BaseEvent event, UpdateLineageRow update) {
    if (!(event instanceof LineageEvent lineageEvent)) {
      return 0;
    }

    int failures = 0;
    if (searchService != null) {
      try {
        if (!searchService.indexEvent(lineageEvent, effectiveRunUuid(update))) {
          failures++;
        }
      } catch (RuntimeException e) {
        failures++;
        log.error("Failed to index queued OpenLineage event", e);
      }
    }

    if (lineageEvent.getEventType() != null && runService.hasRunTransitionListeners()) {
      try {
        failures += notifyListeners(lineageEvent, update);
      } catch (RuntimeException e) {
        failures++;
        log.error("Failed to notify listeners for queued OpenLineage event", e);
      }
    }
    return failures;
  }

  private static UUID effectiveRunUuid(UpdateLineageRow update) {
    UpdateLineageRow effectiveUpdate =
        Objects.requireNonNull(update, "queued lineage projection returned no update");
    RunRow effectiveRun =
        Objects.requireNonNull(
            effectiveUpdate.getRun(), "queued lineage projection returned no run");
    return Objects.requireNonNull(
        effectiveRun.getUuid(), "queued lineage projection returned no run UUID");
  }

  public CompletableFuture<Void> createAsync(DatasetEvent event) {
    return submit(
        () ->
            attemptBoth(
                () ->
                    createDatasetEvent(
                        event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
                        createJsonArray(event, mapper),
                        event.getProducer()),
                () -> updateMarquezModel(event, mapper)));
  }

  public CompletableFuture<Void> createAsync(JobEvent event) {
    return submit(
        () ->
            attemptBoth(
                () ->
                    createJobEvent(
                        event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
                        event.getJob().getName(),
                        event.getJob().getNamespace(),
                        createJsonArray(event, mapper),
                        event.getProducer()),
                () -> updateMarquezModel(event, mapper)));
  }

  // Legacy non-queued intake intentionally retains split raw/projection writes; effective run
  // identity resolution is scoped to durable queued processing.
  public CompletableFuture<Void> createAsync(LineageEvent event) {
    return submit(
        () -> {
          if (searchService != null) {
            searchService.indexEvent(event);
          }

          UUID runUuid = runUuidFromEvent(event.getRun());
          attemptBoth(
              () ->
                  createLineageEvent(
                      event.getEventType() == null ? "" : event.getEventType(),
                      event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
                      runUuid,
                      event.getJob().getName(),
                      event.getJob().getNamespace(),
                      createJsonArray(event, mapper),
                      event.getProducer()),
              () -> updateMarquezModelAndNotifyListeners(event));
        });
  }

  private void updateMarquezModelAndNotifyListeners(LineageEvent event) {
    boolean listenerSnapshotRequired =
        event.getEventType() != null && runService.hasRunTransitionListeners();
    UpdateLineageRow update = updateMarquezModel(event, mapper, listenerSnapshotRequired);
    if (listenerSnapshotRequired) {
      notifyListeners(event, update);
    }
  }

  private CompletableFuture<Void> submit(Runnable task) {
    try {
      return CompletableFuture.runAsync(withSentry(withMdc(task)), executor);
    } catch (RejectedExecutionException e) {
      return CompletableFuture.failedFuture(new IntakeOverloadedException(e));
    }
  }

  private void attemptBoth(Runnable rawEventWrite, Runnable relationalProjection) {
    RuntimeException failure = null;
    try {
      rawEventWrite.run();
    } catch (RuntimeException e) {
      failure = e;
    }

    try {
      relationalProjection.run();
    } catch (RuntimeException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }

    if (failure != null) {
      throw failure;
    }
  }

  private int notifyListeners(LineageEvent event, UpdateLineageRow update) {
    int failures = 0;
    boolean isStreaming =
        Optional.ofNullable(event.getJob()).map(j -> j.isStreamingJob()).orElse(false);
    if (update.getRunIoSnapshot() != null) {
      if (event.getEventType().equalsIgnoreCase("COMPLETE") || isStreaming) {
        failures += buildJobOutputUpdate(update).map(runService::notify).orElse(0);
      }
      failures += buildJobInputUpdate(update).map(runService::notify).orElse(0);
    } else {
      log.warn("No run I/O snapshot available for run {}", update.getRun().getUuid());
    }
    failures += buildRunTransition(update).map(runService::notify).orElse(0);
    return failures;
  }

  /**
   * Try to convert the run id to a UUID. If it isn't a properly formatted UUID, generate one from
   * the string bytes
   *
   * @param run
   * @return the {@link UUID} for the run
   */
  private UUID runUuidFromEvent(LineageEvent.Run run) {
    return Utils.openLineageRunUuid(run.getRunId());
  }

  private Optional<JobOutputUpdate> buildJobOutputUpdate(UpdateLineageRow record) {
    RunId runId = RunId.of(record.getRun().getUuid());
    return buildJobOutput(runId, buildJobVersionId(record), record);
  }

  private Optional<JobInputUpdate> buildJobInputUpdate(UpdateLineageRow record) {
    RunId runId = RunId.of(record.getRun().getUuid());
    return buildJobInput(
        record.getRun(),
        record.getRunArgs(),
        record.getJob(),
        buildJobVersionId(record),
        runId,
        record);
  }

  public JobVersionId buildJobVersionId(UpdateLineageRow record) {
    if (record.getJobVersionBag() != null) {
      return JobVersionId.builder()
          .version(record.getJobVersionBag().getJobVersionRow().getUuid())
          .namespace(NamespaceName.of(record.getNamespace().getName()))
          .name(JobName.of(record.getJob().getName()))
          .build();
    }
    return null;
  }

  Optional<JobOutputUpdate> buildJobOutput(
      RunId runId, JobVersionId jobVersionId, UpdateLineageRow record) {
    List<ExtendedDatasetVersionRow> datasets =
        record.getRunIoSnapshot() == null
            ? Collections.emptyList()
            : record.getRunIoSnapshot().getOutputs();

    // Do not trigger a JobOutput event if there are no new datasets
    if (datasets.isEmpty() && record.getOutputs().isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        new JobOutputUpdate(
            runId,
            jobVersionId,
            JobName.of(record.getJob().getName()),
            NamespaceName.of(record.getJob().getNamespaceName()),
            RunService.buildRunOutputs(datasets)));
  }

  Optional<JobInputUpdate> buildJobInput(
      RunRow run,
      RunArgsRow runArgsRow,
      JobRow jobRow,
      JobVersionId jobVersionId,
      RunId runId,
      UpdateLineageRow record) {
    List<ExtendedDatasetVersionRow> datasets =
        record.getRunIoSnapshot() == null
            ? Collections.emptyList()
            : record.getRunIoSnapshot().getInputs();
    // Do not trigger a JobInput event if there are no new datasets
    if (datasets.isEmpty() || record.getInputs().isEmpty()) {
      return Optional.empty();
    }

    Map<String, String> runArgs;
    try {
      runArgs = Utils.fromJson(runArgsRow.getArgs(), new TypeReference<Map<String, String>>() {});
    } catch (Exception e) {
      runArgs = new HashMap<>();
    }

    return Optional.of(
        new JobInputUpdate(
            runId,
            RunMeta.builder()
                .id(RunId.of(run.getUuid()))
                .nominalStartTime(run.getNominalStartTime().orElse(null))
                .nominalEndTime(run.getNominalEndTime().orElse(null))
                .args(runArgs)
                .build(),
            jobVersionId,
            JobName.of(jobRow.getName()),
            NamespaceName.of(jobRow.getNamespaceName()),
            RunService.buildRunInputs(datasets)));
  }

  private Optional<RunTransition> buildRunTransition(UpdateLineageRow record) {
    RunId runId = RunId.of(record.getRun().getUuid());
    RunStateRow runStateRow = record.getRunState();
    if (runStateRow == null) {
      return Optional.empty();
    }
    RunState newState = RunState.valueOf(runStateRow.getState());
    RunState oldState = newState.isStarting() ? null : RunState.RUNNING;
    return Optional.of(new RunTransition(runId, oldState, newState));
  }
}
