/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static marquez.logging.MdcPropagating.withMdc;
import static marquez.tracing.SentryPropagating.withSentry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
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
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import marquez.common.Utils;
import marquez.common.models.JobName;
import marquez.common.models.JobVersionId;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.BaseDao;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageEventDao;
import marquez.db.OpenLineageEventDao.OpenLineageEventWrite;
import marquez.db.OpenLineageProjector;
import marquez.db.OpenLineageProjector.DatasetProjectionResult;
import marquez.db.OpenLineageProjector.JobProjectionResult;
import marquez.db.OpenLineageProjector.ProjectionRequest;
import marquez.db.OpenLineageProjector.ProjectionResult;
import marquez.db.OpenLineageProjector.RunProjectionResult;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.JobRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunRow;
import marquez.db.models.RunStateRow;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import marquez.service.models.RunMeta;

@Slf4j
public class OpenLineageService {
  private static final Runnable NOOP = () -> {};

  private final OpenLineageDao projectionDao;
  private final OpenLineageEventDao eventDao;
  private final OpenLineageProjector projector;
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
    this(
        baseDao.createOpenLineageDao(),
        baseDao.createOpenLineageEventDao(),
        OpenLineageProjector.getInstance(),
        runService,
        searchService,
        executor);
  }

  public OpenLineageService(
      OpenLineageDao projectionDao,
      OpenLineageEventDao eventDao,
      OpenLineageProjector projector,
      RunService runService,
      SearchService searchService) {
    this(projectionDao, eventDao, projector, runService, searchService, ForkJoinPool.commonPool());
  }

  public OpenLineageService(
      OpenLineageDao projectionDao,
      OpenLineageEventDao eventDao,
      OpenLineageProjector projector,
      RunService runService,
      SearchService searchService,
      Executor executor) {
    this.projectionDao = Objects.requireNonNull(projectionDao, "projectionDao");
    this.eventDao = Objects.requireNonNull(eventDao, "eventDao");
    this.projector = Objects.requireNonNull(projector, "projector");
    this.runService = runService;
    this.searchService = searchService;
    this.executor = executor;
  }

  /** One parsed queued event paired with the exact JSON admitted to the durable queue. */
  record QueuedEvent(BaseEvent event, String eventJson) {
    QueuedEvent {
      Objects.requireNonNull(event, "event");
      if (eventJson == null || eventJson.isBlank()) {
        throw new IllegalArgumentException("eventJson is required");
      }
    }
  }

  /** A committed queue projection awaiting best-effort post-commit publication. */
  record CommittedEvent(long queueId, LineageEvent event, RunProjectionResult projection) {
    CommittedEvent {
      if (queueId <= 0) {
        throw new IllegalArgumentException("queueId must be positive");
      }
      Objects.requireNonNull(event, "event");
      Objects.requireNonNull(projection, "projection");
    }
  }

  /**
   * Projects and stores an ordered batch through transaction-attached DAOs.
   *
   * <p>The caller owns the transaction and queue acknowledgement. Projection uses one batch call,
   * raw storage uses one bulk write, and post-commit side effects are excluded.
   *
   * @return immutable projection results aligned one-to-one with {@code events}
   */
  List<ProjectionResult> processQueuedBatchInTransaction(
      List<QueuedEvent> events, BaseDao transactionalDaos) {
    Objects.requireNonNull(events, "events");
    Objects.requireNonNull(transactionalDaos, "transactionalDaos");
    if (events.isEmpty()) {
      throw new IllegalArgumentException("events must not be empty");
    }

    List<QueuedEvent> stableEvents = List.copyOf(events);
    List<ProjectionRequest> requests =
        stableEvents.stream()
            .map(event -> projectionRequest(event.event(), event.eventJson()))
            .toList();

    List<ProjectionResult> results = projectBatchInTransaction(transactionalDaos, requests);
    List<OpenLineageEventWrite> rawEvents = new ArrayList<>(results.size());
    for (int index = 0; index < results.size(); index++) {
      rawEvents.add(rawEventWrite(stableEvents.get(index), results.get(index)));
    }

    Objects.requireNonNull(
            transactionalDaos.createOpenLineageEventDao(), "transactional OpenLineage event DAO")
        .createLineageEvents(List.copyOf(rawEvents));
    return results;
  }

  private List<ProjectionResult> projectBatchInTransaction(
      BaseDao transactionalDaos, List<ProjectionRequest> requests) {
    List<ProjectionResult> projected =
        Objects.requireNonNull(
            projector.projectBatchInTransaction(transactionalDaos, mapper, requests),
            "projector returned null results");
    if (projected.size() != requests.size()) {
      throw new IllegalStateException(
          "Expected " + requests.size() + " projection results, but received " + projected.size());
    }

    List<ProjectionResult> results = List.copyOf(projected);
    for (int index = 0; index < results.size(); index++) {
      ProjectionResult result = Objects.requireNonNull(results.get(index), "projection result");
      if (!requests.get(index).equals(result.request())) {
        throw new IllegalStateException("Projection result order does not match request order");
      }
    }
    return results;
  }

  private static Instant eventInstant(BaseEvent event) {
    if (event instanceof DatasetEvent datasetEvent) {
      return datasetEvent.getEventTime().toInstant();
    }
    if (event instanceof JobEvent jobEvent) {
      return jobEvent.getEventTime().toInstant();
    }
    if (event instanceof LineageEvent lineageEvent) {
      return lineageEvent.getEventTime().toInstant();
    }
    throw new IllegalArgumentException(
        "Unsupported OpenLineage event type: " + event.getClass().getName());
  }

  /** Publishes non-transactional projections after the queue ACK has committed. */
  int publishQueuedEventBestEffort(LineageEvent event, RunProjectionResult projection) {
    int failures = 0;
    if (searchService != null) {
      try {
        if (!searchService.indexEvent(event, effectiveRunUuid(projection))) {
          failures++;
        }
      } catch (RuntimeException e) {
        failures++;
        log.error("Failed to index queued OpenLineage event", e);
      }
    }

    if (event.getEventType() != null && runService.hasRunTransitionListeners()) {
      try {
        failures += notifyListeners(event, projection);
      } catch (RuntimeException e) {
        failures++;
        log.error("Failed to notify listeners for queued OpenLineage event", e);
      }
    }
    return failures;
  }

  /** Publishes an ordered batch after its queue transaction has committed. */
  int publishQueuedEventsBestEffort(List<CommittedEvent> committedEvents) {
    Objects.requireNonNull(committedEvents, "committedEvents");
    int failures = 0;

    if (searchService != null) {
      List<SearchService.IndexEntry> indexEntries = new ArrayList<>(committedEvents.size());
      for (CommittedEvent committedEvent : committedEvents) {
        try {
          indexEntries.add(
              new SearchService.IndexEntry(
                  committedEvent.event(), effectiveRunUuid(committedEvent.projection())));
        } catch (RuntimeException e) {
          failures++;
          log.error(
              "Failed to prepare queued OpenLineage event {} for indexing",
              committedEvent.queueId(),
              e);
        }
      }
      if (!indexEntries.isEmpty()) {
        try {
          failures += searchService.indexEventsBestEffort(indexEntries);
        } catch (RuntimeException e) {
          failures += indexEntries.size();
          log.error(
              "Failed to bulk index {} committed queued OpenLineage event(s)",
              indexEntries.size(),
              e);
        }
      }
    }

    for (CommittedEvent committedEvent : committedEvents) {
      if (committedEvent.event().getEventType() == null) {
        continue;
      }
      try {
        if (runService.hasRunTransitionListeners()) {
          failures += notifyListeners(committedEvent.event(), committedEvent.projection());
        }
      } catch (RuntimeException e) {
        failures++;
        log.error(
            "Failed to notify listeners for queued OpenLineage event {}",
            committedEvent.queueId(),
            e);
      }
    }
    return failures;
  }

  private static UUID effectiveRunUuid(RunProjectionResult projection) {
    RunRow run =
        Objects.requireNonNull(
            Objects.requireNonNull(projection, "lineage projection").run(),
            "lineage projection returned no run");
    return Objects.requireNonNull(run.getUuid(), "queued lineage projection returned no run UUID");
  }

  public CompletableFuture<Void> createAsync(DatasetEvent event) {
    return createAsync(event, NOOP);
  }

  public CompletableFuture<Void> createAsync(JobEvent event) {
    return createAsync(event, NOOP);
  }

  // Legacy non-queued intake intentionally retains split raw/projection writes; effective run
  // identity resolution is scoped to durable queued processing.
  public CompletableFuture<Void> createAsync(LineageEvent event) {
    return createAsync(
        event,
        () -> {
          if (searchService != null) {
            searchService.indexEvent(event);
          }
        });
  }

  private CompletableFuture<Void> createAsync(BaseEvent event, Runnable beforeWrites) {
    return submit(
        () -> {
          beforeWrites.run();
          String eventJson = serializeEvent(event);
          UUID runUuid =
              event instanceof LineageEvent lineage
                  ? Utils.openLineageRunUuid(lineage.getRun().getRunId())
                  : null;
          attemptBoth(
              () -> writeRawEvent(event, eventJson, eventDao, runUuid),
              () -> projectEvent(event, eventJson));
        });
  }

  private ProjectionResult projectEvent(BaseEvent event, String eventJson) {
    ProjectionRequest request = projectionRequest(event, eventJson);
    ProjectionResult result =
        Objects.requireNonNull(
            projectionDao.inTransaction(
                transactional -> projector.projectInTransaction(transactional, mapper, request)),
            "projector returned null result");
    if (!request.equals(result.request())) {
      throw new IllegalStateException("Projection result does not match its request");
    }
    if (request.listenerSnapshotRequired() && result instanceof RunProjectionResult runProjection) {
      notifyListeners((LineageEvent) event, runProjection);
    }
    return result;
  }

  private ProjectionRequest projectionRequest(BaseEvent event, String eventJson) {
    return new ProjectionRequest(event, eventJson, listenerSnapshotRequired(event));
  }

  private boolean listenerSnapshotRequired(BaseEvent event) {
    return event instanceof LineageEvent lineageEvent
        && lineageEvent.getEventType() != null
        && runService.hasRunTransitionListeners();
  }

  private String serializeEvent(BaseEvent event) {
    try {
      return mapper.writeValueAsString(event);
    } catch (Exception e) {
      throw new RuntimeException("Could not write lineage event to db", e);
    }
  }

  private static void writeRawEvent(
      BaseEvent event, String eventJson, OpenLineageEventDao dao, @Nullable UUID lineageRunUuid) {
    if (event instanceof DatasetEvent datasetEvent) {
      dao.createDatasetEvent(eventInstant(event), eventJson, datasetEvent.getProducer());
      return;
    }
    if (event instanceof JobEvent jobEvent) {
      dao.createJobEvent(
          eventInstant(event),
          jobEvent.getJob().getName(),
          jobEvent.getJob().getNamespace(),
          eventJson,
          jobEvent.getProducer());
      return;
    }
    if (event instanceof LineageEvent lineageEvent) {
      dao.createLineageEvent(
          lineageEvent.getEventType() == null ? "" : lineageEvent.getEventType(),
          eventInstant(event),
          Objects.requireNonNull(lineageRunUuid, "lineageRunUuid"),
          lineageEvent.getJob().getName(),
          lineageEvent.getJob().getNamespace(),
          eventJson,
          lineageEvent.getProducer());
      return;
    }
    throw new IllegalArgumentException(
        "Unsupported OpenLineage event type: " + event.getClass().getName());
  }

  private static OpenLineageEventWrite rawEventWrite(
      QueuedEvent queuedEvent, ProjectionResult result) {
    BaseEvent event = queuedEvent.event();
    if (event instanceof LineageEvent lineageEvent) {
      if (!(result instanceof RunProjectionResult runProjection)) {
        throw unexpectedProjectionResult(event, result);
      }
      return OpenLineageEventWrite.run(
          lineageEvent.getEventType(),
          eventInstant(event),
          effectiveRunUuid(runProjection),
          lineageEvent.getJob().getName(),
          lineageEvent.getJob().getNamespace(),
          queuedEvent.eventJson(),
          lineageEvent.getProducer());
    }
    if (event instanceof JobEvent jobEvent) {
      if (!(result instanceof JobProjectionResult)) {
        throw unexpectedProjectionResult(event, result);
      }
      return OpenLineageEventWrite.job(
          eventInstant(event),
          jobEvent.getJob().getName(),
          jobEvent.getJob().getNamespace(),
          queuedEvent.eventJson(),
          jobEvent.getProducer());
    }
    if (event instanceof DatasetEvent datasetEvent) {
      if (!(result instanceof DatasetProjectionResult)) {
        throw unexpectedProjectionResult(event, result);
      }
      return OpenLineageEventWrite.dataset(
          eventInstant(event), queuedEvent.eventJson(), datasetEvent.getProducer());
    }
    throw new IllegalArgumentException(
        "Unsupported OpenLineage event type: " + event.getClass().getName());
  }

  private static IllegalStateException unexpectedProjectionResult(
      BaseEvent event, ProjectionResult result) {
    return new IllegalStateException(
        "Projection result %s does not match event %s"
            .formatted(result.getClass().getName(), event.getClass().getName()));
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

  private int notifyListeners(LineageEvent event, RunProjectionResult projection) {
    int failures = 0;
    boolean isStreaming =
        Optional.ofNullable(event.getJob()).map(j -> j.isStreamingJob()).orElse(false);
    if (projection.runIoSnapshot() != null) {
      if (event.getEventType().equalsIgnoreCase("COMPLETE") || isStreaming) {
        failures += buildJobOutputUpdate(projection).map(runService::notify).orElse(0);
      }
      failures += buildJobInputUpdate(projection).map(runService::notify).orElse(0);
    } else {
      log.warn("No run I/O snapshot available for run {}", projection.run().getUuid());
    }
    failures += buildRunTransition(projection).map(runService::notify).orElse(0);
    return failures;
  }

  private Optional<JobOutputUpdate> buildJobOutputUpdate(RunProjectionResult projection) {
    RunId runId = RunId.of(projection.run().getUuid());
    return buildJobOutput(runId, buildJobVersionId(projection), projection);
  }

  private Optional<JobInputUpdate> buildJobInputUpdate(RunProjectionResult projection) {
    RunId runId = RunId.of(projection.run().getUuid());
    return buildJobInput(
        projection.run(),
        projection.runArgs(),
        projection.job(),
        buildJobVersionId(projection),
        runId,
        projection);
  }

  public JobVersionId buildJobVersionId(RunProjectionResult projection) {
    if (projection.jobVersion() != null) {
      return JobVersionId.builder()
          .version(projection.jobVersion().jobVersion().getUuid())
          .namespace(NamespaceName.of(projection.namespace().getName()))
          .name(JobName.of(projection.job().getName()))
          .build();
    }
    return null;
  }

  Optional<JobOutputUpdate> buildJobOutput(
      RunId runId, JobVersionId jobVersionId, RunProjectionResult projection) {
    List<ExtendedDatasetVersionRow> datasets =
        projection.runIoSnapshot() == null ? List.of() : projection.runIoSnapshot().getOutputs();

    // Do not trigger a JobOutput event if there are no new datasets
    if (datasets.isEmpty() && projection.outputs().isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        new JobOutputUpdate(
            runId,
            jobVersionId,
            JobName.of(projection.job().getName()),
            NamespaceName.of(projection.job().getNamespaceName()),
            RunService.buildRunOutputs(datasets)));
  }

  Optional<JobInputUpdate> buildJobInput(
      RunRow run,
      RunArgsRow runArgsRow,
      JobRow jobRow,
      JobVersionId jobVersionId,
      RunId runId,
      RunProjectionResult projection) {
    List<ExtendedDatasetVersionRow> datasets =
        projection.runIoSnapshot() == null ? List.of() : projection.runIoSnapshot().getInputs();
    // Do not trigger a JobInput event if there are no new datasets
    if (datasets.isEmpty() || projection.inputs().isEmpty()) {
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

  private Optional<RunTransition> buildRunTransition(RunProjectionResult projection) {
    RunId runId = RunId.of(projection.run().getUuid());
    RunStateRow runStateRow = projection.runState();
    if (runStateRow == null) {
      return Optional.empty();
    }
    RunState newState = RunState.valueOf(runStateRow.getState());
    RunState oldState = newState.isStarting() ? null : RunState.RUNNING;
    return Optional.of(new RunTransition(runId, oldState, newState));
  }
}
