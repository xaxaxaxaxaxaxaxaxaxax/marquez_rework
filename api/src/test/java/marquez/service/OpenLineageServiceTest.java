/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import marquez.common.Utils;
import marquez.db.BaseDao;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageEventDao;
import marquez.db.OpenLineageEventDao.OpenLineageEventWrite;
import marquez.db.OpenLineageProjector;
import marquez.db.OpenLineageProjector.DatasetProjection;
import marquez.db.OpenLineageProjector.DatasetProjectionResult;
import marquez.db.OpenLineageProjector.JobProjectionResult;
import marquez.db.OpenLineageProjector.JobVersionProjection;
import marquez.db.OpenLineageProjector.ProjectionRequest;
import marquez.db.OpenLineageProjector.ProjectionResult;
import marquez.db.OpenLineageProjector.RunProjectionResult;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.JobRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunIoSnapshot;
import marquez.db.models.RunRow;
import marquez.db.models.RunStateRow;
import marquez.service.OpenLineageService.CommittedEvent;
import marquez.service.OpenLineageService.QueuedEvent;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.sqlobject.transaction.TransactionalCallback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class OpenLineageServiceTest {
  private static final UUID RUN_ID = UUID.fromString("de2d8a76-57b3-42f6-8d26-06d6179ac45c");
  private static final UUID EFFECTIVE_RUN_ID =
      UUID.fromString("ec0f5598-20ab-4d60-ab4d-6fc280748251");

  private OpenLineageDao projectionDao;
  private OpenLineageEventDao eventDao;
  private OpenLineageProjector projector;
  private RunService runService;
  private SearchService searchService;

  @BeforeEach
  @SuppressWarnings({"rawtypes", "unchecked"})
  void setUp() throws Exception {
    projectionDao = mock(OpenLineageDao.class);
    eventDao = mock(OpenLineageEventDao.class);
    projector = mock(OpenLineageProjector.class);
    runService = mock(RunService.class);
    searchService = mock(SearchService.class);
    when(projectionDao.inTransaction(any(TransactionalCallback.class)))
        .thenAnswer(
            invocation -> {
              TransactionalCallback callback = invocation.getArgument(0);
              return callback.inTransaction(projectionDao);
            });
    when(projector.projectInTransaction(
            eq(projectionDao), any(ObjectMapper.class), any(ProjectionRequest.class)))
        .thenAnswer(invocation -> projection(invocation.getArgument(2)));
  }

  @Test
  void baseDaoConstructorUsesSeparateProjectionAndRawEventDaos() {
    BaseDao baseDao = mock(BaseDao.class);
    when(baseDao.createOpenLineageDao()).thenReturn(projectionDao);
    when(baseDao.createOpenLineageEventDao()).thenReturn(eventDao);

    new OpenLineageService(baseDao, runService, searchService, Runnable::run);

    verify(baseDao).createOpenLineageDao();
    verify(baseDao).createOpenLineageEventDao();
  }

  @Test
  void admitsOneTaskAndIndexesBeforeRawWriteAndProjection() throws Exception {
    AtomicInteger submittedTasks = new AtomicInteger();
    Executor directExecutor =
        task -> {
          submittedTasks.incrementAndGet();
          task.run();
        };
    OpenLineageService service = service(directExecutor);
    LineageEvent event = lineageEvent();
    ArgumentCaptor<String> eventJson = ArgumentCaptor.forClass(String.class);

    service.createAsync(event).join();

    assertThat(submittedTasks).hasValue(1);
    InOrder order = inOrder(searchService, eventDao, projectionDao, projector);
    order.verify(searchService).indexEvent(event);
    order
        .verify(eventDao)
        .createLineageEvent(
            eq(event.getEventType()),
            eq(event.getEventTime().toInstant()),
            eq(RUN_ID),
            eq(event.getJob().getName()),
            eq(event.getJob().getNamespace()),
            eventJson.capture(),
            eq(event.getProducer()));
    order.verify(projectionDao).inTransaction(any(TransactionalCallback.class));
    order
        .verify(projector)
        .projectInTransaction(
            eq(projectionDao), any(ObjectMapper.class), any(ProjectionRequest.class));
    assertThat(Utils.getMapper().readTree(eventJson.getValue()))
        .isEqualTo(Utils.getMapper().readTree(Utils.toJson(event)));
    verify(searchService, never()).indexEvent(eq(event), any(UUID.class));
  }

  @Test
  void attemptsProjectionWhenRawEventWriteFails() {
    RuntimeException rawFailure = new RuntimeException("raw");
    doThrow(rawFailure)
        .when(eventDao)
        .createLineageEvent(any(), any(), any(), any(), any(), any(), any());
    OpenLineageService service = service(Runnable::run);

    CompletionException thrown =
        assertThrows(CompletionException.class, () -> service.createAsync(lineageEvent()).join());

    assertThat(thrown.getCause()).isSameAs(rawFailure);
    verify(projector)
        .projectInTransaction(
            eq(projectionDao), any(ObjectMapper.class), any(ProjectionRequest.class));
  }

  @Test
  void preservesFirstFailureAndSuppressesProjectionFailure() {
    IllegalStateException rawFailure = new IllegalStateException("raw");
    IllegalArgumentException projectionFailure = new IllegalArgumentException("projection");
    doThrow(rawFailure)
        .when(eventDao)
        .createLineageEvent(any(), any(), any(), any(), any(), any(), any());
    when(projector.projectInTransaction(
            eq(projectionDao), any(ObjectMapper.class), any(ProjectionRequest.class)))
        .thenThrow(projectionFailure);
    OpenLineageService service = service(Runnable::run);

    CompletionException thrown =
        assertThrows(CompletionException.class, () -> service.createAsync(lineageEvent()).join());

    assertThat(thrown.getCause()).isSameAs(rawFailure);
    assertThat(rawFailure.getSuppressed()).containsExactly(projectionFailure);
  }

  @Test
  void rejectionHasNoSearchOrDatabaseSideEffects() {
    Executor rejectingExecutor =
        task -> {
          throw new RejectedExecutionException("full");
        };
    OpenLineageService service = service(rejectingExecutor);

    CompletionException thrown =
        assertThrows(CompletionException.class, () -> service.createAsync(lineageEvent()).join());

    assertThat(thrown.getCause()).isInstanceOf(IntakeOverloadedException.class);
    verifyNoInteractions(searchService, eventDao, projectionDao, projector);
  }

  @Test
  void searchItemFailureAbortsBeforeDatabaseWrites() {
    doThrow(new IllegalStateException("bulk item failed"))
        .when(searchService)
        .indexEvent(any(LineageEvent.class));
    OpenLineageService service = service(Runnable::run);

    assertThrows(CompletionException.class, () -> service.createAsync(lineageEvent()).join());

    verifyNoInteractions(eventDao, projectionDao, projector);
  }

  @Test
  void queuedBatchProjectsOnceAndBulkWritesExactJsonWithEffectiveRunUuid() {
    BaseDao transactionalDaos = mock(BaseDao.class);
    OpenLineageEventDao transactionalEvents = mock(OpenLineageEventDao.class);
    when(transactionalDaos.createOpenLineageEventDao()).thenReturn(transactionalEvents);
    LineageEvent run = lineageEvent();
    DatasetEvent dataset = datasetEvent();
    JobEvent job = jobEvent();
    String runJson = "{ \"kind\" : \"run\", \"number\" : 1.00 }";
    String datasetJson = "{\n  \"kind\" : \"dataset\", \"number\" : 2.00\n}";
    String jobJson = "{ \"kind\" : \"job\", \"number\" : 3.00 }";
    List<QueuedEvent> events =
        List.of(
            new QueuedEvent(run, runJson),
            new QueuedEvent(dataset, datasetJson),
            new QueuedEvent(job, jobJson));
    when(projector.projectBatchInTransaction(eq(transactionalDaos), any(ObjectMapper.class), any()))
        .thenAnswer(
            invocation -> {
              List<ProjectionRequest> requests = invocation.getArgument(2);
              return List.of(
                  runProjection(requests.get(0), EFFECTIVE_RUN_ID),
                  datasetProjection(requests.get(1)),
                  jobProjection(requests.get(2)));
            });
    OpenLineageService service = service(Runnable::run);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ProjectionRequest>> requests = ArgumentCaptor.forClass(List.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<OpenLineageEventWrite>> rawEvents = ArgumentCaptor.forClass(List.class);

    List<ProjectionResult> results =
        service.processQueuedBatchInTransaction(events, transactionalDaos);

    verify(projector)
        .projectBatchInTransaction(
            eq(transactionalDaos), any(ObjectMapper.class), requests.capture());
    verify(transactionalEvents).createLineageEvents(rawEvents.capture());
    assertThat(results).hasSize(3);
    assertThat(results)
        .extracting(result -> result.request().event())
        .containsExactly(run, dataset, job);
    assertThrows(UnsupportedOperationException.class, () -> results.add(results.get(0)));
    assertThat(requests.getValue())
        .extracting(ProjectionRequest::exactEventJson)
        .containsExactly(runJson, datasetJson, jobJson);
    assertThat(requests.getValue())
        .extracting(ProjectionRequest::listenerSnapshotRequired)
        .containsOnly(false);
    assertThat(requests.getValue().get(0).order().getEventTime())
        .isEqualTo(run.getEventTime().toInstant());
    assertThat(requests.getValue().get(0).order().getEventKey())
        .isEqualTo(Utils.sha256Utf8(runJson));
    assertThat(rawEvents.getValue())
        .extracting(OpenLineageEventWrite::eventJson)
        .containsExactly(runJson, datasetJson, jobJson);
    assertThat(rawEvents.getValue().get(0).runUuid())
        .isEqualTo(EFFECTIVE_RUN_ID)
        .isNotEqualTo(RUN_ID);
    assertThat(rawEvents.getValue().get(1).runUuid()).isNull();
    assertThat(rawEvents.getValue().get(2).runUuid()).isNull();
    verifyNoInteractions(searchService);
    verify(runService, never()).notify(any(JobInputUpdate.class));
    verify(runService, never()).notify(any(JobOutputUpdate.class));
    verify(runService, never()).notify(any(RunTransition.class));
  }

  @Test
  void queuedBatchRawFailurePropagatesAfterOneProjectionCall() {
    BaseDao transactionalDaos = mock(BaseDao.class);
    OpenLineageEventDao transactionalEvents = mock(OpenLineageEventDao.class);
    when(transactionalDaos.createOpenLineageEventDao()).thenReturn(transactionalEvents);
    LineageEvent event = lineageEvent();
    QueuedEvent queued = new QueuedEvent(event, "{\"kind\":\"run\"}");
    when(projector.projectBatchInTransaction(eq(transactionalDaos), any(ObjectMapper.class), any()))
        .thenAnswer(
            invocation -> {
              List<ProjectionRequest> requests = invocation.getArgument(2);
              return List.of(runProjection(requests.get(0), EFFECTIVE_RUN_ID));
            });
    RuntimeException rawFailure = new RuntimeException("raw write failed");
    when(transactionalEvents.createLineageEvents(any())).thenThrow(rawFailure);
    OpenLineageService service = service(Runnable::run);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> service.processQueuedBatchInTransaction(List.of(queued), transactionalDaos));

    assertThat(thrown).isSameAs(rawFailure);
    InOrder order = inOrder(projector, transactionalEvents);
    order
        .verify(projector)
        .projectBatchInTransaction(eq(transactionalDaos), any(ObjectMapper.class), any());
    order.verify(transactionalEvents).createLineageEvents(any());
  }

  @Test
  void queuedBatchRejectsInvalidProjectionResultsBeforeRawWrite() {
    BaseDao transactionalDaos = mock(BaseDao.class);
    OpenLineageEventDao transactionalEvents = mock(OpenLineageEventDao.class);
    when(transactionalDaos.createOpenLineageEventDao()).thenReturn(transactionalEvents);
    QueuedEvent first = new QueuedEvent(lineageEvent("START"), "{\"ordinal\":1}");
    QueuedEvent second = new QueuedEvent(lineageEvent("COMPLETE"), "{\"ordinal\":2}");
    when(projector.projectBatchInTransaction(eq(transactionalDaos), any(ObjectMapper.class), any()))
        .thenReturn(List.of());
    OpenLineageService service = service(Runnable::run);

    assertThrows(
        IllegalStateException.class,
        () -> service.processQueuedBatchInTransaction(List.of(first, second), transactionalDaos));
    verifyNoInteractions(transactionalEvents);

    when(projector.projectBatchInTransaction(eq(transactionalDaos), any(ObjectMapper.class), any()))
        .thenAnswer(
            invocation -> {
              List<ProjectionRequest> requests = invocation.getArgument(2);
              return List.of(
                  runProjection(requests.get(1), EFFECTIVE_RUN_ID),
                  runProjection(requests.get(0), RUN_ID));
            });

    assertThrows(
        IllegalStateException.class,
        () -> service.processQueuedBatchInTransaction(List.of(first, second), transactionalDaos));
    verifyNoInteractions(transactionalEvents);

    when(projector.projectBatchInTransaction(eq(transactionalDaos), any(ObjectMapper.class), any()))
        .thenAnswer(
            invocation -> {
              List<ProjectionRequest> requests = invocation.getArgument(2);
              return List.of(datasetProjection(requests.get(0)));
            });

    assertThrows(
        IllegalStateException.class,
        () -> service.processQueuedBatchInTransaction(List.of(first), transactionalDaos));
    verifyNoInteractions(transactionalEvents);
  }

  @Test
  void queuedEventRejectsMalformedExactJsonBoundaryValues() {
    assertThrows(NullPointerException.class, () -> new QueuedEvent(null, "{}"));
    assertThrows(IllegalArgumentException.class, () -> new QueuedEvent(lineageEvent(), null));
    assertThrows(IllegalArgumentException.class, () -> new QueuedEvent(lineageEvent(), "  "));
  }

  @Test
  void listenerSnapshotDecisionIsCapturedPerLegacyEvent() {
    when(runService.hasRunTransitionListeners()).thenReturn(true);
    LineageEvent event = lineageEvent("START");
    RunProjectionResult projected = listenerProjection(event, RUN_ID, "RUNNING");
    when(projector.projectInTransaction(
            eq(projectionDao), any(ObjectMapper.class), any(ProjectionRequest.class)))
        .thenReturn(projected);
    OpenLineageService service = service(Runnable::run);
    ArgumentCaptor<ProjectionRequest> request = ArgumentCaptor.forClass(ProjectionRequest.class);

    service.createAsync(event).join();

    verify(projector)
        .projectInTransaction(eq(projectionDao), any(ObjectMapper.class), request.capture());
    assertThat(request.getValue().listenerSnapshotRequired()).isTrue();
    verify(runService).notify(any(RunTransition.class));
  }

  @Test
  void nullEventTypeDoesNotRequestListenerSnapshot() {
    OpenLineageService service = service(Runnable::run);
    LineageEvent event = lineageEvent(null);
    ArgumentCaptor<ProjectionRequest> request = ArgumentCaptor.forClass(ProjectionRequest.class);

    service.createAsync(event).join();

    verify(runService, never()).hasRunTransitionListeners();
    verify(projector)
        .projectInTransaction(eq(projectionDao), any(ObjectMapper.class), request.capture());
    assertThat(request.getValue().listenerSnapshotRequired()).isFalse();
  }

  @Test
  void queuedPostCommitSearchUsesEffectiveRunUuid() {
    LineageEvent event = lineageEvent();
    RunProjectionResult projection = runProjection(event, EFFECTIVE_RUN_ID);
    when(searchService.indexEvent(event, EFFECTIVE_RUN_ID)).thenReturn(false);
    OpenLineageService service = service(Runnable::run);

    int failures = service.publishQueuedEventBestEffort(event, projection);

    assertThat(failures).isEqualTo(1);
    verify(searchService).indexEvent(event, EFFECTIVE_RUN_ID);
    verify(searchService, never()).indexEvent(event);
  }

  @Test
  void queuedPostCommitIsolatesSearchExceptionAndSumsListenerFailures() {
    LineageEvent event = lineageEvent("COMPLETE");
    RunProjectionResult projection = listenerProjection(event, RUN_ID, "COMPLETED");
    when(runService.hasRunTransitionListeners()).thenReturn(true);
    doThrow(new IllegalStateException("search failed"))
        .when(searchService)
        .indexEvent(event, RUN_ID);
    when(runService.notify(any(JobOutputUpdate.class))).thenReturn(2);
    when(runService.notify(any(JobInputUpdate.class))).thenReturn(3);
    when(runService.notify(any(RunTransition.class))).thenReturn(4);
    OpenLineageService service = service(Runnable::run);

    int failures = service.publishQueuedEventBestEffort(event, projection);

    assertThat(failures).isEqualTo(10);
    verify(runService).notify(any(JobOutputUpdate.class));
    verify(runService).notify(any(JobInputUpdate.class));
    verify(runService).notify(any(RunTransition.class));
  }

  @Test
  void committedEventRequiresImmutableRunProjection() {
    LineageEvent event = lineageEvent();
    RunProjectionResult projection = runProjection(event, EFFECTIVE_RUN_ID);

    assertThrows(NullPointerException.class, () -> new CommittedEvent(61, null, projection));
    assertThrows(NullPointerException.class, () -> new CommittedEvent(61, event, null));
    assertThrows(IllegalArgumentException.class, () -> new CommittedEvent(0, event, projection));
  }

  @Test
  @SuppressWarnings("unchecked")
  void queuedBatchPostCommitIsolatesSearchPreparationFailurePerEvent() {
    LineageEvent firstEvent = lineageEvent("START");
    RunProjectionResult failingProjection = runProjection(firstEvent, EFFECTIVE_RUN_ID);
    when(failingProjection.run().getUuid())
        .thenThrow(new RuntimeException("missing effective run"));
    LineageEvent followingEvent = lineageEvent("COMPLETE");
    RunProjectionResult followingProjection = runProjection(followingEvent, EFFECTIVE_RUN_ID);
    when(searchService.indexEventsBestEffort(any())).thenReturn(3);
    OpenLineageService service = service(Runnable::run);
    ArgumentCaptor<List<SearchService.IndexEntry>> entries = ArgumentCaptor.forClass(List.class);

    int failures =
        service.publishQueuedEventsBestEffort(
            List.of(
                new CommittedEvent(64, firstEvent, failingProjection),
                new CommittedEvent(65, followingEvent, followingProjection)));

    assertThat(failures).isEqualTo(4);
    verify(searchService).indexEventsBestEffort(entries.capture());
    assertThat(entries.getValue())
        .extracting(SearchService.IndexEntry::event)
        .containsExactly(followingEvent);
  }

  @Test
  @SuppressWarnings("unchecked")
  void queuedBatchPostCommitPreservesEventMajorListenerOrderAndState() {
    UUID firstRunId = UUID.fromString("c436718f-997d-42ed-9030-e2033583d3f8");
    UUID secondRunId = UUID.fromString("753d98cf-9e04-4bc6-989b-757ebbf88c3c");
    LineageEvent firstEvent = lineageEvent("COMPLETE");
    LineageEvent secondEvent = lineageEvent("COMPLETE");
    RunProjectionResult firstProjection = listenerProjection(firstEvent, firstRunId, "COMPLETED");
    RunProjectionResult secondProjection =
        listenerProjection(secondEvent, secondRunId, "COMPLETED");
    when(runService.hasRunTransitionListeners()).thenReturn(true);
    when(searchService.indexEventsBestEffort(any())).thenReturn(2);
    when(runService.notify(any(JobOutputUpdate.class))).thenReturn(1);
    when(runService.notify(any(JobInputUpdate.class))).thenReturn(2);
    when(runService.notify(any(RunTransition.class))).thenReturn(3);
    OpenLineageService service = service(Runnable::run);
    ArgumentCaptor<List<SearchService.IndexEntry>> entries = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<JobOutputUpdate> outputs = ArgumentCaptor.forClass(JobOutputUpdate.class);
    ArgumentCaptor<JobInputUpdate> inputs = ArgumentCaptor.forClass(JobInputUpdate.class);
    ArgumentCaptor<RunTransition> transitions = ArgumentCaptor.forClass(RunTransition.class);

    int failures =
        service.publishQueuedEventsBestEffort(
            List.of(
                new CommittedEvent(71, firstEvent, firstProjection),
                new CommittedEvent(72, secondEvent, secondProjection)));

    assertThat(failures).isEqualTo(14);
    InOrder order = inOrder(searchService, runService);
    order.verify(searchService).indexEventsBestEffort(entries.capture());
    order.verify(runService).notify(outputs.capture());
    order.verify(runService).notify(inputs.capture());
    order.verify(runService).notify(transitions.capture());
    order.verify(runService).notify(outputs.capture());
    order.verify(runService).notify(inputs.capture());
    order.verify(runService).notify(transitions.capture());
    assertThat(entries.getValue())
        .extracting(SearchService.IndexEntry::event)
        .containsExactly(firstEvent, secondEvent);
    assertThat(entries.getValue())
        .extracting(SearchService.IndexEntry::effectiveRunUuid)
        .containsExactly(firstRunId, secondRunId);
    assertThat(outputs.getAllValues())
        .extracting(output -> output.getRunId().getValue())
        .containsExactly(firstRunId, secondRunId);
    assertThat(inputs.getAllValues())
        .extracting(input -> input.getRunId().getValue())
        .containsExactly(firstRunId, secondRunId);
    assertThat(transitions.getAllValues())
        .extracting(transition -> transition.getRunId().getValue())
        .containsExactly(firstRunId, secondRunId);
    verify(searchService, never()).indexEvent(any(LineageEvent.class), any(UUID.class));
  }

  @Test
  void queuedBatchPostCommitIsolatesBulkAndPerEventListenerRuntimeFailures() {
    LineageEvent failingEvent = lineageEvent("COMPLETE");
    RunIoSnapshot failingSnapshot = mock(RunIoSnapshot.class);
    when(failingSnapshot.getOutputs()).thenThrow(new RuntimeException("listener build failed"));
    RunProjectionResult failingProjection =
        runProjection(
            new ProjectionRequest(failingEvent, Utils.toJson(failingEvent), true),
            EFFECTIVE_RUN_ID,
            "COMPLETED",
            failingSnapshot,
            Optional.of(List.of(datasetProjection())),
            Optional.of(List.of(datasetProjection())));
    LineageEvent followingEvent = lineageEvent("COMPLETE");
    RunProjectionResult followingProjection =
        listenerProjection(
            followingEvent, UUID.fromString("753d98cf-9e04-4bc6-989b-757ebbf88c3c"), "COMPLETED");
    when(runService.hasRunTransitionListeners()).thenReturn(true);
    when(searchService.indexEventsBestEffort(any()))
        .thenThrow(new IllegalStateException("bulk search failed"));
    OpenLineageService service = service(Runnable::run);

    int failures =
        service.publishQueuedEventsBestEffort(
            List.of(
                new CommittedEvent(81, failingEvent, failingProjection),
                new CommittedEvent(82, followingEvent, followingProjection)));

    assertThat(failures).isEqualTo(3);
    verify(searchService).indexEventsBestEffort(any());
    verify(runService).notify(any(JobOutputUpdate.class));
    verify(runService).notify(any(JobInputUpdate.class));
    verify(runService).notify(any(RunTransition.class));
  }

  @Test
  void queuedBatchPostCommitDoesNotCatchBulkError() {
    AssertionError fatal = new AssertionError("fatal");
    LineageEvent event = lineageEvent();
    RunProjectionResult projection = runProjection(event, EFFECTIVE_RUN_ID);
    when(searchService.indexEventsBestEffort(any())).thenThrow(fatal);
    OpenLineageService service = service(Runnable::run);

    AssertionError thrown =
        assertThrows(
            AssertionError.class,
            () ->
                service.publishQueuedEventsBestEffort(
                    List.of(new CommittedEvent(91, event, projection))));

    assertThat(thrown).isSameAs(fatal);
    verify(searchService).indexEventsBestEffort(any());
  }

  private OpenLineageService service(Executor executor) {
    return new OpenLineageService(
        projectionDao, eventDao, projector, runService, searchService, executor);
  }

  private static ProjectionResult projection(ProjectionRequest request) {
    if (request.event() instanceof LineageEvent) {
      return runProjection(request, RUN_ID);
    }
    if (request.event() instanceof DatasetEvent) {
      return datasetProjection(request);
    }
    if (request.event() instanceof JobEvent) {
      return jobProjection(request);
    }
    throw new IllegalArgumentException("unsupported test event");
  }

  private static RunProjectionResult runProjection(LineageEvent event, UUID runId) {
    return runProjection(new ProjectionRequest(event, Utils.toJson(event), false), runId);
  }

  private static RunProjectionResult runProjection(ProjectionRequest request, UUID runId) {
    return runProjection(request, runId, null, null, Optional.empty(), Optional.empty());
  }

  private static RunProjectionResult listenerProjection(
      LineageEvent event, UUID runId, String runState) {
    ExtendedDatasetVersionRow datasetVersion = mock(ExtendedDatasetVersionRow.class);
    when(datasetVersion.getUuid()).thenReturn(UUID.randomUUID());
    when(datasetVersion.getNamespaceName()).thenReturn("dataset-namespace");
    when(datasetVersion.getDatasetName()).thenReturn("dataset");
    return runProjection(
        new ProjectionRequest(event, Utils.toJson(event), true),
        runId,
        runState,
        new RunIoSnapshot(List.of(datasetVersion), List.of(datasetVersion)),
        Optional.of(List.of(datasetProjection())),
        Optional.of(List.of(datasetProjection())));
  }

  private static RunProjectionResult runProjection(
      ProjectionRequest request,
      UUID runId,
      String runState,
      RunIoSnapshot snapshot,
      Optional<List<DatasetProjection>> inputs,
      Optional<List<DatasetProjection>> outputs) {
    NamespaceRow namespace = mock(NamespaceRow.class);
    when(namespace.getName()).thenReturn("job-namespace");
    JobRow job = mock(JobRow.class);
    when(job.getName()).thenReturn("job");
    when(job.getNamespaceName()).thenReturn("job-namespace");
    RunArgsRow runArgs = mock(RunArgsRow.class);
    when(runArgs.getArgs()).thenReturn("{}");
    RunRow run = mock(RunRow.class);
    when(run.getUuid()).thenReturn(runId);
    when(run.getNominalStartTime()).thenReturn(Optional.empty());
    when(run.getNominalEndTime()).thenReturn(Optional.empty());
    RunStateRow state = null;
    if (runState != null) {
      state = mock(RunStateRow.class);
      when(state.getState()).thenReturn(runState);
    }
    return new RunProjectionResult(
        request, namespace, job, runArgs, run, state, inputs, outputs, snapshot, null);
  }

  private static JobProjectionResult jobProjection(ProjectionRequest request) {
    JobRow job = mock(JobRow.class);
    JobVersionProjection version =
        new JobVersionProjection(job, mock(JobVersionRow.class), List.of(), List.of());
    return new JobProjectionResult(
        request, mock(NamespaceRow.class), job, Optional.empty(), Optional.empty(), version);
  }

  private static DatasetProjectionResult datasetProjection(ProjectionRequest request) {
    return new DatasetProjectionResult(request, mock(NamespaceRow.class), List.of());
  }

  private static DatasetProjection datasetProjection() {
    return new DatasetProjection(
        mock(DatasetRow.class), mock(DatasetVersionRow.class), mock(NamespaceRow.class), List.of());
  }

  private static LineageEvent lineageEvent() {
    return lineageEvent("COMPLETE");
  }

  private static LineageEvent lineageEvent(String eventType) {
    return LineageEvent.builder()
        .eventType(eventType)
        .eventTime(Instant.parse("2026-08-10T00:00:00Z").atZone(ZoneOffset.UTC))
        .run(new LineageEvent.Run(RUN_ID.toString(), null))
        .job(LineageEvent.Job.builder().namespace("job-namespace").name("job").build())
        .inputs(
            List.of(
                LineageEvent.Dataset.builder().namespace("input-namespace").name("input").build()))
        .outputs(
            List.of(
                LineageEvent.Dataset.builder()
                    .namespace("output-namespace")
                    .name("output")
                    .build()))
        .producer("https://example.com/producer")
        .build();
  }

  private static DatasetEvent datasetEvent() {
    return DatasetEvent.builder()
        .eventTime(Instant.parse("2026-08-10T00:00:00Z").atZone(ZoneOffset.UTC))
        .dataset(
            LineageEvent.Dataset.builder().namespace("dataset-namespace").name("dataset").build())
        .producer("https://example.com/producer")
        .build();
  }

  private static JobEvent jobEvent() {
    return JobEvent.builder()
        .eventTime(Instant.parse("2026-08-10T00:00:00Z").atZone(ZoneOffset.UTC))
        .job(LineageEvent.Job.builder().namespace("job-namespace").name("job").build())
        .producer("https://example.com/producer")
        .build();
  }
}
