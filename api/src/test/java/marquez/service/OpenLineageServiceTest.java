/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
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
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.JobRow;
import marquez.db.models.ProjectionOrder;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunIoSnapshot;
import marquez.db.models.RunRow;
import marquez.db.models.RunStateRow;
import marquez.db.models.UpdateLineageRow;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.postgresql.util.PGobject;

class OpenLineageServiceTest {
  private static final UUID RUN_ID = UUID.fromString("de2d8a76-57b3-42f6-8d26-06d6179ac45c");
  private static final UUID EFFECTIVE_RUN_ID =
      UUID.fromString("ec0f5598-20ab-4d60-ab4d-6fc280748251");

  private BaseDao baseDao;
  private OpenLineageDao openLineageDao;
  private RunService runService;
  private SearchService searchService;
  private UpdateLineageRow update;

  @BeforeEach
  void setUp() {
    baseDao = mock(BaseDao.class);
    openLineageDao = mock(OpenLineageDao.class);
    runService = mock(RunService.class);
    searchService = mock(SearchService.class);
    update = mock(UpdateLineageRow.class);
    when(baseDao.createOpenLineageDao()).thenReturn(openLineageDao);
    when(openLineageDao.updateMarquezModel(
            any(LineageEvent.class), any(ObjectMapper.class), anyBoolean()))
        .thenReturn(update);
  }

  @Test
  void admitsOneTaskAndIndexesBeforeBothDatabaseBranches() {
    AtomicInteger submittedTasks = new AtomicInteger();
    Executor directExecutor =
        task -> {
          submittedTasks.incrementAndGet();
          task.run();
        };
    OpenLineageService service = service(directExecutor);
    LineageEvent event = lineageEvent();
    PGobject serializedEvent = OpenLineageDao.createJsonObject("{\"synchronous\":\"serialized\"}");
    when(openLineageDao.createJsonArray(eq(event), any(ObjectMapper.class)))
        .thenReturn(serializedEvent);

    service.createAsync(event).join();

    assertThat(submittedTasks).hasValue(1);
    InOrder order = inOrder(searchService, openLineageDao);
    order.verify(searchService).indexEvent(event);
    order.verify(openLineageDao).createJsonArray(eq(event), any(ObjectMapper.class));
    order
        .verify(openLineageDao)
        .createLineageEvent(any(), any(), any(), any(), any(), same(serializedEvent), any());
    order
        .verify(openLineageDao)
        .updateMarquezModel(any(LineageEvent.class), any(ObjectMapper.class), eq(false));
    verify(searchService, never()).indexEvent(eq(event), any(UUID.class));
  }

  @Test
  void attemptsProjectionWhenRawEventWriteFails() {
    RuntimeException rawFailure = new RuntimeException("raw");
    doThrow(rawFailure)
        .when(openLineageDao)
        .createLineageEvent(any(), any(), any(), any(), any(), any(), any());
    OpenLineageService service = service(Runnable::run);

    CompletionException thrown =
        assertThrows(CompletionException.class, () -> service.createAsync(lineageEvent()).join());

    assertThat(thrown.getCause()).isSameAs(rawFailure);
    verify(openLineageDao)
        .updateMarquezModel(any(LineageEvent.class), any(ObjectMapper.class), eq(false));
  }

  @Test
  void preservesFirstFailureAndSuppressesProjectionFailure() {
    IllegalStateException rawFailure = new IllegalStateException("raw");
    IllegalArgumentException projectionFailure = new IllegalArgumentException("projection");
    doThrow(rawFailure)
        .when(openLineageDao)
        .createLineageEvent(any(), any(), any(), any(), any(), any(), any());
    when(openLineageDao.updateMarquezModel(
            any(LineageEvent.class), any(ObjectMapper.class), anyBoolean()))
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
    verifyNoInteractions(searchService, openLineageDao);
  }

  @Test
  void searchItemFailureAbortsBeforeDatabaseWrites() {
    doThrow(new IllegalStateException("bulk item failed"))
        .when(searchService)
        .indexEvent(any(LineageEvent.class));
    OpenLineageService service = service(Runnable::run);

    assertThrows(CompletionException.class, () -> service.createAsync(lineageEvent()).join());

    verifyNoInteractions(openLineageDao);
  }

  @Test
  void doesNotRequestListenerSnapshotForStartWhenNoListenersAreRegistered() {
    OpenLineageService service = service(Runnable::run);
    LineageEvent event = lineageEvent("START");

    service.createAsync(event).join();

    verify(runService).hasRunTransitionListeners();
    verify(openLineageDao).updateMarquezModel(eq(event), any(ObjectMapper.class), eq(false));
    verify(update, never()).getRunIoSnapshot();
    verify(runService, never()).notify(any(JobInputUpdate.class));
    verify(runService, never()).notify(any(JobOutputUpdate.class));
    verify(runService, never()).notify(any(RunTransition.class));
  }

  @Test
  void requestsListenerSnapshotAndUsesItForStartWhenListenersAreRegistered() {
    when(runService.hasRunTransitionListeners()).thenReturn(true);
    when(update.getRunIoSnapshot()).thenReturn(RunIoSnapshot.empty());
    RunRow run = mock(RunRow.class);
    when(run.getUuid()).thenReturn(RUN_ID);
    when(update.getRun()).thenReturn(run);
    OpenLineageService service = service(Runnable::run);
    LineageEvent event = lineageEvent("START");

    service.createAsync(event).join();

    verify(openLineageDao).updateMarquezModel(eq(event), any(ObjectMapper.class), eq(true));
    verify(update, atLeastOnce()).getRunIoSnapshot();
  }

  @Test
  void nullEventTypeDoesNotRequestListenerSnapshot() {
    OpenLineageService service = service(Runnable::run);
    LineageEvent event = lineageEvent(null);

    service.createAsync(event).join();

    verify(runService, never()).hasRunTransitionListeners();
    verify(openLineageDao).updateMarquezModel(eq(event), any(ObjectMapper.class), eq(false));
    verify(update, never()).getRunIoSnapshot();
  }

  @Test
  void queuedLineageProjectsBeforeRawWriteWithEffectiveRunUuid() {
    OpenLineageDao transactionalDao = mock(OpenLineageDao.class);
    when(transactionalDao.updateMarquezModel(
            any(LineageEvent.class),
            any(ObjectMapper.class),
            anyBoolean(),
            any(ProjectionOrder.class)))
        .thenReturn(update);
    RunRow effectiveRun = mock(RunRow.class);
    when(effectiveRun.getUuid()).thenReturn(EFFECTIVE_RUN_ID);
    when(update.getRun()).thenReturn(effectiveRun);
    OpenLineageService service = service(Runnable::run);
    LineageEvent event = lineageEvent();
    String eventJson = "{ \"kind\" : \"run\", \"number\" : 1.00 }";
    ArgumentCaptor<PGobject> rawEvent = ArgumentCaptor.forClass(PGobject.class);
    ArgumentCaptor<UUID> rawRunUuid = ArgumentCaptor.forClass(UUID.class);
    ArgumentCaptor<ProjectionOrder> projectionOrder =
        ArgumentCaptor.forClass(ProjectionOrder.class);

    UpdateLineageRow result =
        service.processQueuedInTransaction(event, eventJson, transactionalDao);

    assertThat(result).isSameAs(update);
    InOrder order = inOrder(transactionalDao);
    order
        .verify(transactionalDao)
        .updateMarquezModel(
            eq(event), any(ObjectMapper.class), eq(false), projectionOrder.capture());
    order
        .verify(transactionalDao)
        .createLineageEvent(
            any(), any(), rawRunUuid.capture(), any(), any(), rawEvent.capture(), any());
    verifyNoInteractions(searchService);
    verify(runService, never()).notify(any(JobInputUpdate.class));
    verify(runService, never()).notify(any(JobOutputUpdate.class));
    verify(runService, never()).notify(any(RunTransition.class));
    assertThat(rawEvent.getValue().getType()).isEqualTo("json");
    assertThat(rawEvent.getValue().getValue()).isEqualTo(eventJson);
    assertThat(rawRunUuid.getValue()).isEqualTo(EFFECTIVE_RUN_ID).isNotEqualTo(RUN_ID);
    assertThat(projectionOrder.getValue().getEventTime())
        .isEqualTo(event.getEventTime().toInstant());
    assertThat(projectionOrder.getValue().getEventKey()).isEqualTo(Utils.sha256Utf8(eventJson));
  }

  @Test
  void queuedLineageRawFailurePropagatesAfterProjectionForCallerRollback() {
    OpenLineageDao transactionalDao = mock(OpenLineageDao.class);
    when(transactionalDao.updateMarquezModel(
            any(LineageEvent.class),
            any(ObjectMapper.class),
            anyBoolean(),
            any(ProjectionOrder.class)))
        .thenReturn(update);
    RunRow effectiveRun = mock(RunRow.class);
    when(effectiveRun.getUuid()).thenReturn(EFFECTIVE_RUN_ID);
    when(update.getRun()).thenReturn(effectiveRun);
    RuntimeException rawFailure = new RuntimeException("raw write failed");
    doThrow(rawFailure)
        .when(transactionalDao)
        .createLineageEvent(any(), any(), any(), any(), any(), any(), any());
    OpenLineageService service = service(Runnable::run);
    LineageEvent event = lineageEvent();

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                service.processQueuedInTransaction(event, "{\"kind\":\"run\"}", transactionalDao));

    assertThat(thrown).isSameAs(rawFailure);
    InOrder order = inOrder(transactionalDao);
    order
        .verify(transactionalDao)
        .updateMarquezModel(
            eq(event), any(ObjectMapper.class), eq(false), any(ProjectionOrder.class));
    order
        .verify(transactionalDao)
        .createLineageEvent(any(), any(), eq(EFFECTIVE_RUN_ID), any(), any(), any(), any());
    verifyNoInteractions(searchService);
  }

  @Test
  void queuedDatasetAndJobEventsUseTransactionDao() {
    OpenLineageDao transactionalDao = mock(OpenLineageDao.class);
    OpenLineageService service = service(Runnable::run);
    DatasetEvent datasetEvent =
        DatasetEvent.builder()
            .eventTime(Instant.parse("2026-08-10T00:00:00Z").atZone(ZoneOffset.UTC))
            .dataset(
                LineageEvent.Dataset.builder()
                    .namespace("dataset-namespace")
                    .name("dataset")
                    .build())
            .producer("https://example.com/producer")
            .build();
    JobEvent jobEvent =
        JobEvent.builder()
            .eventTime(Instant.parse("2026-08-10T00:00:00Z").atZone(ZoneOffset.UTC))
            .job(LineageEvent.Job.builder().namespace("job-namespace").name("job").build())
            .producer("https://example.com/producer")
            .build();
    String datasetEventJson = "{\n  \"kind\" : \"dataset\", \"number\" : 2.00\n}";
    String jobEventJson = "{ \"kind\" : \"job\", \"number\" : 3.00 }";
    ArgumentCaptor<PGobject> rawDatasetEvent = ArgumentCaptor.forClass(PGobject.class);
    ArgumentCaptor<PGobject> rawJobEvent = ArgumentCaptor.forClass(PGobject.class);

    assertThat(service.processQueuedInTransaction(datasetEvent, datasetEventJson, transactionalDao))
        .isNull();
    assertThat(service.processQueuedInTransaction(jobEvent, jobEventJson, transactionalDao))
        .isNull();

    InOrder order = inOrder(transactionalDao);
    order.verify(transactionalDao).createDatasetEvent(any(), rawDatasetEvent.capture(), any());
    order
        .verify(transactionalDao)
        .updateMarquezModel(eq(datasetEvent), any(ObjectMapper.class), any(ProjectionOrder.class));
    order
        .verify(transactionalDao)
        .createJobEvent(any(), any(), any(), rawJobEvent.capture(), any());
    order
        .verify(transactionalDao)
        .updateMarquezModel(eq(jobEvent), any(ObjectMapper.class), any(ProjectionOrder.class));
    assertThat(rawDatasetEvent.getValue().getType()).isEqualTo("json");
    assertThat(rawDatasetEvent.getValue().getValue()).isEqualTo(datasetEventJson);
    assertThat(rawJobEvent.getValue().getType()).isEqualTo("json");
    assertThat(rawJobEvent.getValue().getValue()).isEqualTo(jobEventJson);
  }

  @Test
  void jsonObjectPreservesExactSerializedValueAndRejectsNull() {
    String eventJson = "{\n  \"number\" : 1.00, \"text\" : \"exact\"\n}";

    PGobject jsonObject = OpenLineageDao.createJsonObject(eventJson);

    assertThat(jsonObject.getType()).isEqualTo("json");
    assertThat(jsonObject.getValue()).isSameAs(eventJson);
    assertThrows(IllegalArgumentException.class, () -> OpenLineageDao.createJsonObject(null));
  }

  @Test
  void synchronousJsonCreationStillSerializesOnceBeforeWrapping() throws Exception {
    OpenLineageDao dao = mock(OpenLineageDao.class, CALLS_REAL_METHODS);
    ObjectMapper mapper = mock(ObjectMapper.class);
    LineageEvent event = lineageEvent();
    String eventJson = "{ \"synchronous\" : true }";
    when(mapper.writeValueAsString(event)).thenReturn(eventJson);

    PGobject jsonObject = dao.createJsonArray(event, mapper);

    verify(mapper).writeValueAsString(event);
    assertThat(jsonObject.getType()).isEqualTo("json");
    assertThat(jsonObject.getValue()).isSameAs(eventJson);
  }

  @Test
  void queuedPostCommitSearchFailureDoesNotSuppressListeners() {
    when(runService.hasRunTransitionListeners()).thenReturn(true);
    doThrow(new IllegalStateException("search failed"))
        .when(searchService)
        .indexEvent(any(LineageEvent.class), eq(RUN_ID));
    when(update.getRunIoSnapshot()).thenReturn(RunIoSnapshot.empty());
    RunRow run = mock(RunRow.class);
    when(run.getUuid()).thenReturn(RUN_ID);
    when(update.getRun()).thenReturn(run);
    RunStateRow runState = mock(RunStateRow.class);
    when(runState.getState()).thenReturn("RUNNING");
    when(update.getRunState()).thenReturn(runState);
    OpenLineageService service = service(Runnable::run);

    int failures = service.publishQueuedEventBestEffort(lineageEvent("START"), update);

    assertThat(failures).isEqualTo(1);
    verify(runService).notify(any(RunTransition.class));
  }

  @Test
  void queuedPostCommitSearchUsesEffectiveRunUuid() {
    RunRow effectiveRun = mock(RunRow.class);
    when(effectiveRun.getUuid()).thenReturn(EFFECTIVE_RUN_ID);
    when(update.getRun()).thenReturn(effectiveRun);
    LineageEvent event = lineageEvent();
    when(searchService.indexEvent(event, EFFECTIVE_RUN_ID)).thenReturn(true);
    OpenLineageService service = service(Runnable::run);

    int failures = service.publishQueuedEventBestEffort(event, update);

    assertThat(failures).isZero();
    verify(searchService).indexEvent(event, EFFECTIVE_RUN_ID);
    verify(searchService, never()).indexEvent(event);
  }

  @Test
  void queuedPostCommitSumsSearchAndListenerCallbackFailures() {
    when(runService.hasRunTransitionListeners()).thenReturn(true);
    when(searchService.indexEvent(any(LineageEvent.class), eq(RUN_ID))).thenReturn(false);
    ExtendedDatasetVersionRow datasetVersion = mock(ExtendedDatasetVersionRow.class);
    when(datasetVersion.getUuid()).thenReturn(UUID.randomUUID());
    when(datasetVersion.getNamespaceName()).thenReturn("dataset-namespace");
    when(datasetVersion.getDatasetName()).thenReturn("dataset");
    when(update.getRunIoSnapshot())
        .thenReturn(new RunIoSnapshot(List.of(datasetVersion), List.of(datasetVersion)));
    when(update.getInputs())
        .thenReturn(Optional.of(List.of(mock(UpdateLineageRow.DatasetRecord.class))));
    RunRow run = mock(RunRow.class);
    when(run.getUuid()).thenReturn(RUN_ID);
    when(run.getNominalStartTime()).thenReturn(Optional.empty());
    when(run.getNominalEndTime()).thenReturn(Optional.empty());
    when(update.getRun()).thenReturn(run);
    RunArgsRow runArgs = mock(RunArgsRow.class);
    when(update.getRunArgs()).thenReturn(runArgs);
    JobRow job = mock(JobRow.class);
    when(job.getName()).thenReturn("job");
    when(job.getNamespaceName()).thenReturn("job-namespace");
    when(update.getJob()).thenReturn(job);
    RunStateRow runState = mock(RunStateRow.class);
    when(runState.getState()).thenReturn("RUNNING");
    when(update.getRunState()).thenReturn(runState);
    when(runService.notify(any(JobOutputUpdate.class))).thenReturn(2);
    when(runService.notify(any(JobInputUpdate.class))).thenReturn(3);
    when(runService.notify(any(RunTransition.class))).thenReturn(4);
    OpenLineageService service = service(Runnable::run);

    int failures = service.publishQueuedEventBestEffort(lineageEvent("COMPLETE"), update);

    assertThat(failures).isEqualTo(10);
    verify(runService).notify(any(JobOutputUpdate.class));
    verify(runService).notify(any(JobInputUpdate.class));
    verify(runService).notify(any(RunTransition.class));
  }

  private OpenLineageService service(Executor executor) {
    return new OpenLineageService(baseDao, runService, searchService, executor);
  }

  private LineageEvent lineageEvent() {
    return lineageEvent("COMPLETE");
  }

  private LineageEvent lineageEvent(String eventType) {
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
}
