/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
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
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import marquez.db.BaseDao;
import marquez.db.OpenLineageDao;
import marquez.db.models.UpdateLineageRow;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.models.LineageEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class OpenLineageServiceTest {
  private static final UUID RUN_ID = UUID.fromString("de2d8a76-57b3-42f6-8d26-06d6179ac45c");

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
    when(openLineageDao.updateMarquezModel(any(LineageEvent.class), any(ObjectMapper.class)))
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

    service.createAsync(event).join();

    assertThat(submittedTasks).hasValue(1);
    InOrder order = inOrder(searchService, openLineageDao);
    order.verify(searchService).indexEvent(event);
    order
        .verify(openLineageDao)
        .createLineageEvent(any(), any(), any(), any(), any(), any(), any());
    order
        .verify(openLineageDao)
        .updateMarquezModel(any(LineageEvent.class), any(ObjectMapper.class));
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
    verify(openLineageDao).updateMarquezModel(any(LineageEvent.class), any(ObjectMapper.class));
  }

  @Test
  void preservesFirstFailureAndSuppressesProjectionFailure() {
    IllegalStateException rawFailure = new IllegalStateException("raw");
    IllegalArgumentException projectionFailure = new IllegalArgumentException("projection");
    doThrow(rawFailure)
        .when(openLineageDao)
        .createLineageEvent(any(), any(), any(), any(), any(), any(), any());
    when(openLineageDao.updateMarquezModel(any(LineageEvent.class), any(ObjectMapper.class)))
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
  void skipsSnapshotAndListenerDtoWorkWhenNoListenersAreRegistered() {
    OpenLineageService service = service(Runnable::run);

    service.createAsync(lineageEvent()).join();

    verify(runService).hasRunTransitionListeners();
    verify(update, never()).getRunIoSnapshot();
    verify(runService, never()).notify(any(JobInputUpdate.class));
    verify(runService, never()).notify(any(JobOutputUpdate.class));
    verify(runService, never()).notify(any(RunTransition.class));
  }

  private OpenLineageService service(Executor executor) {
    return new OpenLineageService(baseDao, runService, searchService, executor);
  }

  private LineageEvent lineageEvent() {
    return LineageEvent.builder()
        .eventType("COMPLETE")
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
