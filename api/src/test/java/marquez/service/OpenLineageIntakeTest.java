/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import marquez.db.OpenLineageQueueDao;
import marquez.db.OpenLineageQueueDao.PreparedEvent;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.LineageEvent;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class OpenLineageIntakeTest {
  private final OpenLineageQueueDao queueDao = mock(OpenLineageQueueDao.class);
  private final Runnable wakeUp = mock(Runnable.class);
  private final OpenLineageIntake intake = new OpenLineageIntake(queueDao, wakeUp);
  private final PreparedEvent prepared = new PreparedEvent(UUID.randomUUID(), "{}");
  private final PreparedEvent secondPrepared = new PreparedEvent(UUID.randomUUID(), "{}");
  private final List<PreparedEvent> batch = List.of(prepared, secondPrepared);

  @Test
  void enqueueCommitsBeforeWakingWorker() {
    when(queueDao.enqueue(prepared)).thenReturn(42L);

    assertEquals(42L, intake.enqueue(prepared));

    InOrder inOrder = inOrder(queueDao, wakeUp);
    inOrder.verify(queueDao).enqueue(prepared);
    inOrder.verify(wakeUp).run();
  }

  @Test
  void enqueueRemainsSuccessfulWhenWakeUpFails() {
    when(queueDao.enqueue(prepared)).thenReturn(42L);
    doThrow(new IllegalStateException("stopped")).when(wakeUp).run();

    assertEquals(42L, intake.enqueue(prepared));

    InOrder inOrder = inOrder(queueDao, wakeUp);
    inOrder.verify(queueDao).enqueue(prepared);
    inOrder.verify(wakeUp).run();
  }

  @Test
  void commitIndeterminateFailureIsPropagatedWithoutRetryOrWakeUp() {
    IllegalStateException failure = new IllegalStateException("commit response lost");
    when(queueDao.enqueue(prepared)).thenThrow(failure);

    assertSame(failure, assertThrows(IllegalStateException.class, () -> intake.enqueue(prepared)));
    verify(queueDao).enqueue(prepared);
    verifyNoInteractions(wakeUp);
  }

  @Test
  void enqueueAllCommitsBeforeWakingWorkerOnce() {
    when(queueDao.enqueueAll(same(batch))).thenReturn(2);

    assertEquals(2, intake.enqueueAll(batch));

    InOrder inOrder = inOrder(queueDao, wakeUp);
    inOrder.verify(queueDao).enqueueAll(same(batch));
    inOrder.verify(wakeUp).run();
    verifyNoMoreInteractions(queueDao, wakeUp);
  }

  @Test
  void enqueueAllRemainsSuccessfulWhenWakeUpFails() {
    when(queueDao.enqueueAll(same(batch))).thenReturn(2);
    doThrow(new IllegalStateException("stopped")).when(wakeUp).run();

    assertEquals(2, intake.enqueueAll(batch));

    InOrder inOrder = inOrder(queueDao, wakeUp);
    inOrder.verify(queueDao).enqueueAll(same(batch));
    inOrder.verify(wakeUp).run();
    verifyNoMoreInteractions(queueDao, wakeUp);
  }

  @Test
  void batchCommitIndeterminateFailureIsPropagatedWithoutRetryOrWakeUp() {
    IllegalStateException failure = new IllegalStateException("batch commit response lost");
    when(queueDao.enqueueAll(same(batch))).thenThrow(failure);

    assertSame(failure, assertThrows(IllegalStateException.class, () -> intake.enqueueAll(batch)));
    verify(queueDao).enqueueAll(same(batch));
    verifyNoMoreInteractions(queueDao);
    verifyNoInteractions(wakeUp);
  }

  @Test
  void emptyBatchReturnsZeroWithoutQueueOrWakeUp() {
    assertEquals(0, intake.enqueueAll(List.of()));

    verifyNoInteractions(queueDao, wakeUp);
  }

  @Test
  void nullBatchIsRejectedWithoutQueueOrWakeUp() {
    assertThrows(NullPointerException.class, () -> intake.enqueueAll(null));

    verifyNoInteractions(queueDao, wakeUp);
  }

  @Test
  void baseEventIsPreparedBeforeEnqueue() {
    BaseEvent event =
        DatasetEvent.builder()
            .eventTime(ZonedDateTime.parse("2026-08-11T00:00:00Z"))
            .dataset(LineageEvent.Dataset.builder().namespace("namespace").name("dataset").build())
            .producer("https://example.com/producer")
            .schemaURL(
                URI.create(
                    "https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/DatasetEvent"))
            .build();
    PreparedEvent expected = OpenLineageQueueDao.prepare(event);
    when(queueDao.enqueue(expected)).thenReturn(42L);

    assertEquals(42L, intake.enqueue(event));

    verify(queueDao).enqueue(expected);
  }
}
