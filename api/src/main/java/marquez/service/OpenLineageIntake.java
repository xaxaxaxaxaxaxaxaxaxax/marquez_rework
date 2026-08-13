/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.db.OpenLineageQueueDao;
import marquez.db.OpenLineageQueueDao.PreparedEvent;
import marquez.service.models.BaseEvent;

/** Durably accepts OpenLineage events and wakes the asynchronous processor. */
@Slf4j
public final class OpenLineageIntake {
  private final OpenLineageQueueDao queueDao;
  private final Runnable wakeUp;

  public OpenLineageIntake(
      @NonNull final OpenLineageQueueDao queueDao, @NonNull final Runnable wakeUp) {
    this.queueDao = queueDao;
    this.wakeUp = wakeUp;
  }

  /**
   * Returns after the event has been committed to the durable queue. Waking the worker is only an
   * optimization: polling and restart recovery guarantee that a committed event remains visible.
   */
  public long enqueue(@NonNull final BaseEvent event) {
    return enqueue(OpenLineageQueueDao.prepare(event));
  }

  /** Enqueues an event already validated and serialized by durable admission. */
  public long enqueue(@NonNull final PreparedEvent event) {
    final long queueId = queueDao.enqueue(event);
    try {
      wakeUp.run();
    } catch (RuntimeException wakeFailure) {
      log.warn(
          "OpenLineage event {} was queued, but the worker could not be woken",
          queueId,
          wakeFailure);
    }
    return queueId;
  }
}
