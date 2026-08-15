/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.db.OpenLineageQueueDao;
import marquez.db.OpenLineageQueueDao.PreparedAdmission;
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

  /**
   * Atomically enqueues a prepared admission. Workers may project ready subsets independently, so
   * admission is not a completion barrier. Returns the admitted count after commit; waking the
   * worker remains a best-effort optimization.
   */
  public int enqueueAll(@NonNull final PreparedAdmission admission) {
    return admission.size() == 0 ? 0 : wakeAfterBatch(queueDao.enqueueAll(admission));
  }

  /** Compatibility overload for callers that prepare events individually. */
  public int enqueueAll(@NonNull final List<PreparedEvent> events) {
    if (events.isEmpty()) {
      return 0;
    }
    return wakeAfterBatch(queueDao.enqueueAll(events));
  }

  private int wakeAfterBatch(int admitted) {
    try {
      wakeUp.run();
    } catch (RuntimeException wakeFailure) {
      log.warn(
          "OpenLineage batch ({} event(s)) was queued, but the worker could not be woken",
          admitted,
          wakeFailure);
    }
    return admitted;
  }
}
