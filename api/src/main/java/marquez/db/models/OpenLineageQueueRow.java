/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.models;

import java.time.Instant;
import java.util.UUID;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

/**
 * An OpenLineage event protected by its locked lane head, including its proposed attempt ordinal. A
 * bounded batch claim may also return that head's immediate same-admission follower.
 */
public record OpenLineageQueueRow(
    long id,
    UUID orderingKey,
    String eventJson,
    Instant enqueuedAt,
    int attemptCount,
    String lastError,
    boolean refreshDueOnAdvance,
    Long admissionId) {

  @JdbiConstructor
  public OpenLineageQueueRow {}

  /**
   * Compatibility constructor for singleton rows and callers that do not inspect queue metadata.
   */
  public OpenLineageQueueRow(
      long id,
      UUID orderingKey,
      String eventJson,
      Instant enqueuedAt,
      int attemptCount,
      String lastError) {
    this(id, orderingKey, eventJson, enqueuedAt, attemptCount, lastError, false, null);
  }
}
