/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.models;

import java.time.Instant;
import java.util.Arrays;
import lombok.NonNull;

/** Stable total order used only for mutable projections shared by OpenLineage queue lanes. */
public final class ProjectionOrder {
  private final Instant eventTime;
  private final byte[] eventKey;

  public ProjectionOrder(@NonNull Instant eventTime, @NonNull byte[] eventKey) {
    if (eventKey.length != 32) {
      throw new IllegalArgumentException("eventKey must be a SHA-256 digest");
    }
    this.eventTime = eventTime;
    this.eventKey = Arrays.copyOf(eventKey, eventKey.length);
  }

  public Instant getEventTime() {
    return eventTime;
  }

  public byte[] getEventKey() {
    return Arrays.copyOf(eventKey, eventKey.length);
  }
}
