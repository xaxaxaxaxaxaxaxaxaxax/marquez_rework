/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service.models;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/** A request for a single, fixed-width statistics series. */
public record StatsQuery(
    Metric metric,
    Scope scope,
    String namespace,
    String jobName,
    UUID runId,
    Instant startAt,
    Instant endAt,
    Duration rollup) {

  public enum Metric {
    LINEAGE_EVENTS_START("START"),
    LINEAGE_EVENTS_COMPLETE("COMPLETE"),
    LINEAGE_EVENTS_FAIL("FAIL"),
    LINEAGE_EVENTS_ABORT("ABORT"),
    JOBS_TOTAL(null),
    DATASETS_TOTAL(null),
    SOURCES_TOTAL(null);

    private final String eventType;

    Metric(String eventType) {
      this.eventType = eventType;
    }

    public boolean isFlow() {
      return eventType != null;
    }

    public String eventType() {
      if (!isFlow()) {
        throw new IllegalStateException(name() + " is not a lineage-event metric");
      }
      return eventType;
    }
  }

  public enum Scope {
    GLOBAL,
    NAMESPACE,
    JOB,
    RUN
  }
}
