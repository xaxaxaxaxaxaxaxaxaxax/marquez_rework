/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** A dense, ascending statistics series and the effective query used to produce it. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record StatsResult(
    StatsQuery.Metric metric,
    StatsQuery.Scope scope,
    String namespace,
    String jobName,
    UUID runId,
    Instant startAt,
    Instant endAt,
    @JsonSerialize(using = ToStringSerializer.class) Duration rollup,
    List<Point> points) {

  public StatsResult {
    points = List.copyOf(points);
  }

  public record Point(Instant startInterval, Instant endInterval, long value) {}
}
