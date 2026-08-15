/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.models;

import java.time.Instant;
import lombok.NonNull;
import lombok.Value;

/** A scalar metric value for a half-open interval. */
@Value
public class MetricPoint {
  @NonNull Instant startAt;
  @NonNull Instant endAt;
  long value;
}
