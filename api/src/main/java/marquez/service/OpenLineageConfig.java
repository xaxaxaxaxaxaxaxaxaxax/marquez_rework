/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import lombok.Getter;

/** Durable OpenLineage intake worker settings. */
public class OpenLineageConfig {
  public static final int DEFAULT_WORKER_THREADS = 8;
  public static final long DEFAULT_POLL_INTERVAL_MILLIS = 1_000;
  public static final int DEFAULT_MAX_ATTEMPTS = 10;
  public static final long DEFAULT_RETRY_INITIAL_DELAY_MILLIS = 1_000;
  public static final long DEFAULT_RETRY_MAX_DELAY_MILLIS = 60_000;
  public static final long DEFAULT_SHUTDOWN_GRACE_PERIOD_MILLIS = 30_000;

  /**
   * @deprecated The durable database queue is not count-capped.
   */
  @Deprecated public static final int DEFAULT_QUEUE_CAPACITY = 100;

  @Getter
  @Min(1)
  @JsonProperty
  private int workerThreads = DEFAULT_WORKER_THREADS;

  @Getter
  @Min(1)
  @JsonProperty
  private long pollIntervalMillis = DEFAULT_POLL_INTERVAL_MILLIS;

  @Getter
  @Min(1)
  @JsonProperty
  private int maxAttempts = DEFAULT_MAX_ATTEMPTS;

  @Getter
  @Min(1)
  @JsonProperty
  private long retryInitialDelayMillis = DEFAULT_RETRY_INITIAL_DELAY_MILLIS;

  @Getter
  @Min(1)
  @JsonProperty
  private long retryMaxDelayMillis = DEFAULT_RETRY_MAX_DELAY_MILLIS;

  @Getter
  @Min(0)
  @JsonProperty
  private long shutdownGracePeriodMillis = DEFAULT_SHUTDOWN_GRACE_PERIOD_MILLIS;

  /**
   * Retained only so configurations from the former in-memory executor continue to deserialize. The
   * durable database queue ignores this value.
   */
  @Deprecated
  @Min(1)
  @JsonProperty
  private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

  /**
   * @deprecated The durable database queue is not count-capped.
   */
  @Deprecated
  public int getQueueCapacity() {
    return queueCapacity;
  }

  /** Rejects the former lease setting instead of silently accepting an ineffective timeout. */
  @JsonSetter("leaseDurationMillis")
  private void rejectRemovedLeaseDuration(long ignored) {
    throw new IllegalArgumentException(
        "openLineage.leaseDurationMillis has been removed; queue ownership is transaction-scoped");
  }

  @AssertTrue(message = "retryInitialDelayMillis must not exceed retryMaxDelayMillis")
  @JsonIgnore
  public boolean isRetryDelayRangeValid() {
    return retryInitialDelayMillis <= retryMaxDelayMillis;
  }
}
