/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.Min;
import lombok.Getter;

/** Bounded execution settings for OpenLineage intake. */
public class OpenLineageConfig {
  public static final int DEFAULT_WORKER_THREADS = 8;
  public static final int DEFAULT_QUEUE_CAPACITY = 100;

  @Getter
  @Min(1)
  @JsonProperty
  private int workerThreads = DEFAULT_WORKER_THREADS;

  @Getter
  @Min(1)
  @JsonProperty
  private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
}
