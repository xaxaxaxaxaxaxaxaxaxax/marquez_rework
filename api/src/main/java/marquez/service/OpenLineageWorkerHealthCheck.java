/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import com.codahale.metrics.health.HealthCheck;
import java.util.Objects;
import java.util.function.Supplier;

/** Reports whether the durable OpenLineage worker can actively drain admitted events. */
public final class OpenLineageWorkerHealthCheck extends HealthCheck {
  public static final String NAME = "open-lineage-worker";

  private final Supplier<OpenLineageWorker.HealthStatus> statusSupplier;

  public OpenLineageWorkerHealthCheck(OpenLineageWorker worker) {
    this(Objects.requireNonNull(worker, "worker")::healthStatus);
  }

  OpenLineageWorkerHealthCheck(Supplier<OpenLineageWorker.HealthStatus> statusSupplier) {
    this.statusSupplier = Objects.requireNonNull(statusSupplier, "statusSupplier");
  }

  @Override
  protected Result check() {
    OpenLineageWorker.HealthStatus status = statusSupplier.get();
    if (status.healthy()) {
      return Result.healthy(status.message());
    }
    if (status.failure() != null) {
      return Result.builder().unhealthy(status.failure()).withMessage(status.message()).build();
    }
    return Result.unhealthy(status.message());
  }
}
