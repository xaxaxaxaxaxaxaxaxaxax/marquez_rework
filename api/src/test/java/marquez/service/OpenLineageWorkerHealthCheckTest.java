/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.health.HealthCheck.Result;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class OpenLineageWorkerHealthCheckTest {

  @Test
  void mapsHealthyAndLifecycleStatuses() {
    AtomicReference<OpenLineageWorker.HealthStatus> status =
        new AtomicReference<>(
            new OpenLineageWorker.HealthStatus(false, "OpenLineage worker has not started", null));
    OpenLineageWorkerHealthCheck healthCheck = new OpenLineageWorkerHealthCheck(status::get);

    Result notStarted = healthCheck.execute();
    assertThat(notStarted.isHealthy()).isFalse();
    assertThat(notStarted.getMessage()).isEqualTo("OpenLineage worker has not started");
    assertThat(notStarted.getError()).isNull();

    status.set(new OpenLineageWorker.HealthStatus(true, "OpenLineage worker is running", null));
    Result running = healthCheck.execute();
    assertThat(running.isHealthy()).isTrue();
    assertThat(running.getMessage()).isEqualTo("OpenLineage worker is running");
    assertThat(running.getError()).isNull();
  }

  @Test
  void preservesFatalTaskAndCoordinatorFailures() {
    AssertionError taskFailure = new AssertionError("fatal task failure");
    AtomicReference<OpenLineageWorker.HealthStatus> status =
        new AtomicReference<>(
            new OpenLineageWorker.HealthStatus(
                false, "OpenLineage worker task failed", taskFailure));
    OpenLineageWorkerHealthCheck healthCheck = new OpenLineageWorkerHealthCheck(status::get);

    Result failedTask = healthCheck.execute();
    assertThat(failedTask.isHealthy()).isFalse();
    assertThat(failedTask.getMessage()).isEqualTo("OpenLineage worker task failed");
    assertThat(failedTask.getError()).isSameAs(taskFailure);

    IllegalStateException coordinatorFailure =
        new IllegalStateException("coordinator infrastructure failure");
    status.set(
        new OpenLineageWorker.HealthStatus(
            false, "OpenLineage worker coordinator failed", coordinatorFailure));
    Result failedCoordinator = healthCheck.execute();
    assertThat(failedCoordinator.isHealthy()).isFalse();
    assertThat(failedCoordinator.getMessage()).isEqualTo("OpenLineage worker coordinator failed");
    assertThat(failedCoordinator.getError()).isSameAs(coordinatorFailure);
  }

  @Test
  void preservesPersistentPollFailure() {
    IllegalStateException pollFailure = new IllegalStateException("database unavailable");
    OpenLineageWorkerHealthCheck healthCheck =
        new OpenLineageWorkerHealthCheck(
            () ->
                new OpenLineageWorker.HealthStatus(
                    false, "OpenLineage queue polling is persistently failing", pollFailure));

    Result failed = healthCheck.execute();

    assertThat(failed.isHealthy()).isFalse();
    assertThat(failed.getMessage()).isEqualTo("OpenLineage queue polling is persistently failing");
    assertThat(failed.getError()).isSameAs(pollFailure);
  }
}
