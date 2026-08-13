/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.codahale.metrics.health.HealthCheckRegistry;
import marquez.service.OpenLineageWorker;
import marquez.service.OpenLineageWorkerHealthCheck;
import org.junit.jupiter.api.Test;

class MarquezAppHealthCheckRegistrationTest {

  @Test
  void registersOpenLineageWorkerHealthCheck() {
    HealthCheckRegistry healthChecks = new HealthCheckRegistry();

    MarquezApp.registerHealthChecks(healthChecks, mock(OpenLineageWorker.class));

    assertThat(healthChecks.getNames()).containsExactly(OpenLineageWorkerHealthCheck.NAME);
    assertThat(healthChecks.getHealthCheck(OpenLineageWorkerHealthCheck.NAME))
        .isInstanceOf(OpenLineageWorkerHealthCheck.class);
  }
}
