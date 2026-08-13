/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import java.util.List;
import marquez.jobs.DbRetentionConfig;
import marquez.jobs.DbRetentionJob;
import marquez.jobs.MaterializeViewRefresherJob;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MarquezAppLifecycleOrderTest {

  @Test
  void workerIsRegisteredLastWhenRetentionIsEnabled() {
    LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
    Jdbi jdbi = mock(Jdbi.class);
    MarquezConfig config = mock(MarquezConfig.class);
    Managed worker = mock(Managed.class);
    when(config.hasDbRetentionPolicy()).thenReturn(true);
    when(config.getDbRetention()).thenReturn(new DbRetentionConfig());

    MarquezApp.registerManagedServices(lifecycle, jdbi, config, worker);

    List<Managed> registrations = registrations(lifecycle, 3);
    assertThat(registrations.get(0)).isInstanceOf(DbRetentionJob.class);
    assertThat(registrations.get(1)).isInstanceOf(MaterializeViewRefresherJob.class);
    assertThat(registrations.get(2)).isSameAs(worker);
  }

  @Test
  void workerIsRegisteredLastWhenRetentionIsDisabled() {
    LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
    Jdbi jdbi = mock(Jdbi.class);
    MarquezConfig config = mock(MarquezConfig.class);
    Managed worker = mock(Managed.class);
    when(config.hasDbRetentionPolicy()).thenReturn(false);

    MarquezApp.registerManagedServices(lifecycle, jdbi, config, worker);

    List<Managed> registrations = registrations(lifecycle, 2);
    assertThat(registrations.get(0)).isInstanceOf(MaterializeViewRefresherJob.class);
    assertThat(registrations.get(1)).isSameAs(worker);
  }

  private static List<Managed> registrations(LifecycleEnvironment lifecycle, int count) {
    ArgumentCaptor<Managed> registrations = ArgumentCaptor.forClass(Managed.class);
    verify(lifecycle, times(count)).manage(registrations.capture());
    return registrations.getAllValues();
  }
}
