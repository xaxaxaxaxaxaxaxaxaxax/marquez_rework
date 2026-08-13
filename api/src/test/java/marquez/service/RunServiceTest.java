/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import marquez.db.BaseDao;
import marquez.db.JobDao;
import marquez.db.JobVersionDao;
import marquez.db.RunDao;
import marquez.db.RunStateDao;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunTransition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RunServiceTest {
  private RunTransitionListener first;
  private RunTransitionListener middle;
  private RunTransitionListener last;
  private RunService runService;

  @BeforeEach
  void setUp() {
    BaseDao baseDao = mock(BaseDao.class);
    when(baseDao.createRunDao()).thenReturn(mock(RunDao.class));
    when(baseDao.createJobVersionDao()).thenReturn(mock(JobVersionDao.class));
    when(baseDao.createRunStateDao()).thenReturn(mock(RunStateDao.class));
    when(baseDao.createJobDao()).thenReturn(mock(JobDao.class));
    first = mock(RunTransitionListener.class);
    middle = mock(RunTransitionListener.class);
    last = mock(RunTransitionListener.class);
    runService = new RunService(baseDao, List.of(first, middle, last));
  }

  @Test
  void everyNotifyOverloadCountsFailuresAndContinuesToRemainingListeners() {
    JobInputUpdate input = mock(JobInputUpdate.class);
    JobOutputUpdate output = mock(JobOutputUpdate.class);
    RunTransition transition = mock(RunTransition.class);
    doThrow(new IllegalStateException("first input")).when(first).notify(input);
    doThrow(new IllegalStateException("last input")).when(last).notify(input);
    doThrow(new IllegalStateException("first output")).when(first).notify(output);
    doThrow(new IllegalStateException("last output")).when(last).notify(output);
    doThrow(new IllegalStateException("first transition")).when(first).notify(transition);
    doThrow(new IllegalStateException("last transition")).when(last).notify(transition);

    assertThat(runService.notify(input)).isEqualTo(2);
    assertThat(runService.notify(output)).isEqualTo(2);
    assertThat(runService.notify(transition)).isEqualTo(2);

    verify(middle).notify(input);
    verify(middle).notify(output);
    verify(middle).notify(transition);
  }
}
