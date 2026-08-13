/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Test;

/** The test suite for {@link DbRetentionJob}. */
class DbRetentionJobTest {
  @Test
  void runtimeFailureDoesNotPreventNextIteration() {
    final Jdbi jdbi = mock(Jdbi.class);
    final DbRetentionConfig config = new DbRetentionConfig();
    final AtomicInteger attempts = new AtomicInteger();
    final DbRetentionJob job =
        new DbRetentionJob(
            jdbi,
            config,
            (iterationJdbi, numberOfRowsPerBatch, retentionDays) -> {
              assertThat(iterationJdbi).isSameAs(jdbi);
              assertThat(numberOfRowsPerBatch).isEqualTo(config.getNumberOfRowsPerBatch());
              assertThat(retentionDays).isEqualTo(config.getRetentionDays());
              if (attempts.getAndIncrement() == 0) {
                throw new RuntimeException("transient database failure");
              }
            });

    assertThatCode(job::runOneIteration).doesNotThrowAnyException();
    assertThatCode(job::runOneIteration).doesNotThrowAnyException();
    assertThat(attempts.get()).isEqualTo(2);
  }
}
