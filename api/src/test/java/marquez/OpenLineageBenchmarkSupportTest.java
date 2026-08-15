/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class OpenLineageBenchmarkSupportTest {
  @Test
  void summarizesDistributions() {
    assertThat(Distribution.of(List.of(3.0, 1.0, 2.0))).isEqualTo(new Distribution(2.0, 1.0, 3.0));
    assertThat(Distribution.of(List.of(4.0, 1.0, 3.0, 2.0)))
        .isEqualTo(new Distribution(2.5, 1.0, 4.0));
    assertThatThrownBy(() -> Distribution.of(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("samples must not be empty");
  }

  @Test
  void calculatesCountRate() {
    assertThat(new TimedCount(5, 2_000_000_000L).perSecond()).isEqualTo(2.5);
  }

  @Test
  void describesQueueAdmissionShapes() {
    assertThat(QueueSnapshot.empty()).isEqualTo(new QueueSnapshot(0, 0, 0, 0, 0, 0, 0));
    assertThat(QueueSnapshot.singular(8)).isEqualTo(new QueueSnapshot(8, 8, 0, 0, 8, 0, 0));
    assertThat(QueueSnapshot.batch(16, 2, 8)).isEqualTo(new QueueSnapshot(16, 16, 0, 2, 0, 8, 8));
  }
}
