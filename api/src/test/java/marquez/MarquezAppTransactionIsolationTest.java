/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.dropwizard.db.DataSourceFactory;
import org.junit.jupiter.api.Test;

class MarquezAppTransactionIsolationTest {
  @Test
  void normalizesDefaultIsolationToReadCommitted() {
    DataSourceFactory sourceFactory = new DataSourceFactory();

    assertThat(sourceFactory.getDefaultTransactionIsolation())
        .isEqualTo(DataSourceFactory.TransactionIsolation.DEFAULT);

    MarquezApp.enforceReadCommittedTransactionIsolation(sourceFactory);

    assertThat(sourceFactory.getDefaultTransactionIsolation())
        .isEqualTo(DataSourceFactory.TransactionIsolation.READ_COMMITTED);
  }

  @Test
  void acceptsExplicitReadCommittedIsolation() {
    DataSourceFactory sourceFactory = new DataSourceFactory();
    sourceFactory.setDefaultTransactionIsolation(
        DataSourceFactory.TransactionIsolation.READ_COMMITTED);

    MarquezApp.enforceReadCommittedTransactionIsolation(sourceFactory);

    assertThat(sourceFactory.getDefaultTransactionIsolation())
        .isEqualTo(DataSourceFactory.TransactionIsolation.READ_COMMITTED);
  }

  @Test
  void rejectsEveryExplicitNonReadCommittedIsolation() {
    for (DataSourceFactory.TransactionIsolation isolation :
        DataSourceFactory.TransactionIsolation.values()) {
      if (isolation == DataSourceFactory.TransactionIsolation.DEFAULT
          || isolation == DataSourceFactory.TransactionIsolation.READ_COMMITTED) {
        continue;
      }
      DataSourceFactory sourceFactory = new DataSourceFactory();
      sourceFactory.setDefaultTransactionIsolation(isolation);

      assertThatThrownBy(() -> MarquezApp.enforceReadCommittedTransactionIsolation(sourceFactory))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("db.defaultTransactionIsolation=READ_COMMITTED")
          .hasMessageContaining(isolation.name());
    }
  }
}
