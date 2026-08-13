/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.jupiter.api.Test;

class MarquezConfigTest {
  @Test
  void defaultsDatabaseAutoCommentsToFalse() {
    MarquezConfig config = new MarquezConfig();

    assertThat(config.getDataSourceFactory().isAutoCommentsEnabled()).isFalse();
  }

  @Test
  void yamlWithoutDatabaseAutoCommentsKeepsDefault() throws Exception {
    MarquezConfig config =
        Jackson.newObjectMapper(new YAMLFactory()).readValue("db: {}\n", MarquezConfig.class);

    assertThat(config.getDataSourceFactory().isAutoCommentsEnabled()).isFalse();
  }

  @Test
  void yamlCanEnableDatabaseAutoComments() throws Exception {
    MarquezConfig config =
        Jackson.newObjectMapper(new YAMLFactory())
            .readValue("db:\n  autoCommentsEnabled: true\n", MarquezConfig.class);

    assertThat(config.getDataSourceFactory().isAutoCommentsEnabled()).isTrue();
  }
}
