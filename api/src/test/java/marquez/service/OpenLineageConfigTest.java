/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.dropwizard.jackson.Jackson;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import org.junit.jupiter.api.Test;

class OpenLineageConfigTest {
  private static final Validator VALIDATOR =
      Validation.buildDefaultValidatorFactory().getValidator();

  @Test
  void defaults() {
    OpenLineageConfig config = new OpenLineageConfig();

    assertThat(OpenLineageConfig.MAX_PROJECTION_BATCH_SIZE).isEqualTo(64);
    assertThat(config.getWorkerThreads()).isEqualTo(8);
    assertThat(config.getProjectionBatchSize()).isEqualTo(8);
    assertThat(config.getPollIntervalMillis()).isEqualTo(1_000);
    assertThat(config.getMaxAttempts()).isEqualTo(10);
    assertThat(config.getRetryInitialDelayMillis()).isEqualTo(1_000);
    assertThat(config.getRetryMaxDelayMillis()).isEqualTo(60_000);
    assertThat(config.getShutdownGracePeriodMillis()).isEqualTo(30_000);
    assertThat(VALIDATOR.validate(config)).isEmpty();
  }

  @Test
  void overrides() throws Exception {
    OpenLineageConfig config =
        fromJson(
            """
            {
              "workerThreads": 2,
              "projectionBatchSize": 4,
              "pollIntervalMillis": 10,
              "maxAttempts": 3,
              "retryInitialDelayMillis": 30,
              "retryMaxDelayMillis": 40,
              "shutdownGracePeriodMillis": 50
            }
            """);

    assertThat(config.getWorkerThreads()).isEqualTo(2);
    assertThat(config.getProjectionBatchSize()).isEqualTo(4);
    assertThat(config.getPollIntervalMillis()).isEqualTo(10);
    assertThat(config.getMaxAttempts()).isEqualTo(3);
    assertThat(config.getRetryInitialDelayMillis()).isEqualTo(30);
    assertThat(config.getRetryMaxDelayMillis()).isEqualTo(40);
    assertThat(config.getShutdownGracePeriodMillis()).isEqualTo(50);
    assertThat(VALIDATOR.validate(config)).isEmpty();
  }

  @Test
  void rejectsNonPositiveWorkerSettings() throws Exception {
    OpenLineageConfig config =
        fromJson(
            """
            {
              "workerThreads": 0,
              "projectionBatchSize": 0,
              "pollIntervalMillis": 0,
              "maxAttempts": 0,
              "retryInitialDelayMillis": 0,
              "retryMaxDelayMillis": 0,
              "shutdownGracePeriodMillis": -1
            }
            """);

    Set<String> invalidProperties =
        VALIDATOR.validate(config).stream()
            .map(violation -> violation.getPropertyPath().toString())
            .collect(java.util.stream.Collectors.toSet());
    assertThat(invalidProperties)
        .contains(
            "workerThreads",
            "projectionBatchSize",
            "pollIntervalMillis",
            "maxAttempts",
            "retryInitialDelayMillis",
            "retryMaxDelayMillis",
            "shutdownGracePeriodMillis");
  }

  @Test
  void acceptsProjectionBatchSizeBounds() throws Exception {
    OpenLineageConfig singleton = fromJson("{\"projectionBatchSize\": 1}");
    OpenLineageConfig maximum =
        fromJson("{\"projectionBatchSize\": " + OpenLineageConfig.MAX_PROJECTION_BATCH_SIZE + "}");

    assertThat(VALIDATOR.validate(singleton)).isEmpty();
    assertThat(VALIDATOR.validate(maximum)).isEmpty();
  }

  @Test
  void rejectsProjectionBatchSizeAboveMaximum() throws Exception {
    OpenLineageConfig config =
        fromJson(
            "{\"projectionBatchSize\": " + (OpenLineageConfig.MAX_PROJECTION_BATCH_SIZE + 1) + "}");

    assertThat(VALIDATOR.validate(config))
        .extracting(violation -> violation.getPropertyPath().toString())
        .containsExactly("projectionBatchSize");
  }

  @Test
  void rejectsInvertedRetryDelayRange() throws Exception {
    OpenLineageConfig config =
        fromJson("{\"retryInitialDelayMillis\": 200, \"retryMaxDelayMillis\": 100}");

    Set<ConstraintViolation<OpenLineageConfig>> violations = VALIDATOR.validate(config);
    assertThat(violations)
        .anySatisfy(
            violation ->
                assertThat(violation.getPropertyPath().toString())
                    .isEqualTo("retryDelayRangeValid"));
  }

  @Test
  void rejectsRemovedLeaseDurationSetting() {
    assertThatThrownBy(() -> fromJson("{\"leaseDurationMillis\": 20}"))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("leaseDurationMillis has been removed");
  }

  @Test
  void serializationDoesNotAdvertiseLeaseConfiguration() throws Exception {
    String json = Jackson.newObjectMapper().writeValueAsString(new OpenLineageConfig());

    assertThat(json).doesNotContain("leaseDurationMillis");
  }

  @SuppressWarnings("deprecation")
  @Test
  void deprecatedQueueCapacityStillDeserializes() throws Exception {
    OpenLineageConfig config = fromJson("{\"queueCapacity\": 17}");

    assertThat(config.getQueueCapacity()).isEqualTo(17);
    assertThat(VALIDATOR.validate(config)).isEmpty();
  }

  private OpenLineageConfig fromJson(String json) throws Exception {
    return Jackson.newObjectMapper().readValue(json, OpenLineageConfig.class);
  }
}
