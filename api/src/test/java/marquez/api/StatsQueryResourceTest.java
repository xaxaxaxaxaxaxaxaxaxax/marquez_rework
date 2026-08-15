/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.dropwizard.jersey.errors.ErrorMessage;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import marquez.common.Utils;
import marquez.db.models.IntervalMetric;
import marquez.service.InvalidStatsQueryException;
import marquez.service.ServiceFactory;
import marquez.service.StatsService;
import marquez.service.models.StatsQuery;
import marquez.service.models.StatsQuery.Metric;
import marquez.service.models.StatsQuery.Scope;
import marquez.service.models.StatsResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(DropwizardExtensionsSupport.class)
class StatsQueryResourceTest {
  private static final StatsService STATS_SERVICE = mock(StatsService.class);
  private static final StatsResource RESOURCE;
  private static final ResourceExtension UNDER_TEST;

  static {
    ServiceFactory serviceFactory = mock(ServiceFactory.class);
    when(serviceFactory.getStatsService()).thenReturn(STATS_SERVICE);
    RESOURCE = new StatsResource(serviceFactory);
    UNDER_TEST =
        ResourceExtension.builder()
            .setMapper(Utils.newObjectMapper())
            .addResource(RESOURCE)
            .build();
  }

  @BeforeEach
  void setUp() {
    reset(STATS_SERVICE);
  }

  @Test
  void parsesQueryAndSerializesCanonicalResult() throws Exception {
    UUID runId = UUID.fromString("d46e465b-d358-4d32-83d4-df660ff614dd");
    Instant startAt = Instant.parse("2026-08-01T00:00:00Z");
    Instant endAt = Instant.parse("2026-08-01T02:00:00Z");
    StatsQuery expected =
        new StatsQuery(
            Metric.LINEAGE_EVENTS_COMPLETE,
            Scope.RUN,
            null,
            null,
            runId,
            startAt,
            endAt,
            Duration.ofHours(1));
    StatsResult result =
        new StatsResult(
            expected.metric(),
            expected.scope(),
            null,
            null,
            runId,
            startAt,
            endAt,
            Duration.ofHours(1),
            List.of(new StatsResult.Point(startAt, startAt.plus(Duration.ofHours(1)), 7L)));
    when(STATS_SERVICE.query(expected)).thenReturn(result);

    try (Response response =
        UNDER_TEST
            .target("/api/v1/stats/query")
            .queryParam("metric", "LINEAGE_EVENTS_COMPLETE")
            .queryParam("scope", "RUN")
            .queryParam("runId", runId)
            .queryParam("startAt", "2026-08-01T12:00:00+12:00")
            .queryParam("endAt", "2026-08-01T14:00:00+12:00")
            .queryParam("rollup", "PT60M")
            .request()
            .get()) {
      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE)).isTrue();
      JsonNode json = Utils.newObjectMapper().readTree(response.readEntity(String.class));
      assertThat(json.path("metric").asText()).isEqualTo("LINEAGE_EVENTS_COMPLETE");
      assertThat(json.path("scope").asText()).isEqualTo("RUN");
      assertThat(json.path("runId").asText()).isEqualTo(runId.toString());
      assertThat(json.has("namespace")).isFalse();
      assertThat(json.has("jobName")).isFalse();
      assertThat(json.path("startAt").asText()).isEqualTo("2026-08-01T00:00:00Z");
      assertThat(json.path("endAt").asText()).isEqualTo("2026-08-01T02:00:00Z");
      assertThat(json.path("rollup").asText()).isEqualTo("PT1H");
      assertThat(json.at("/points/0/startInterval").asText()).isEqualTo("2026-08-01T00:00:00Z");
      assertThat(json.at("/points/0/endInterval").asText()).isEqualTo("2026-08-01T01:00:00Z");
      assertThat(json.at("/points/0/value").asLong()).isEqualTo(7L);
    }
    verify(STATS_SERVICE).query(expected);
  }

  @Test
  void malformedHttpQueryReturnsJsonError() throws Exception {
    try (Response response =
        UNDER_TEST
            .target("/api/v1/stats/query")
            .queryParam("metric", "jobs_total")
            .request()
            .get()) {
      assertThat(response.getStatus()).isEqualTo(400);
      assertThat(response.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE)).isTrue();
      JsonNode error = Utils.newObjectMapper().readTree(response.readEntity(String.class));
      assertThat(error.path("code").asInt()).isEqualTo(400);
      assertThat(error.path("message").asText()).isEqualTo("Invalid metric: jobs_total");
    }
    verifyNoInteractions(STATS_SERVICE);
  }

  @Test
  void passesOptionalValuesToServiceForDefaulting() {
    StatsQuery expected =
        new StatsQuery(Metric.JOBS_TOTAL, null, null, null, null, null, null, null);
    StatsResult result =
        new StatsResult(
            Metric.JOBS_TOTAL,
            Scope.GLOBAL,
            null,
            null,
            null,
            Instant.parse("2026-08-01T00:00:00Z"),
            Instant.parse("2026-08-02T00:00:00Z"),
            Duration.ofHours(1),
            List.of());
    when(STATS_SERVICE.query(expected)).thenReturn(result);

    Response response = RESOURCE.query("JOBS_TOTAL", null, null, null, null, null, null, null);

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isSameAs(result);
    verify(STATS_SERVICE).query(expected);
  }

  @ParameterizedTest
  @MethodSource("malformedQueries")
  void rejectsMalformedParametersBeforeService(
      String metric,
      String scope,
      String runId,
      String startAt,
      String endAt,
      String rollup,
      String expectedMessage) {
    Response response = RESOURCE.query(metric, scope, null, null, runId, startAt, endAt, rollup);

    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.getMediaType()).isEqualTo(MediaType.APPLICATION_JSON_TYPE);
    assertThat(response.getEntity()).isInstanceOf(ErrorMessage.class);
    JsonNode error = Utils.newObjectMapper().valueToTree(response.getEntity());
    assertThat(error.path("code").asInt()).isEqualTo(400);
    assertThat(error.path("message").asText()).isEqualTo(expectedMessage);
    verifyNoInteractions(STATS_SERVICE);
  }

  @Test
  void mapsOnlyInvalidStatsQueryExceptionToBadRequest() {
    when(STATS_SERVICE.query(any(StatsQuery.class)))
        .thenThrow(new InvalidStatsQueryException("Range must not exceed 366 days"));

    Response response = RESOURCE.query("JOBS_TOTAL", null, null, null, null, null, null, null);

    assertThat(response.getStatus()).isEqualTo(400);
    JsonNode error = Utils.newObjectMapper().valueToTree(response.getEntity());
    assertThat(error.path("message").asText()).isEqualTo("Range must not exceed 366 days");
  }

  @Test
  void doesNotMisclassifyUnexpectedServiceFailures() {
    when(STATS_SERVICE.query(any(StatsQuery.class)))
        .thenThrow(new IllegalStateException("query failed"));

    assertThatThrownBy(() -> RESOURCE.query("JOBS_TOTAL", null, null, null, null, null, null, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("query failed");
  }

  @Test
  void literalQueryRouteDoesNotChangeLegacyJobRoute() throws Exception {
    Instant startAt = Instant.parse("2026-08-01T00:00:00Z");
    when(STATS_SERVICE.getLastDayJobs())
        .thenReturn(List.of(new IntervalMetric(startAt, startAt.plus(Duration.ofHours(1)), 3)));

    try (Response response =
        UNDER_TEST.target("/api/v1/stats/jobs").queryParam("period", "DAY").request().get()) {
      assertThat(response.getStatus()).isEqualTo(200);
      JsonNode json = Utils.newObjectMapper().readTree(response.readEntity(String.class));
      assertThat(json.isArray()).isTrue();
      assertThat(json.at("/0/count").asInt()).isEqualTo(3);
    }
    verify(STATS_SERVICE).getLastDayJobs();
  }

  private static Stream<Arguments> malformedQueries() {
    return Stream.of(
        Arguments.of(null, null, null, null, null, null, "metric is required"),
        Arguments.of("jobs_total", null, null, null, null, null, "Invalid metric: jobs_total"),
        Arguments.of("JOBS_TOTAL", "global", null, null, null, null, "Invalid scope: global"),
        Arguments.of(
            "LINEAGE_EVENTS_START",
            "RUN",
            "not-a-uuid",
            null,
            null,
            null,
            "Invalid runId: not-a-uuid"),
        Arguments.of(
            "LINEAGE_EVENTS_START",
            "RUN",
            "1-1-1-1-1",
            null,
            null,
            null,
            "Invalid runId: 1-1-1-1-1"),
        Arguments.of(
            "JOBS_TOTAL",
            null,
            null,
            "2026-08-01T00:00:00",
            null,
            null,
            "Invalid startAt: 2026-08-01T00:00:00"),
        Arguments.of("JOBS_TOTAL", null, null, null, "tomorrow", null, "Invalid endAt: tomorrow"),
        Arguments.of("JOBS_TOTAL", null, null, null, null, "hourly", "Invalid rollup: hourly"));
  }
}
