/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import marquez.db.StatsDao;
import marquez.db.models.MetricPoint;
import marquez.service.models.StatsQuery;
import marquez.service.models.StatsQuery.Metric;
import marquez.service.models.StatsQuery.Scope;
import marquez.service.models.StatsResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StatsServiceTest {
  private static final Instant NOW = Instant.parse("2025-01-03T12:00:00Z");
  private static final Instant DEFAULT_START = NOW.minus(Duration.ofHours(24));
  private static final long HOUR_MILLIS = Duration.ofHours(1).toMillis();

  private StatsDao statsDao;
  private StatsService statsService;

  @BeforeEach
  void setUp() {
    statsDao = mock(StatsDao.class);
    statsService = new StatsService(statsDao, Clock.fixed(NOW, ZoneOffset.UTC));
  }

  @Test
  void appliesDefaultsAndMapsDatabasePoints() {
    MetricPoint databasePoint =
        new MetricPoint(DEFAULT_START, DEFAULT_START.plus(Duration.ofHours(1)), 4_000_000_000L);
    when(statsDao.queryLineageEventsGlobal("START", DEFAULT_START, NOW, HOUR_MILLIS))
        .thenReturn(List.of(databasePoint));

    StatsResult result = statsService.query(query(Metric.LINEAGE_EVENTS_START));

    assertThat(result.metric()).isEqualTo(Metric.LINEAGE_EVENTS_START);
    assertThat(result.scope()).isEqualTo(Scope.GLOBAL);
    assertThat(result.startAt()).isEqualTo(DEFAULT_START);
    assertThat(result.endAt()).isEqualTo(NOW);
    assertThat(result.rollup()).isEqualTo(Duration.ofHours(1));
    assertThat(result.namespace()).isNull();
    assertThat(result.jobName()).isNull();
    assertThat(result.runId()).isNull();
    assertThat(result.points())
        .containsExactly(
            new StatsResult.Point(
                DEFAULT_START, DEFAULT_START.plus(Duration.ofHours(1)), 4_000_000_000L));
  }

  @Test
  void truncatesDefaultClockTimeToPostgresPrecision() {
    Instant clockTime = NOW.plusNanos(123_456_789);
    Instant effectiveEnd = clockTime.truncatedTo(ChronoUnit.MICROS);
    Instant effectiveStart = effectiveEnd.minus(Duration.ofHours(24));
    StatsService preciseClockService =
        new StatsService(statsDao, Clock.fixed(clockTime, ZoneOffset.UTC));
    when(statsDao.queryJobsGlobal(effectiveStart, effectiveEnd, HOUR_MILLIS)).thenReturn(List.of());

    StatsResult result = preciseClockService.query(query(Metric.JOBS_TOTAL));

    assertThat(result.startAt()).isEqualTo(effectiveStart);
    assertThat(result.endAt()).isEqualTo(effectiveEnd);
    verify(statsDao).queryJobsGlobal(effectiveStart, effectiveEnd, HOUR_MILLIS);
  }

  @Test
  void dispatchesExactReportedLineageScopes() {
    UUID runId = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    when(statsDao.queryLineageEventsForNamespace(
            "FAIL", "reported-namespace", DEFAULT_START, NOW, HOUR_MILLIS))
        .thenReturn(List.of());
    when(statsDao.queryLineageEventsForJob(
            "COMPLETE", "reported-namespace", "reported-job", DEFAULT_START, NOW, HOUR_MILLIS))
        .thenReturn(List.of());
    when(statsDao.queryLineageEventsForRun("ABORT", runId, DEFAULT_START, NOW, HOUR_MILLIS))
        .thenReturn(List.of());

    statsService.query(
        new StatsQuery(
            Metric.LINEAGE_EVENTS_FAIL,
            Scope.NAMESPACE,
            "reported-namespace",
            null,
            null,
            DEFAULT_START,
            NOW,
            Duration.ofHours(1)));
    statsService.query(
        new StatsQuery(
            Metric.LINEAGE_EVENTS_COMPLETE,
            Scope.JOB,
            "reported-namespace",
            "reported-job",
            null,
            DEFAULT_START,
            NOW,
            Duration.ofHours(1)));
    statsService.query(
        new StatsQuery(
            Metric.LINEAGE_EVENTS_ABORT,
            Scope.RUN,
            null,
            null,
            runId,
            DEFAULT_START,
            NOW,
            Duration.ofHours(1)));

    verify(statsDao)
        .queryLineageEventsForNamespace(
            "FAIL", "reported-namespace", DEFAULT_START, NOW, HOUR_MILLIS);
    verify(statsDao)
        .queryLineageEventsForJob(
            "COMPLETE", "reported-namespace", "reported-job", DEFAULT_START, NOW, HOUR_MILLIS);
    verify(statsDao).queryLineageEventsForRun("ABORT", runId, DEFAULT_START, NOW, HOUR_MILLIS);
  }

  @Test
  void dispatchesEachSupportedStockShape() {
    when(statsDao.queryJobsGlobal(DEFAULT_START, NOW, HOUR_MILLIS)).thenReturn(List.of());
    when(statsDao.queryJobsForNamespace("ns", DEFAULT_START, NOW, HOUR_MILLIS))
        .thenReturn(List.of());
    when(statsDao.queryDatasetsGlobal(DEFAULT_START, NOW, HOUR_MILLIS)).thenReturn(List.of());
    when(statsDao.queryDatasetsForNamespace("ns", DEFAULT_START, NOW, HOUR_MILLIS))
        .thenReturn(List.of());
    when(statsDao.querySourcesGlobal(DEFAULT_START, NOW, HOUR_MILLIS)).thenReturn(List.of());

    statsService.query(boundedQuery(Metric.JOBS_TOTAL, Scope.GLOBAL, null, null, null));
    statsService.query(boundedQuery(Metric.JOBS_TOTAL, Scope.NAMESPACE, "ns", null, null));
    statsService.query(boundedQuery(Metric.DATASETS_TOTAL, Scope.GLOBAL, null, null, null));
    statsService.query(boundedQuery(Metric.DATASETS_TOTAL, Scope.NAMESPACE, "ns", null, null));
    statsService.query(boundedQuery(Metric.SOURCES_TOTAL, Scope.GLOBAL, null, null, null));

    verify(statsDao).queryJobsGlobal(DEFAULT_START, NOW, HOUR_MILLIS);
    verify(statsDao).queryJobsForNamespace("ns", DEFAULT_START, NOW, HOUR_MILLIS);
    verify(statsDao).queryDatasetsGlobal(DEFAULT_START, NOW, HOUR_MILLIS);
    verify(statsDao).queryDatasetsForNamespace("ns", DEFAULT_START, NOW, HOUR_MILLIS);
    verify(statsDao).querySourcesGlobal(DEFAULT_START, NOW, HOUR_MILLIS);
  }

  @Test
  void acceptsExactlyOneThousandBucketsAndRejectsOneThousandAndOne() {
    Instant startAt = Instant.parse("2025-01-01T00:00:00Z");
    when(statsDao.queryLineageEventsGlobal(
            "START", startAt, startAt.plus(Duration.ofMinutes(1000)), 60_000L))
        .thenReturn(List.of());

    statsService.query(
        new StatsQuery(
            Metric.LINEAGE_EVENTS_START,
            Scope.GLOBAL,
            null,
            null,
            null,
            startAt,
            startAt.plus(Duration.ofMinutes(1000)),
            Duration.ofMinutes(1)));

    assertThatThrownBy(
            () ->
                statsService.query(
                    new StatsQuery(
                        Metric.LINEAGE_EVENTS_START,
                        Scope.GLOBAL,
                        null,
                        null,
                        null,
                        startAt,
                        startAt.plus(Duration.ofMinutes(1001)),
                        Duration.ofMinutes(1))))
        .isInstanceOf(InvalidStatsQueryException.class)
        .hasMessageContaining("1000 buckets");
  }

  @Test
  void rejectsInvalidRangesRollupsAndScopeCombinationsBeforeQuerying() {
    List<StatsQuery> invalidQueries =
        List.of(
            query(null),
            boundedQuery(Metric.SOURCES_TOTAL, Scope.NAMESPACE, "ns", null, null),
            boundedQuery(Metric.JOBS_TOTAL, Scope.RUN, null, null, UUID.randomUUID()),
            boundedQuery(Metric.LINEAGE_EVENTS_START, Scope.JOB, "ns", null, null),
            new StatsQuery(
                Metric.LINEAGE_EVENTS_START,
                Scope.GLOBAL,
                null,
                null,
                null,
                NOW,
                NOW,
                Duration.ofHours(1)),
            new StatsQuery(
                Metric.LINEAGE_EVENTS_START,
                Scope.GLOBAL,
                null,
                null,
                null,
                DEFAULT_START,
                NOW,
                Duration.ofSeconds(59)),
            new StatsQuery(
                Metric.LINEAGE_EVENTS_START,
                Scope.GLOBAL,
                null,
                null,
                null,
                NOW.minus(Duration.ofDays(367)),
                NOW,
                Duration.ofDays(1)));

    invalidQueries.forEach(
        query ->
            assertThatThrownBy(() -> statsService.query(query))
                .isInstanceOf(InvalidStatsQueryException.class));
    verifyNoInteractions(statsDao);
  }

  @Test
  void rejectsOverlongSelectorsAndSubMillisecondRollups() {
    String tooLong = "n".repeat(1025);

    assertThatThrownBy(
            () ->
                statsService.query(
                    boundedQuery(
                        Metric.LINEAGE_EVENTS_START, Scope.NAMESPACE, tooLong, null, null)))
        .isInstanceOf(InvalidStatsQueryException.class)
        .hasMessageContaining("1024");
    assertThatThrownBy(
            () ->
                statsService.query(
                    new StatsQuery(
                        Metric.LINEAGE_EVENTS_START,
                        Scope.GLOBAL,
                        null,
                        null,
                        null,
                        DEFAULT_START,
                        NOW,
                        Duration.ofMinutes(1).plusNanos(1))))
        .isInstanceOf(InvalidStatsQueryException.class)
        .hasMessageContaining("millisecond precision");
    verifyNoInteractions(statsDao);
  }

  @Test
  void rejectsSubMicrosecondTimestampsBeforeQuerying() {
    assertThatThrownBy(
            () ->
                statsService.query(
                    new StatsQuery(
                        Metric.LINEAGE_EVENTS_START,
                        Scope.GLOBAL,
                        null,
                        null,
                        null,
                        DEFAULT_START.plusNanos(1),
                        NOW,
                        Duration.ofMinutes(1))))
        .isInstanceOf(InvalidStatsQueryException.class)
        .hasMessageContaining("startAt must have microsecond precision");
    verifyNoInteractions(statsDao);
  }

  @Test
  void reportsDefaultStartOverflowAsInvalidQuery() {
    StatsService minimumClockService =
        new StatsService(statsDao, Clock.fixed(Instant.MIN, ZoneOffset.UTC));

    assertThatThrownBy(() -> minimumClockService.query(query(Metric.LINEAGE_EVENTS_START)))
        .isInstanceOf(InvalidStatsQueryException.class)
        .hasMessageContaining("Default startAt");
    verifyNoInteractions(statsDao);
  }

  private static StatsQuery query(Metric metric) {
    return new StatsQuery(metric, null, null, null, null, null, null, null);
  }

  private static StatsQuery boundedQuery(
      Metric metric, Scope scope, String namespace, String jobName, UUID runId) {
    return new StatsQuery(
        metric, scope, namespace, jobName, runId, DEFAULT_START, NOW, Duration.ofHours(1));
  }
}
