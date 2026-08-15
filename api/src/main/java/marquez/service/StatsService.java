/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import marquez.db.StatsDao;
import marquez.db.models.IntervalMetric;
import marquez.db.models.LineageMetric;
import marquez.db.models.MetricPoint;
import marquez.service.models.StatsQuery;
import marquez.service.models.StatsQuery.Metric;
import marquez.service.models.StatsQuery.Scope;
import marquez.service.models.StatsResult;

public class StatsService {
  static final Duration DEFAULT_RANGE = Duration.ofHours(24);
  static final Duration DEFAULT_ROLLUP = Duration.ofHours(1);
  static final Duration MIN_ROLLUP = Duration.ofMinutes(1);
  static final Duration MAX_ROLLUP = Duration.ofDays(30);
  static final Duration MAX_RANGE = Duration.ofDays(366);
  static final long MAX_BUCKETS = 1000;
  static final int MAX_SELECTOR_LENGTH = 1024;

  private final StatsDao statsDao;
  private final Clock clock;

  public StatsService(StatsDao statsDao) {
    this(statsDao, Clock.systemUTC());
  }

  StatsService(StatsDao statsDao, Clock clock) {
    this.statsDao = statsDao;
    this.clock = clock;
  }

  public StatsResult query(StatsQuery query) {
    StatsQuery effectiveQuery = validateAndApplyDefaults(query);
    List<StatsResult.Point> points =
        queryDao(effectiveQuery).stream().map(StatsService::toResultPoint).toList();
    return new StatsResult(
        effectiveQuery.metric(),
        effectiveQuery.scope(),
        effectiveQuery.namespace(),
        effectiveQuery.jobName(),
        effectiveQuery.runId(),
        effectiveQuery.startAt(),
        effectiveQuery.endAt(),
        effectiveQuery.rollup(),
        points);
  }

  public List<LineageMetric> getLastDayLineageMetrics() {
    return this.statsDao.getLastDayMetrics();
  }

  public List<LineageMetric> getLastWeekLineageMetrics(String timezone) {
    return this.statsDao.getLastWeekMetrics(timezone);
  }

  public List<IntervalMetric> getLastDayJobs() {
    return this.statsDao.getLastDayJobs();
  }

  public List<IntervalMetric> getLastWeekJobs(String timezone) {
    return this.statsDao.getLastWeekJobs(timezone);
  }

  public List<IntervalMetric> getLastDayDatasets() {
    return this.statsDao.getLastDayDatasets();
  }

  public List<IntervalMetric> getLastWeekDatasets(String timezone) {
    return this.statsDao.getLastWeekDatasets(timezone);
  }

  public List<IntervalMetric> getLastDaySources() {
    return this.statsDao.getLastDaySources();
  }

  public List<IntervalMetric> getLastWeekSources(String timezone) {
    return this.statsDao.getLastWeekSources(timezone);
  }

  private StatsQuery validateAndApplyDefaults(StatsQuery query) {
    if (query == null) {
      throw invalid("Query is required");
    }
    Metric metric = query.metric();
    if (metric == null) {
      throw invalid("Metric is required");
    }

    Scope scope = query.scope() == null ? Scope.GLOBAL : query.scope();
    Instant endAt =
        query.endAt() == null ? clock.instant().truncatedTo(ChronoUnit.MICROS) : query.endAt();
    Instant startAt;
    try {
      startAt = query.startAt() == null ? endAt.minus(DEFAULT_RANGE) : query.startAt();
    } catch (DateTimeException | ArithmeticException error) {
      throw new InvalidStatsQueryException(
          "Default startAt is outside the supported instant range", error);
    }
    Duration rollup = query.rollup() == null ? DEFAULT_ROLLUP : query.rollup();

    validateTimestampPrecision(startAt, "startAt");
    validateTimestampPrecision(endAt, "endAt");
    validateRange(startAt, endAt, rollup);
    validateScope(metric, scope, query.namespace(), query.jobName(), query.runId());

    return new StatsQuery(
        metric, scope, query.namespace(), query.jobName(), query.runId(), startAt, endAt, rollup);
  }

  private static void validateRange(Instant startAt, Instant endAt, Duration rollup) {
    if (!startAt.isBefore(endAt)) {
      throw invalid("startAt must be before endAt");
    }
    if (rollup.compareTo(MIN_ROLLUP) < 0 || rollup.compareTo(MAX_ROLLUP) > 0) {
      throw invalid("Rollup must be between PT1M and P30D");
    }
    if (rollup.toNanos() % 1_000_000 != 0) {
      throw invalid("Rollup must have millisecond precision");
    }

    Duration range = Duration.between(startAt, endAt);
    if (range.compareTo(MAX_RANGE) > 0) {
      throw invalid("Range must not exceed 366 days");
    }
    long completeBuckets = range.dividedBy(rollup);
    long bucketCount =
        rollup.multipliedBy(completeBuckets).equals(range) ? completeBuckets : completeBuckets + 1;
    if (bucketCount > MAX_BUCKETS) {
      throw invalid("Query must not produce more than 1000 buckets");
    }
  }

  private static void validateTimestampPrecision(Instant instant, String name) {
    if (instant.getNano() % 1_000 != 0) {
      throw invalid(name + " must have microsecond precision");
    }
  }

  private static void validateScope(
      Metric metric, Scope scope, String namespace, String jobName, java.util.UUID runId) {
    switch (scope) {
      case GLOBAL -> requireAbsent(namespace, jobName, runId, "GLOBAL");
      case NAMESPACE -> {
        requireNotBlank(namespace, "namespace");
        requireAbsent(null, jobName, runId, "NAMESPACE");
        if (Metric.SOURCES_TOTAL.equals(metric)) {
          throw invalid("SOURCES_TOTAL only supports GLOBAL scope");
        }
      }
      case JOB -> {
        requireNotBlank(namespace, "namespace");
        requireNotBlank(jobName, "jobName");
        requireAbsent(null, null, runId, "JOB");
        requireFlow(metric, scope);
      }
      case RUN -> {
        if (runId == null) {
          throw invalid("runId is required for RUN scope");
        }
        requireAbsent(namespace, jobName, null, "RUN");
        requireFlow(metric, scope);
      }
    }
  }

  private static void requireFlow(Metric metric, Scope scope) {
    if (!metric.isFlow()) {
      throw invalid(metric + " does not support " + scope + " scope");
    }
  }

  private static void requireNotBlank(String value, String name) {
    if (value == null || value.isBlank()) {
      throw invalid(name + " is required for this scope");
    }
    if (value.length() > MAX_SELECTOR_LENGTH) {
      throw invalid(name + " must not exceed 1024 characters");
    }
  }

  private static void requireAbsent(
      String namespace, String jobName, java.util.UUID runId, String scope) {
    if (namespace != null || jobName != null || runId != null) {
      throw invalid("Unrelated scope identifiers are not allowed for " + scope + " scope");
    }
  }

  private static StatsResult.Point toResultPoint(MetricPoint point) {
    return new StatsResult.Point(point.getStartAt(), point.getEndAt(), point.getValue());
  }

  private List<MetricPoint> queryDao(StatsQuery query) {
    long bucketMillis = query.rollup().toMillis();
    if (query.metric().isFlow()) {
      return switch (query.scope()) {
        case GLOBAL ->
            statsDao.queryLineageEventsGlobal(
                query.metric().eventType(), query.startAt(), query.endAt(), bucketMillis);
        case NAMESPACE ->
            statsDao.queryLineageEventsForNamespace(
                query.metric().eventType(),
                query.namespace(),
                query.startAt(),
                query.endAt(),
                bucketMillis);
        case JOB ->
            statsDao.queryLineageEventsForJob(
                query.metric().eventType(),
                query.namespace(),
                query.jobName(),
                query.startAt(),
                query.endAt(),
                bucketMillis);
        case RUN ->
            statsDao.queryLineageEventsForRun(
                query.metric().eventType(),
                query.runId(),
                query.startAt(),
                query.endAt(),
                bucketMillis);
      };
    }
    return switch (query.metric()) {
      case JOBS_TOTAL ->
          Scope.GLOBAL.equals(query.scope())
              ? statsDao.queryJobsGlobal(query.startAt(), query.endAt(), bucketMillis)
              : statsDao.queryJobsForNamespace(
                  query.namespace(), query.startAt(), query.endAt(), bucketMillis);
      case DATASETS_TOTAL ->
          Scope.GLOBAL.equals(query.scope())
              ? statsDao.queryDatasetsGlobal(query.startAt(), query.endAt(), bucketMillis)
              : statsDao.queryDatasetsForNamespace(
                  query.namespace(), query.startAt(), query.endAt(), bucketMillis);
      case SOURCES_TOTAL ->
          statsDao.querySourcesGlobal(query.startAt(), query.endAt(), bucketMillis);
      default -> throw new IllegalStateException("Unexpected flow metric in stock query");
    };
  }

  private static InvalidStatsQueryException invalid(String message) {
    return new InvalidStatsQueryException(message);
  }
}
