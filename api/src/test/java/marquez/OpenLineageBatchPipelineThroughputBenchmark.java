/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.ManagedDataSource;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import marquez.api.JdbiUtils;
import marquez.common.Utils;
import marquez.db.DbMigration;
import marquez.db.FlywayFactory;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageQueueDao;
import marquez.db.OpenLineageQueueDao.PreparedEvent;
import marquez.service.OpenLineageConfig;
import marquez.service.OpenLineageIntake;
import marquez.service.OpenLineageService;
import marquez.service.OpenLineageWorker;
import marquez.service.OpenLineageWorkerHealthCheck;
import marquez.service.RunService;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.jackson2.Jackson2Config;
import org.jdbi.v3.jackson2.Jackson2Plugin;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Opt-in production-pipeline benchmark for projection batch sizes one and eight.
 *
 * <p>Run with {@code -DrunOpenLineageBatchPipelineThroughputBenchmark=true}. Deterministic paired
 * fixtures are admitted outside the measured interval. Only projector drain is timed, and every
 * correctness or worker-health assertion is a benchmark gate.
 */
@Testcontainers
@Tag("IntegrationTests")
@EnabledIfSystemProperty(named = "runOpenLineageBatchPipelineThroughputBenchmark", matches = "true")
public class OpenLineageBatchPipelineThroughputBenchmark {
  private static final int EVENTS_PER_ADMISSION = 8;
  private static final int WARMUP_ADMISSIONS =
      Integer.getInteger("openLineageBatchPipelineBenchmark.warmupAdmissions", 16);
  private static final int ADMISSIONS_PER_CELL =
      Integer.getInteger("openLineageBatchPipelineBenchmark.admissionsPerCell", 64);
  private static final int TRIALS =
      Integer.getInteger("openLineageBatchPipelineBenchmark.trials", 7);
  private static final long DRAIN_TIMEOUT_SECONDS =
      Long.getLong("openLineageBatchPipelineBenchmark.drainTimeoutSeconds", 180L);
  private static final Instant EVENT_TIME = Instant.parse("2026-08-15T00:00:00Z");
  private static final String NAMESPACE = "batch-pipeline-benchmark";
  private static final String PRODUCER = "https://example.com/marquez-batch-pipeline-benchmark";
  private static final URI RUN_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/RunEvent");
  private static final List<String> INVALIDATING_METERS =
      List.of(
          "retried",
          "dead_lettered",
          "batch_fallback",
          "poll_failed",
          "coordinator_failed",
          "task_failed",
          "state_transition_failed",
          "post_commit_failed");

  @Container
  private static final PostgresContainer POSTGRES =
      PostgresContainer.create("open-lineage-batch-pipeline-throughput-benchmark");

  private static ManagedDataSource dataSource;
  private static Jdbi jdbi;

  @BeforeAll
  static void setUpDatabase() throws Exception {
    DataSourceFactory sourceFactory = new DataSourceFactory();
    sourceFactory.setDriverClass("org.postgresql.Driver");
    sourceFactory.setUrl(POSTGRES.getJdbcUrl());
    sourceFactory.setUser(POSTGRES.getUsername());
    sourceFactory.setPassword(POSTGRES.getPassword());
    sourceFactory.setInitialSize(1);
    sourceFactory.setMinSize(1);
    sourceFactory.setMaxSize(1);
    sourceFactory.setDefaultTransactionIsolation(
        DataSourceFactory.TransactionIsolation.READ_COMMITTED);

    ManagedDataSource configuredDataSource =
        sourceFactory.build(new MetricRegistry(), "open-lineage-batch-pipeline-benchmark");
    try {
      configuredDataSource.start();
      DbMigration.migrateDbOrError(new FlywayFactory(), configuredDataSource, true);
      Jdbi configuredJdbi =
          Jdbi.create(configuredDataSource)
              .installPlugin(new SqlObjectPlugin())
              .installPlugin(new PostgresPlugin())
              .installPlugin(new Jackson2Plugin());
      configuredJdbi.getConfig(Jackson2Config.class).setMapper(Utils.getMapper());
      dataSource = configuredDataSource;
      jdbi = configuredJdbi;
    } catch (Exception | Error failure) {
      try {
        configuredDataSource.stop();
      } catch (Exception stopFailure) {
        failure.addSuppressed(stopFailure);
      }
      throw failure;
    }
  }

  @AfterAll
  static void tearDownDatabase() throws Exception {
    if (dataSource != null) {
      dataSource.stop();
    }
  }

  @Test
  void compareProjectionBatchSizesOneAndEight() throws Exception {
    assertThat(WARMUP_ADMISSIONS).isPositive();
    assertThat(ADMISSIONS_PER_CELL).isPositive();
    assertThat(TRIALS).isPositive();
    assertThat(DRAIN_TIMEOUT_SECONDS).isPositive();

    runCell(1, WARMUP_ADMISSIONS, "warmup", "warmup-size1");
    runCell(8, WARMUP_ADMISSIONS, "warmup", "warmup-size8");

    List<TimedCount> size1Results = new ArrayList<>(TRIALS);
    List<TimedCount> size8Results = new ArrayList<>(TRIALS);
    List<Double> pairedDrainSpeedups = new ArrayList<>(TRIALS);
    int size8FasterPairs = 0;
    for (int trial = 1; trial <= TRIALS; trial++) {
      String fixtureKey = "trial-" + trial;
      String order;
      TimedCount size1;
      TimedCount size8;
      if ((trial & 1) == 1) {
        order = "size1,size8";
        size1 = runCell(1, ADMISSIONS_PER_CELL, fixtureKey, fixtureKey + "-size1");
        size8 = runCell(8, ADMISSIONS_PER_CELL, fixtureKey, fixtureKey + "-size8");
      } else {
        order = "size8,size1";
        size8 = runCell(8, ADMISSIONS_PER_CELL, fixtureKey, fixtureKey + "-size8");
        size1 = runCell(1, ADMISSIONS_PER_CELL, fixtureKey, fixtureKey + "-size1");
      }

      double pairedDrainSpeedup = size8.perSecond() / size1.perSecond();
      if (pairedDrainSpeedup > 1.0) {
        size8FasterPairs++;
      }
      size1Results.add(size1);
      size8Results.add(size8);
      pairedDrainSpeedups.add(pairedDrainSpeedup);

      System.out.printf(
          Locale.ROOT,
          "OPENLINEAGE_BATCH_PIPELINE_PAIR trial=%d order=%s events=%d "
              + "size1_drain_events_per_second=%.1f size8_drain_events_per_second=%.1f "
              + "paired_drain_speedup=%.3fx%n",
          trial,
          order,
          size1.count(),
          size1.perSecond(),
          size8.perSecond(),
          pairedDrainSpeedup);
    }

    List<Double> size1DrainThroughputs = size1Results.stream().map(TimedCount::perSecond).toList();
    List<Double> size8DrainThroughputs = size8Results.stream().map(TimedCount::perSecond).toList();
    Distribution size1Distribution = Distribution.of(size1DrainThroughputs);
    Distribution size8Distribution = Distribution.of(size8DrainThroughputs);
    Distribution speedupDistribution = Distribution.of(pairedDrainSpeedups);

    System.out.printf(
        Locale.ROOT,
        "OPENLINEAGE_BATCH_PIPELINE_SUMMARY admissions_per_cell=%d events_per_cell=%d trials=%d "
            + "size1_drain_median_events_per_second=%.1f size1_drain_range=%.1f..%.1f "
            + "size8_drain_median_events_per_second=%.1f size8_drain_range=%.1f..%.1f "
            + "paired_drain_median_speedup=%.3fx paired_drain_range=%.3f..%.3f "
            + "size8_faster_pairs=%d/%d%n",
        ADMISSIONS_PER_CELL,
        Math.multiplyExact(ADMISSIONS_PER_CELL, EVENTS_PER_ADMISSION),
        TRIALS,
        size1Distribution.median(),
        size1Distribution.minimum(),
        size1Distribution.maximum(),
        size8Distribution.median(),
        size8Distribution.minimum(),
        size8Distribution.maximum(),
        speedupDistribution.median(),
        speedupDistribution.minimum(),
        speedupDistribution.maximum(),
        size8FasterPairs,
        TRIALS);
  }

  private TimedCount runCell(
      int projectionBatchSize, int admissionCount, String fixtureKey, String cellName)
      throws Exception {
    JdbiUtils.cleanDatabase(jdbi);
    List<List<PreparedEvent>> admissions = preparedAdmissions(fixtureKey, admissionCount);
    int eventCount = Math.multiplyExact(admissionCount, EVENTS_PER_ADMISSION);
    OpenLineageIntake intake =
        new OpenLineageIntake(jdbi.onDemand(OpenLineageQueueDao.class), () -> {});

    int admitted = 0;
    for (List<PreparedEvent> admission : admissions) {
      admitted += intake.enqueueAll(admission);
    }
    assertThat(admitted).isEqualTo(eventCount);

    vacuumAnalyze();
    assertQueuedFixture(admissionCount, eventCount);

    MetricRegistry metrics = new MetricRegistry();
    OpenLineageWorker worker = newWorker(projectionBatchSize, metrics);
    Meter successes = metrics.meter(workerMetricName("succeeded"));
    List<Meter> invalidatingMeters =
        INVALIDATING_METERS.stream()
            .map(suffix -> metrics.meter(workerMetricName(suffix)))
            .toList();
    long drainNanos;
    try {
      long drainStartedAt = System.nanoTime();
      worker.start();
      awaitSuccessfulDrain(eventCount, successes, invalidatingMeters, metrics);
      drainNanos = System.nanoTime() - drainStartedAt;
      assertThat(new OpenLineageWorkerHealthCheck(worker).execute().isHealthy())
          .as("worker health after the measured drain")
          .isTrue();
    } finally {
      worker.stop();
    }

    assertNoInvalidatingMeters(metrics);
    assertThat(metrics.meter(workerMetricName("forced_shutdown")).getCount()).isZero();
    assertThat(metrics.meter(workerMetricName("shutdown_incomplete")).getCount()).isZero();

    WorkerCounts workerCounts = workerCounts(metrics);
    long maximumSelected =
        Math.multiplyExact((long) projectionBatchSize, workerCounts.nonemptyClaims());
    assertThat(workerCounts.nonemptyClaims()).isPositive();
    assertThat(workerCounts.nonemptyClaims()).isEqualTo(eventCount / projectionBatchSize);
    assertThat(workerCounts.selected()).isGreaterThanOrEqualTo(workerCounts.nonemptyClaims());
    assertThat(workerCounts.selected()).isLessThanOrEqualTo(maximumSelected);
    assertThat(workerCounts.committedOutcomes()).isLessThanOrEqualTo(workerCounts.selected());
    assertThat(workerCounts.selected()).isEqualTo(eventCount);
    assertThat(workerCounts.committedOutcomes()).isEqualTo(eventCount);
    assertThat(workerCounts.selected()).isEqualTo(maximumSelected);

    assertProjectedFixture(admissionCount, eventCount);
    TimedCount result = new TimedCount(eventCount, drainNanos);
    System.out.printf(
        Locale.ROOT,
        "OPENLINEAGE_BATCH_PIPELINE_CELL cell=%s projection_batch_size=%d admissions=%d "
            + "events=%d drain_millis=%.3f drain_events_per_second=%.1f "
            + "nonempty_claims=%d selected=%d committed_outcomes=%d%n",
        cellName,
        projectionBatchSize,
        admissionCount,
        eventCount,
        nanosToMillis(drainNanos),
        result.perSecond(),
        workerCounts.nonemptyClaims(),
        workerCounts.selected(),
        workerCounts.committedOutcomes());
    return result;
  }

  private OpenLineageWorker newWorker(int projectionBatchSize, MetricRegistry metrics)
      throws Exception {
    OpenLineageDao baseDao = jdbi.onDemand(OpenLineageDao.class);
    RunService runService = new RunService(baseDao, List.of());
    OpenLineageService service = new OpenLineageService(baseDao, runService, Runnable::run);
    return new OpenLineageWorker(jdbi, service, workerConfig(projectionBatchSize), metrics);
  }

  private static OpenLineageConfig workerConfig(int projectionBatchSize) throws Exception {
    return Utils.newObjectMapper()
        .readValue(
            """
            {
              "workerThreads": 1,
              "projectionBatchSize": %d,
              "pollIntervalMillis": 10,
              "maxAttempts": 3,
              "retryInitialDelayMillis": 1000,
              "retryMaxDelayMillis": 1000,
              "shutdownGracePeriodMillis": 30000
            }
            """
                .formatted(projectionBatchSize),
            OpenLineageConfig.class);
  }

  private void awaitSuccessfulDrain(
      int expectedEvents, Meter successes, List<Meter> invalidatingMeters, MetricRegistry metrics)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DRAIN_TIMEOUT_SECONDS);
    while (successes.getCount() < expectedEvents
        && invalidatingMeterCount(invalidatingMeters) == 0
        && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(1);
    }
    assertNoInvalidatingMeters(metrics);
    assertThat(successes.getCount())
        .as("events projected before the drain timeout")
        .isEqualTo(expectedEvents);
  }

  private static long invalidatingMeterCount(List<Meter> meters) {
    long count = 0;
    for (Meter meter : meters) {
      count += meter.getCount();
    }
    return count;
  }

  private static void assertNoInvalidatingMeters(MetricRegistry metrics) {
    for (String suffix : INVALIDATING_METERS) {
      assertThat(metrics.meter(workerMetricName(suffix)).getCount())
          .as("OpenLineage worker %s meter", suffix)
          .isZero();
    }
  }

  private static WorkerCounts workerCounts(MetricRegistry metrics) {
    return new WorkerCounts(
        metrics.histogram(workerMetricName("claim_size")).getCount(),
        metrics.meter(workerMetricName("selected")).getCount(),
        metrics.meter(workerMetricName("succeeded")).getCount(),
        metrics.meter(workerMetricName("retried")).getCount(),
        metrics.meter(workerMetricName("dead_lettered")).getCount());
  }

  private void assertQueuedFixture(int admissionCount, int eventCount) {
    assertThat(QueueSnapshot.read(jdbi))
        .isEqualTo(QueueSnapshot.batch(eventCount, admissionCount, EVENTS_PER_ADMISSION));
    long rawEventCount =
        jdbi.withHandle(
            handle ->
                handle.createQuery("SELECT count(*) FROM lineage_events").mapTo(Long.class).one());
    assertThat(rawEventCount).isZero();
  }

  private void assertProjectedFixture(int admissionCount, int eventCount) {
    assertThat(QueueSnapshot.read(jdbi)).isEqualTo(QueueSnapshot.empty());
    ProjectionCounts counts =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT (SELECT count(*) FROM lineage_events) AS raw_events,
                               (SELECT count(DISTINCT run_uuid) FROM lineage_events)
                                   AS distinct_raw_runs,
                               (SELECT count(*) FROM runs) AS runs,
                               (SELECT count(*) FROM jobs) AS jobs,
                               (SELECT count(*) FROM datasets) AS datasets
                        """)
                    .map(
                        (resultSet, context) ->
                            new ProjectionCounts(
                                resultSet.getLong("raw_events"),
                                resultSet.getLong("distinct_raw_runs"),
                                resultSet.getLong("runs"),
                                resultSet.getLong("jobs"),
                                resultSet.getLong("datasets")))
                    .one());
    assertThat(counts)
        .isEqualTo(
            new ProjectionCounts(
                eventCount,
                eventCount,
                eventCount,
                admissionCount,
                Math.multiplyExact(admissionCount, 4)));
  }

  private void vacuumAnalyze() {
    jdbi.useHandle(handle -> handle.execute("VACUUM (ANALYZE)"));
  }

  private static List<List<PreparedEvent>> preparedAdmissions(
      String fixtureKey, int admissionCount) {
    List<List<PreparedEvent>> admissions = new ArrayList<>(admissionCount);
    for (int admission = 0; admission < admissionCount; admission++) {
      List<LineageEvent.Dataset> inputs =
          List.of(dataset("input-a-" + admission), dataset("input-b-" + admission));
      List<LineageEvent.Dataset> outputs =
          List.of(dataset("output-a-" + admission), dataset("output-b-" + admission));
      List<PreparedEvent> members = new ArrayList<>(EVENTS_PER_ADMISSION);
      for (int event = 0; event < EVENTS_PER_ADMISSION; event++) {
        UUID runId =
            UUID.nameUUIDFromBytes((fixtureKey + ':' + admission + ':' + event).getBytes(UTF_8));
        LineageEvent lineageEvent =
            LineageEvent.builder()
                .eventType("COMPLETE")
                .eventTime(
                    EVENT_TIME
                        .plusSeconds((long) admission * EVENTS_PER_ADMISSION + event)
                        .atZone(UTC))
                .run(new LineageEvent.Run(runId.toString(), null))
                .job(
                    LineageEvent.Job.builder()
                        .namespace(NAMESPACE)
                        .name("job-" + admission)
                        .build())
                .inputs(inputs)
                .outputs(outputs)
                .producer(PRODUCER)
                .schemaURL(RUN_SCHEMA)
                .build();
        members.add(OpenLineageQueueDao.prepare(lineageEvent));
      }
      admissions.add(List.copyOf(members));
    }
    return List.copyOf(admissions);
  }

  private static LineageEvent.Dataset dataset(String name) {
    return LineageEvent.Dataset.builder().namespace(NAMESPACE).name(name).build();
  }

  private static String workerMetricName(String suffix) {
    return MetricRegistry.name(OpenLineageWorker.class, suffix);
  }

  private static double nanosToMillis(long nanos) {
    return nanos / 1_000_000.0;
  }

  private record ProjectionCounts(
      long rawEvents, long distinctRawRuns, long runs, long jobs, long datasets) {}

  private record WorkerCounts(
      long nonemptyClaims, long selected, long succeeded, long retried, long deadLettered) {
    private long committedOutcomes() {
      return Math.addExact(Math.addExact(succeeded, retried), deadLettered);
    }
  }
}
