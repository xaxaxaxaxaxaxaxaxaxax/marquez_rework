/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.ManagedDataSource;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
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
 * Opt-in production-worker benchmark for projection batch sizes one and eight.
 *
 * <p>Run with {@code -DrunOpenLineageBatchPipelineThroughputBenchmark=true}. Fixture construction,
 * durable admission, HOT seeding, and {@code VACUUM (ANALYZE)} are outside the measured interval.
 * Only worker drain is timed, and every relational, queue, metric, or health assertion is a
 * benchmark gate.
 *
 * <p>Set {@code openLineageBatchPipelineBenchmark.revision} to emit a stable revision label. A
 * candidate run can additionally set {@code openLineageBatchPipelineBenchmark.baselineResults} to a
 * captured baseline log. Matching workload/batch-size/trial samples are paired, and every cell must
 * retain at least 95% of the baseline's paired-median event throughput.
 */
@Testcontainers
@Tag("IntegrationTests")
@EnabledIfSystemProperty(named = "runOpenLineageBatchPipelineThroughputBenchmark", matches = "true")
public class OpenLineageBatchPipelineThroughputBenchmark {
  private static final int EVENTS_PER_ADMISSION = 8;
  private static final int WARMUP_ADMISSIONS =
      Integer.getInteger("openLineageBatchPipelineBenchmark.warmupAdmissions", 2);
  private static final int ADMISSIONS_PER_CELL =
      Integer.getInteger("openLineageBatchPipelineBenchmark.admissionsPerCell", 16);
  private static final int TRIALS = 7;
  private static final double MINIMUM_BASELINE_RETENTION = 0.95;
  private static final long DRAIN_TIMEOUT_SECONDS =
      Long.getLong("openLineageBatchPipelineBenchmark.drainTimeoutSeconds", 180L);
  private static final String REVISION =
      System.getProperty("openLineageBatchPipelineBenchmark.revision", "unspecified");
  private static final String BASELINE_RESULTS =
      System.getProperty("openLineageBatchPipelineBenchmark.baselineResults", "");
  private static final String SAMPLE_MARKER = "OPENLINEAGE_PROJECTOR_SAMPLE";
  private static final Instant EVENT_TIME = Instant.parse("2026-08-15T00:00:00Z");
  private static final String NAMESPACE = "batch-pipeline-benchmark";
  private static final String PRODUCER = "https://example.com/marquez-batch-pipeline-benchmark";
  private static final URI PRODUCER_URI = URI.create(PRODUCER);
  private static final URI RUN_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/RunEvent");
  private static final URI FACET_SCHEMA =
      URI.create("https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json");
  private static final List<LineageEvent.SchemaField> EIGHT_FIELDS = schemaFields(8);
  private static final List<LineageEvent.SchemaField> THIRTY_TWO_FIELDS = schemaFields(32);
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
        sourceFactory.build(
            new MetricRegistry(), "open-lineage-batch-pipeline-throughput-benchmark");
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
    assertThat(DRAIN_TIMEOUT_SECONDS).isPositive();
    assertThat(REVISION).matches("[A-Za-z0-9._-]+");

    for (Workload workload : Workload.values()) {
      for (int batchSize : List.of(1, 8)) {
        runCell(workload, batchSize, WARMUP_ADMISSIONS, "warmup");
      }
    }

    Map<SampleKey, Sample> samples = new LinkedHashMap<>();
    for (Workload workload : Workload.values()) {
      for (int trial = 1; trial <= TRIALS; trial++) {
        String fixtureKey = "trial-" + trial;
        List<Integer> order = (trial & 1) == 1 ? List.of(1, 8) : List.of(8, 1);
        for (int batchSize : order) {
          TimedCount timing = runCell(workload, batchSize, ADMISSIONS_PER_CELL, fixtureKey);
          Sample sample =
              new Sample(
                  new SampleKey(workload, batchSize, trial),
                  ADMISSIONS_PER_CELL,
                  timing.count(),
                  timing.nanos());
          assertThat(samples.put(sample.key(), sample)).isNull();
          printSample(sample);
        }

        System.out.printf(
            Locale.ROOT,
            "OPENLINEAGE_PROJECTOR_PAIR schema=1 revision=%s workload=%s trial=%d order=%s "
                + "events=%d size1_drain_events_per_second=%.3f "
                + "size8_drain_events_per_second=%.3f paired_size8_over_size1=%.6f%n",
            REVISION,
            workload,
            trial,
            "size" + order.get(0) + ",size" + order.get(1),
            sample(samples, workload, 1, trial).eventCount(),
            sample(samples, workload, 1, trial).perSecond(),
            sample(samples, workload, 8, trial).perSecond(),
            pairedRatio(samples, workload, trial));
      }
    }

    printSummaries(samples);
    if (!BASELINE_RESULTS.isBlank()) {
      assertBaselineRegression(samples, Path.of(BASELINE_RESULTS));
    }
  }

  private TimedCount runCell(
      Workload workload, int projectionBatchSize, int admissionCount, String fixtureKey)
      throws Exception {
    JdbiUtils.cleanDatabase(jdbi);
    Fixture fixture = fixture(workload, fixtureKey, admissionCount);
    int eventCount = Math.multiplyExact(admissionCount, EVENTS_PER_ADMISSION);
    OpenLineageIntake intake =
        new OpenLineageIntake(jdbi.onDemand(OpenLineageQueueDao.class), () -> {});

    if (fixture.hotSeed() != null) {
      projectHotSeed(intake, fixture.hotSeed());
    }

    int admitted = 0;
    for (List<PreparedEvent> admission : fixture.admissions()) {
      admitted += intake.enqueueAll(admission);
    }
    assertThat(admitted).isEqualTo(eventCount);

    vacuumAnalyze();
    assertQueuedFixture(workload, admissionCount, eventCount, fixture.hotSeed() == null ? 0 : 1);

    Drain drain = drain(projectionBatchSize, eventCount, "measured drain");
    WorkerCounts workerCounts = drain.counts();
    long minimumClaims = (eventCount + (long) projectionBatchSize - 1) / projectionBatchSize;
    long maximumSelected =
        Math.multiplyExact((long) projectionBatchSize, workerCounts.nonemptyClaims());
    assertThat(workerCounts.nonemptyClaims()).isBetween(minimumClaims, (long) eventCount);
    assertThat(workerCounts.selected()).isGreaterThanOrEqualTo(workerCounts.nonemptyClaims());
    assertThat(workerCounts.selected()).isLessThanOrEqualTo(maximumSelected);
    assertThat(workerCounts.committedOutcomes()).isLessThanOrEqualTo(workerCounts.selected());
    assertThat(workerCounts.selected()).isEqualTo(eventCount);
    assertThat(workerCounts.committedOutcomes()).isEqualTo(eventCount);
    if (projectionBatchSize == 1) {
      assertThat(workerCounts.nonemptyClaims()).isEqualTo(eventCount);
      assertThat(workerCounts.maximumClaimSize()).isEqualTo(1);
    } else {
      assertThat(workerCounts.maximumClaimSize()).isGreaterThan(1);
      if (workload != Workload.HOT) {
        assertThat(workerCounts.nonemptyClaims()).isEqualTo(eventCount / projectionBatchSize);
      }
    }

    assertProjectedFixture(fixture.expectation());
    TimedCount result = drain.timing();
    System.out.printf(
        Locale.ROOT,
        "OPENLINEAGE_PROJECTOR_CELL schema=1 revision=%s cell=%s workload=%s "
            + "projection_batch_size=%d admissions=%d events=%d drain_millis=%.3f "
            + "drain_events_per_second=%.3f nonempty_claims=%d maximum_claim_size=%d "
            + "selected=%d committed_outcomes=%d%n",
        REVISION,
        fixtureKey + '-' + workload + "-size" + projectionBatchSize,
        workload,
        projectionBatchSize,
        admissionCount,
        eventCount,
        result.nanos() / 1_000_000.0,
        result.perSecond(),
        workerCounts.nonemptyClaims(),
        workerCounts.maximumClaimSize(),
        workerCounts.selected(),
        workerCounts.committedOutcomes());
    return result;
  }

  private void projectHotSeed(OpenLineageIntake intake, LineageEvent seed) throws Exception {
    intake.enqueue(seed);
    Drain drain = drain(1, 1, "unmeasured HOT seed");
    WorkerCounts workerCounts = drain.counts();
    assertThat(workerCounts.nonemptyClaims()).isEqualTo(1);
    assertThat(workerCounts.maximumClaimSize()).isEqualTo(1);
    assertThat(workerCounts.selected()).isEqualTo(1);
    assertThat(workerCounts.committedOutcomes()).isEqualTo(1);
    assertThat(QueueSnapshot.read(jdbi)).isEqualTo(QueueSnapshot.empty());
  }

  private Drain drain(int projectionBatchSize, int eventCount, String phase) throws Exception {
    MetricRegistry metrics = new MetricRegistry();
    OpenLineageWorker worker = newWorker(projectionBatchSize, metrics);
    Meter successes = metrics.meter(workerMetricName("succeeded"));
    long startedAt = System.nanoTime();
    long nanos;
    try {
      worker.start();
      awaitSuccessfulDrain(eventCount, successes, metrics);
      nanos = System.nanoTime() - startedAt;
      assertThat(new OpenLineageWorkerHealthCheck(worker).execute().isHealthy())
          .as("worker health after the %s", phase)
          .isTrue();
    } finally {
      worker.stop();
    }
    assertCleanWorkerMetrics(metrics);
    return new Drain(new TimedCount(eventCount, nanos), workerCounts(metrics));
  }

  private static void assertCleanWorkerMetrics(MetricRegistry metrics) {
    assertNoInvalidatingMeters(metrics);
    assertThat(metrics.meter(workerMetricName("forced_shutdown")).getCount()).isZero();
    assertThat(metrics.meter(workerMetricName("shutdown_incomplete")).getCount()).isZero();
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

  private void awaitSuccessfulDrain(int expectedEvents, Meter successes, MetricRegistry metrics)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DRAIN_TIMEOUT_SECONDS);
    while (successes.getCount() < expectedEvents
        && invalidatingMeterCount(metrics) == 0
        && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(1);
    }
    assertNoInvalidatingMeters(metrics);
    assertThat(successes.getCount())
        .as("events projected before the drain timeout")
        .isEqualTo(expectedEvents);
  }

  private static long invalidatingMeterCount(MetricRegistry metrics) {
    return INVALIDATING_METERS.stream()
        .mapToLong(suffix -> metrics.meter(workerMetricName(suffix)).getCount())
        .sum();
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
        metrics.histogram(workerMetricName("claim_size")).getSnapshot().getMax(),
        metrics.meter(workerMetricName("selected")).getCount(),
        metrics.meter(workerMetricName("succeeded")).getCount(),
        metrics.meter(workerMetricName("retried")).getCount(),
        metrics.meter(workerMetricName("dead_lettered")).getCount());
  }

  private void assertQueuedFixture(
      Workload workload, int admissionCount, int eventCount, long rawEventCount) {
    assertThat(QueueSnapshot.read(jdbi))
        .isEqualTo(
            new QueueSnapshot(
                eventCount,
                expectedQueueHeads(workload, admissionCount),
                0,
                admissionCount,
                0,
                EVENTS_PER_ADMISSION,
                EVENTS_PER_ADMISSION));
    long actualRawEventCount =
        jdbi.withHandle(
            handle ->
                handle.createQuery("SELECT count(*) FROM lineage_events").mapTo(Long.class).one());
    assertThat(actualRawEventCount).isEqualTo(rawEventCount);
  }

  private void assertProjectedFixture(ProjectionCounts expected) {
    assertThat(QueueSnapshot.read(jdbi)).isEqualTo(QueueSnapshot.empty());
    assertThat(
            queryCounts(
                8,
                """
                SELECT (SELECT count(*) FROM lineage_events),
                       (SELECT count(DISTINCT run_uuid) FROM lineage_events),
                       (SELECT count(*) FROM runs),
                       (SELECT count(*) FROM run_states),
                       (SELECT count(*) FROM jobs),
                       (SELECT count(*) FROM datasets),
                       (SELECT count(*) FROM dataset_fields),
                       (SELECT count(*) FROM column_lineage)
                """))
        .as("raw/run/job/dataset projection counts")
        .containsExactly(expected.core());

    boolean authoritativeRunIoInstalled =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery("SELECT to_regclass('open_lineage_run_io_state') IS NOT NULL")
                    .mapTo(Boolean.class)
                    .one());
    if (!authoritativeRunIoInstalled) {
      System.out.printf(
          Locale.ROOT,
          "OPENLINEAGE_PROJECTOR_COMPATIBILITY schema=1 revision=%s "
              + "authoritative_run_io_state=absent legacy_baseline=true%n",
          REVISION);
      return;
    }

    assertThat(
            queryCounts(
                4,
                """
                SELECT (SELECT count(*) FROM open_lineage_run_io_state
                          WHERE io_type = 'INPUT'),
                       (SELECT count(*) FROM open_lineage_run_io_state
                          WHERE io_type = 'OUTPUT'),
                       (SELECT COALESCE(sum(cardinality(dataset_version_uuids)), 0)
                          FROM open_lineage_run_io_state WHERE io_type = 'INPUT'),
                       (SELECT COALESCE(sum(cardinality(dataset_version_uuids)), 0)
                          FROM open_lineage_run_io_state WHERE io_type = 'OUTPUT')
                """))
        .as("authoritative input/output side and occurrence counts")
        .containsExactly(expected.runIo());
  }

  private long[] queryCounts(int count, String sql) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(sql)
                .map(
                    (resultSet, context) -> {
                      long[] values = new long[count];
                      for (int index = 0; index < count; index++) {
                        values[index] = resultSet.getLong(index + 1);
                      }
                      return values;
                    })
                .one());
  }

  private void vacuumAnalyze() {
    jdbi.useHandle(handle -> handle.execute("VACUUM (ANALYZE)"));
  }

  private static Fixture fixture(Workload workload, String fixtureKey, int admissionCount) {
    List<List<PreparedEvent>> admissions = new ArrayList<>(admissionCount);
    LineageEvent hotSeed =
        workload == Workload.HOT
            ? runEvent(
                "COMPLETE",
                0,
                namedRun(fixtureKey, workload, 0, 0),
                "hot-job",
                datasets(workload, 0, true),
                datasets(workload, 0, false))
            : null;
    for (int admission = 0; admission < admissionCount; admission++) {
      List<PreparedEvent> members = new ArrayList<>(EVENTS_PER_ADMISSION);
      boolean modeled = workload == Workload.M1 || workload == Workload.M3;
      List<LineageEvent.Dataset> inputs = modeled ? datasets(workload, admission, true) : null;
      List<LineageEvent.Dataset> outputs = modeled ? datasets(workload, admission, false) : null;
      for (int event = 0; event < EVENTS_PER_ADMISSION; event++) {
        LineageEvent lineageEvent = hotSeed;
        if (lineageEvent == null) {
          lineageEvent =
              runEvent(
                  modeled ? ((event & 1) == 0 ? "START" : "COMPLETE") : null,
                  (long) admission * EVENTS_PER_ADMISSION + event,
                  namedRun(fixtureKey, workload, admission, modeled ? event / 2 : event),
                  modeled
                      ? workload.name().toLowerCase(Locale.ROOT) + "-job-" + admission
                      : "m0-job-" + admission,
                  inputs,
                  outputs);
        }
        members.add(OpenLineageQueueDao.prepare(lineageEvent));
      }
      admissions.add(List.copyOf(members));
    }
    return new Fixture(
        List.copyOf(admissions), hotSeed, expectedProjection(workload, admissionCount));
  }

  private static LineageEvent runEvent(
      String eventType,
      long seconds,
      UUID runId,
      String jobName,
      List<LineageEvent.Dataset> inputs,
      List<LineageEvent.Dataset> outputs) {
    return LineageEvent.builder()
        .eventType(eventType)
        .eventTime(EVENT_TIME.plusSeconds(seconds).atZone(UTC))
        .run(new LineageEvent.Run(runId.toString(), null))
        .job(LineageEvent.Job.builder().namespace(NAMESPACE).name(jobName).build())
        .inputs(inputs)
        .outputs(outputs)
        .producer(PRODUCER)
        .schemaURL(RUN_SCHEMA)
        .build();
  }

  private static List<LineageEvent.Dataset> datasets(
      Workload workload, int admission, boolean input) {
    List<LineageEvent.Dataset> datasets = new ArrayList<>(3);
    String side = input ? "input" : "output";
    String prefix =
        workload == Workload.HOT
            ? "hot-" + side + '-'
            : workload.name().toLowerCase(Locale.ROOT) + '-' + side + '-' + admission + '-';
    LineageEvent.SchemaDatasetFacet schema =
        new LineageEvent.SchemaDatasetFacet(
            PRODUCER_URI, FACET_SCHEMA, workload == Workload.M3 ? THIRTY_TWO_FIELDS : EIGHT_FIELDS);
    for (int dataset = 0; dataset < 3; dataset++) {
      var facets = LineageEvent.DatasetFacets.builder().schema(schema);
      if (workload == Workload.M3 && !input && dataset == 0) {
        facets.columnLineage(columnLineageFacet(workload, admission));
      }
      datasets.add(
          LineageEvent.Dataset.builder()
              .namespace(NAMESPACE)
              .name(prefix + dataset)
              .facets(facets.build())
              .build());
    }
    return List.copyOf(datasets);
  }

  private static LineageEvent.ColumnLineageDatasetFacet columnLineageFacet(
      Workload workload, int admission) {
    String inputName = workload.name().toLowerCase(Locale.ROOT) + "-input-" + admission + "-0";
    Map<String, LineageEvent.ColumnLineageOutputColumn> columns = new LinkedHashMap<>();
    for (int outputField = 0; outputField < 32; outputField++) {
      List<LineageEvent.ColumnLineageInputField> inputs =
          IntStream.range(0, 8)
              .mapToObj(
                  field ->
                      new LineageEvent.ColumnLineageInputField(
                          NAMESPACE, inputName, fieldName(field)))
              .toList();
      columns.put(
          fieldName(outputField),
          new LineageEvent.ColumnLineageOutputColumn(
              inputs, "benchmark expression " + outputField, "SQL"));
    }
    return new LineageEvent.ColumnLineageDatasetFacet(
        PRODUCER_URI, FACET_SCHEMA, new LineageEvent.ColumnLineageDatasetFacetFields(columns));
  }

  private static List<LineageEvent.SchemaField> schemaFields(int count) {
    return IntStream.range(0, count)
        .mapToObj(field -> new LineageEvent.SchemaField(fieldName(field), "STRING", null))
        .toList();
  }

  private static String fieldName(int field) {
    return "field_" + String.format(Locale.ROOT, "%02d", field);
  }

  private static UUID namedRun(
      String fixtureKey, Workload workload, int admission, int runInAdmission) {
    return UUID.nameUUIDFromBytes(
        (fixtureKey + ':' + workload + ':' + admission + ':' + runInAdmission).getBytes(UTF_8));
  }

  private static ProjectionCounts expectedProjection(Workload workload, int admissionCount) {
    long eventCount = Math.multiplyExact((long) admissionCount, EVENTS_PER_ADMISSION);
    if (workload == Workload.M0) {
      return new ProjectionCounts(
          new long[] {eventCount, eventCount, eventCount, 0, admissionCount, 0, 0, 0},
          new long[] {0, 0, 0, 0});
    }
    if (workload == Workload.HOT) {
      return new ProjectionCounts(
          new long[] {eventCount + 1, 1, 1, eventCount + 1, 1, 6, 48, 0}, new long[] {1, 1, 3, 3});
    }

    long runs = Math.multiplyExact((long) admissionCount, EVENTS_PER_ADMISSION / 2);
    long fieldsPerDataset = workload == Workload.M3 ? 32 : 8;
    long columnEdges = workload == Workload.M3 ? Math.multiplyExact(runs, 256L) : 0;
    long datasets = Math.multiplyExact((long) admissionCount, 6);
    return new ProjectionCounts(
        new long[] {
          eventCount,
          runs,
          runs,
          eventCount,
          admissionCount,
          datasets,
          Math.multiplyExact(datasets, fieldsPerDataset),
          columnEdges
        },
        new long[] {runs, runs, Math.multiplyExact(runs, 3), Math.multiplyExact(runs, 3)});
  }

  private static long expectedQueueHeads(Workload workload, int admissionCount) {
    return switch (workload) {
      case M0 -> Math.multiplyExact((long) admissionCount, EVENTS_PER_ADMISSION);
      case M1, M3 -> Math.multiplyExact((long) admissionCount, EVENTS_PER_ADMISSION / 2);
      case HOT -> 1;
    };
  }

  private static void printSample(Sample sample) {
    System.out.printf(
        Locale.ROOT,
        SAMPLE_MARKER
            + " schema=1 revision=%s workload=%s trial=%d projection_batch_size=%d "
            + "admissions=%d events=%d drain_nanos=%d drain_events_per_second=%.3f%n",
        REVISION,
        sample.key().workload(),
        sample.key().trial(),
        sample.key().projectionBatchSize(),
        sample.admissionCount(),
        sample.eventCount(),
        sample.nanos(),
        sample.perSecond());
  }

  private static void printSummaries(Map<SampleKey, Sample> samples) {
    for (Workload workload : Workload.values()) {
      List<Double> size1 = new ArrayList<>(TRIALS);
      List<Double> size8 = new ArrayList<>(TRIALS);
      List<Double> pairedRatios = new ArrayList<>(TRIALS);
      int size8Wins = 0;
      for (int trial = 1; trial <= TRIALS; trial++) {
        size1.add(sample(samples, workload, 1, trial).perSecond());
        size8.add(sample(samples, workload, 8, trial).perSecond());
        double ratio = pairedRatio(samples, workload, trial);
        pairedRatios.add(ratio);
        if (ratio > 1.0) {
          size8Wins++;
        }
      }

      Distribution size1Distribution = Distribution.of(size1);
      Distribution size8Distribution = Distribution.of(size8);
      Distribution ratioDistribution = Distribution.of(pairedRatios);
      System.out.printf(
          Locale.ROOT,
          "OPENLINEAGE_PROJECTOR_SUMMARY schema=1 revision=%s workload=%s "
              + "admissions_per_cell=%d events_per_cell=%d trials=%d "
              + "size1_median_events_per_second=%.3f size1_range=%.3f..%.3f "
              + "size8_median_events_per_second=%.3f size8_range=%.3f..%.3f "
              + "paired_size8_over_size1_median=%.6f paired_range=%.6f..%.6f "
              + "size8_wins=%d/%d%n",
          REVISION,
          workload,
          ADMISSIONS_PER_CELL,
          Math.multiplyExact(ADMISSIONS_PER_CELL, EVENTS_PER_ADMISSION),
          TRIALS,
          size1Distribution.median(),
          size1Distribution.minimum(),
          size1Distribution.maximum(),
          size8Distribution.median(),
          size8Distribution.minimum(),
          size8Distribution.maximum(),
          ratioDistribution.median(),
          ratioDistribution.minimum(),
          ratioDistribution.maximum(),
          size8Wins,
          TRIALS);
    }
  }

  private static double pairedRatio(Map<SampleKey, Sample> samples, Workload workload, int trial) {
    return sample(samples, workload, 8, trial).perSecond()
        / sample(samples, workload, 1, trial).perSecond();
  }

  private static Sample sample(
      Map<SampleKey, Sample> samples, Workload workload, int projectionBatchSize, int trial) {
    SampleKey key = new SampleKey(workload, projectionBatchSize, trial);
    return requireNonNull(samples.get(key), "Missing benchmark sample " + key);
  }

  private static void assertBaselineRegression(Map<SampleKey, Sample> candidates, Path baselinePath)
      throws Exception {
    Map<SampleKey, Sample> baselines = readBaselineSamples(baselinePath);
    for (Workload workload : Workload.values()) {
      for (int projectionBatchSize : List.of(1, 8)) {
        List<Double> pairedRatios = new ArrayList<>(TRIALS);
        for (int trial = 1; trial <= TRIALS; trial++) {
          SampleKey key = new SampleKey(workload, projectionBatchSize, trial);
          Sample candidate = sample(candidates, workload, projectionBatchSize, trial);
          Sample baseline = baselines.get(key);
          assertThat(baseline).as("baseline sample for %s in %s", key, baselinePath).isNotNull();
          assertThat(candidate.admissionCount()).isEqualTo(baseline.admissionCount());
          assertThat(candidate.eventCount()).isEqualTo(baseline.eventCount());
          pairedRatios.add(candidate.perSecond() / baseline.perSecond());
        }
        Distribution distribution = Distribution.of(pairedRatios);
        System.out.printf(
            Locale.ROOT,
            "OPENLINEAGE_PROJECTOR_REGRESSION schema=1 revision=%s workload=%s "
                + "projection_batch_size=%d baseline_file=%s trials=%d "
                + "paired_candidate_over_baseline_median=%.6f paired_range=%.6f..%.6f "
                + "minimum_allowed=%.6f verdict=%s%n",
            REVISION,
            workload,
            projectionBatchSize,
            baselinePath,
            TRIALS,
            distribution.median(),
            distribution.minimum(),
            distribution.maximum(),
            MINIMUM_BASELINE_RETENTION,
            distribution.median() >= MINIMUM_BASELINE_RETENTION ? "PASS" : "FAIL");
        assertThat(distribution.median())
            .as(
                "%s batch size %d paired-median throughput retention against %s",
                workload, projectionBatchSize, baselinePath)
            .isGreaterThanOrEqualTo(MINIMUM_BASELINE_RETENTION);
      }
    }
  }

  private static Map<SampleKey, Sample> readBaselineSamples(Path baselinePath) throws Exception {
    Map<SampleKey, Sample> samples = new HashMap<>();
    for (String line : Files.readAllLines(baselinePath, UTF_8)) {
      int marker = line.indexOf(SAMPLE_MARKER);
      if (marker < 0) {
        continue;
      }
      Map<String, String> values = keyValues(line.substring(marker + SAMPLE_MARKER.length()));
      assertThat(values.get("schema")).as("sample schema in %s", line).isEqualTo("1");
      SampleKey key =
          new SampleKey(
              Workload.valueOf(required(values, "workload", line)),
              Integer.parseInt(required(values, "projection_batch_size", line)),
              Integer.parseInt(required(values, "trial", line)));
      Sample baseline =
          new Sample(
              key,
              Integer.parseInt(required(values, "admissions", line)),
              Integer.parseInt(required(values, "events", line)),
              Long.parseLong(required(values, "drain_nanos", line)));
      assertThat(samples.put(key, baseline))
          .as("duplicate baseline sample %s in %s", key, baselinePath)
          .isNull();
    }
    assertThat(samples).as("baseline samples in %s", baselinePath).isNotEmpty();
    return samples;
  }

  private static Map<String, String> keyValues(String sample) {
    Map<String, String> values = new HashMap<>();
    for (String token : sample.trim().split("\\s+")) {
      int separator = token.indexOf('=');
      if (separator > 0) {
        values.put(token.substring(0, separator), token.substring(separator + 1));
      }
    }
    return values;
  }

  private static String required(Map<String, String> values, String name, String line) {
    String value = values.get(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing " + name + " in benchmark sample: " + line);
    }
    return value;
  }

  private static String workerMetricName(String suffix) {
    return MetricRegistry.name(OpenLineageWorker.class, suffix);
  }

  private enum Workload {
    M0,
    M1,
    M3,
    HOT
  }

  private record Fixture(
      List<List<PreparedEvent>> admissions, LineageEvent hotSeed, ProjectionCounts expectation) {}

  private record ProjectionCounts(long[] core, long[] runIo) {}

  private record WorkerCounts(
      long nonemptyClaims,
      long maximumClaimSize,
      long selected,
      long succeeded,
      long retried,
      long deadLettered) {
    private long committedOutcomes() {
      return Math.addExact(Math.addExact(succeeded, retried), deadLettered);
    }
  }

  private record Drain(TimedCount timing, WorkerCounts counts) {}

  private record SampleKey(Workload workload, int projectionBatchSize, int trial) {}

  private record Sample(SampleKey key, int admissionCount, int eventCount, long nanos) {
    private double perSecond() {
      return eventCount * 1_000_000_000.0 / nanos;
    }
  }
}
