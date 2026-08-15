/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import marquez.api.JdbiUtils;
import marquez.common.Utils;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageQueueDao;
import marquez.db.OpenLineageQueueDao.PreparedEvent;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.OpenLineageConfig;
import marquez.service.OpenLineageIntake;
import marquez.service.OpenLineageService;
import marquez.service.OpenLineageWorker;
import marquez.service.RunService;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Opt-in production-pipeline benchmark for projection batch sizes one and eight.
 *
 * <p>Run with {@code -DrunOpenLineageBatchPipelineThroughputBenchmark=true}. The fixture is built
 * outside both measured intervals. Admission and drain are timed separately so faster durable
 * intake cannot conceal projection cost. Every correctness assertion is also a benchmark gate.
 */
@Tag("IntegrationTests")
@EnabledIfSystemProperty(named = "runOpenLineageBatchPipelineThroughputBenchmark", matches = "true")
@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
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

  private Jdbi jdbi;

  @BeforeEach
  void setUp(Jdbi configuredJdbi) {
    jdbi = configuredJdbi;
    JdbiUtils.cleanDatabase(jdbi);
  }

  @AfterEach
  void tearDown() {
    JdbiUtils.cleanDatabase(jdbi);
  }

  @Test
  void compareProjectionBatchSizesOneAndEight() throws Exception {
    assertThat(WARMUP_ADMISSIONS).isPositive();
    assertThat(ADMISSIONS_PER_CELL).isPositive();
    assertThat(TRIALS).isPositive();
    assertThat(DRAIN_TIMEOUT_SECONDS).isPositive();

    runCell(1, WARMUP_ADMISSIONS, "warmup", "warmup-size1");
    runCell(8, WARMUP_ADMISSIONS, "warmup", "warmup-size8");

    List<CellResult> size1Results = new ArrayList<>(TRIALS);
    List<CellResult> size8Results = new ArrayList<>(TRIALS);
    List<Double> pairedDrainSpeedups = new ArrayList<>(TRIALS);
    int size8FasterPairs = 0;
    for (int trial = 1; trial <= TRIALS; trial++) {
      String fixtureKey = "trial-" + trial;
      String order;
      CellResult size1;
      CellResult size8;
      if ((trial & 1) == 1) {
        order = "size1,size8";
        size1 = runCell(1, ADMISSIONS_PER_CELL, fixtureKey, fixtureKey + "-size1");
        size8 = runCell(8, ADMISSIONS_PER_CELL, fixtureKey, fixtureKey + "-size8");
      } else {
        order = "size8,size1";
        size8 = runCell(8, ADMISSIONS_PER_CELL, fixtureKey, fixtureKey + "-size8");
        size1 = runCell(1, ADMISSIONS_PER_CELL, fixtureKey, fixtureKey + "-size1");
      }

      double pairedDrainSpeedup = size8.drainEventsPerSecond() / size1.drainEventsPerSecond();
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
          size1.eventCount(),
          size1.drainEventsPerSecond(),
          size8.drainEventsPerSecond(),
          pairedDrainSpeedup);
    }

    List<Double> size1AdmissionThroughputs =
        size1Results.stream().map(CellResult::admissionEventsPerSecond).toList();
    List<Double> size8AdmissionThroughputs =
        size8Results.stream().map(CellResult::admissionEventsPerSecond).toList();
    List<Double> size1DrainThroughputs =
        size1Results.stream().map(CellResult::drainEventsPerSecond).toList();
    List<Double> size8DrainThroughputs =
        size8Results.stream().map(CellResult::drainEventsPerSecond).toList();
    List<Double> size1PipelineThroughputs =
        size1Results.stream().map(CellResult::sequentialPipelineEventsPerSecond).toList();
    List<Double> size8PipelineThroughputs =
        size8Results.stream().map(CellResult::sequentialPipelineEventsPerSecond).toList();

    System.out.printf(
        Locale.ROOT,
        "OPENLINEAGE_BATCH_PIPELINE_SUMMARY admissions_per_cell=%d events_per_cell=%d trials=%d "
            + "size1_admission_median_events_per_second=%.1f "
            + "size8_admission_median_events_per_second=%.1f "
            + "size1_drain_median_events_per_second=%.1f size1_drain_range=%.1f..%.1f "
            + "size8_drain_median_events_per_second=%.1f size8_drain_range=%.1f..%.1f "
            + "size1_sequential_pipeline_median_events_per_second=%.1f "
            + "size8_sequential_pipeline_median_events_per_second=%.1f "
            + "paired_drain_median_speedup=%.3fx paired_drain_range=%.3f..%.3f "
            + "size8_faster_pairs=%d/%d%n",
        ADMISSIONS_PER_CELL,
        Math.multiplyExact(ADMISSIONS_PER_CELL, EVENTS_PER_ADMISSION),
        TRIALS,
        median(size1AdmissionThroughputs),
        median(size8AdmissionThroughputs),
        median(size1DrainThroughputs),
        minimum(size1DrainThroughputs),
        maximum(size1DrainThroughputs),
        median(size8DrainThroughputs),
        minimum(size8DrainThroughputs),
        maximum(size8DrainThroughputs),
        median(size1PipelineThroughputs),
        median(size8PipelineThroughputs),
        median(pairedDrainSpeedups),
        minimum(pairedDrainSpeedups),
        maximum(pairedDrainSpeedups),
        size8FasterPairs,
        TRIALS);
  }

  private CellResult runCell(
      int projectionBatchSize, int admissionCount, String fixtureKey, String cellName)
      throws Exception {
    JdbiUtils.cleanDatabase(jdbi);
    List<List<PreparedEvent>> admissions = preparedAdmissions(fixtureKey, admissionCount);
    List<UUID> expectedRunIds = expectedRunIds(admissions);
    int eventCount = Math.multiplyExact(admissionCount, EVENTS_PER_ADMISSION);
    OpenLineageIntake intake =
        new OpenLineageIntake(jdbi.onDemand(OpenLineageQueueDao.class), () -> {});

    String beforeAdmissionLsn = currentWalLsn();
    long admissionStartedAt = System.nanoTime();
    int admitted = 0;
    for (List<PreparedEvent> admission : admissions) {
      admitted += intake.enqueueAll(admission);
    }
    long admissionNanos = System.nanoTime() - admissionStartedAt;
    String afterAdmissionLsn = currentWalLsn();

    assertThat(admitted).isEqualTo(eventCount);
    assertQueuedFixture(admissionCount, expectedRunIds);

    MetricRegistry metrics = new MetricRegistry();
    OpenLineageWorker worker = newWorker(projectionBatchSize, metrics);
    Meter successes = metrics.meter(workerMetricName("succeeded"));
    Meter retries = metrics.meter(workerMetricName("retried"));
    Meter deadLetters = metrics.meter(workerMetricName("dead_lettered"));
    long drainNanos;
    try {
      long drainStartedAt = System.nanoTime();
      worker.start();
      awaitSuccessfulDrain(eventCount, successes, retries, deadLetters);
      drainNanos = System.nanoTime() - drainStartedAt;
    } finally {
      worker.stop();
    }
    String afterDrainLsn = currentWalLsn();

    assertProjectedFixture(admissionCount, eventCount);
    Histogram claimSize = metrics.histogram(workerMetricName("claim_size"));
    long expectedClaims = projectionBatchSize == 1 ? eventCount : admissionCount;
    assertThat(claimSize.getCount()).isEqualTo(expectedClaims);
    assertThat(claimSize.getSnapshot().getMin()).isEqualTo(projectionBatchSize);
    assertThat(claimSize.getSnapshot().getMax()).isEqualTo(projectionBatchSize);
    assertThat(metrics.meter(workerMetricName("selected")).getCount()).isEqualTo(eventCount);
    assertThat(successes.getCount()).isEqualTo(eventCount);
    assertThat(retries.getCount()).isZero();
    assertThat(deadLetters.getCount()).isZero();
    assertThat(metrics.meter(workerMetricName("batch_fallback")).getCount()).isZero();

    CellResult result =
        new CellResult(
            projectionBatchSize,
            admissionCount,
            eventCount,
            admissionNanos,
            drainNanos,
            admissionCount,
            expectedClaims,
            walBytes(beforeAdmissionLsn, afterAdmissionLsn),
            walBytes(afterAdmissionLsn, afterDrainLsn));
    System.out.printf(
        Locale.ROOT,
        "OPENLINEAGE_BATCH_PIPELINE_CELL cell=%s projection_batch_size=%d admissions=%d "
            + "events=%d admission_millis=%.3f admission_events_per_second=%.1f "
            + "drain_millis=%.3f drain_events_per_second=%.1f "
            + "sequential_pipeline_events_per_second=%.1f admission_transactions=%d "
            + "successful_projection_claim_transactions=%d admission_wal_bytes=%d "
            + "drain_wal_bytes=%d%n",
        cellName,
        projectionBatchSize,
        admissionCount,
        eventCount,
        nanosToMillis(admissionNanos),
        result.admissionEventsPerSecond(),
        nanosToMillis(drainNanos),
        result.drainEventsPerSecond(),
        result.sequentialPipelineEventsPerSecond(),
        result.admissionTransactions(),
        result.successfulProjectionClaimTransactions(),
        result.admissionWalBytes(),
        result.drainWalBytes());
    return result;
  }

  private OpenLineageWorker newWorker(int projectionBatchSize, MetricRegistry metrics)
      throws Exception {
    OpenLineageDao baseDao = jdbi.onDemand(OpenLineageDao.class);
    RunService runService = new RunService(baseDao, List.of());
    OpenLineageService service = new OpenLineageService(baseDao, runService, Runnable::run);
    return new OpenLineageWorker(
        jdbi,
        jdbi.onDemand(OpenLineageQueueDao.class),
        service,
        workerConfig(projectionBatchSize),
        metrics);
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
      int expectedEvents, Meter successes, Meter retries, Meter deadLetters)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DRAIN_TIMEOUT_SECONDS);
    while (successes.getCount() < expectedEvents
        && retries.getCount() == 0
        && deadLetters.getCount() == 0
        && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(1);
    }
    assertThat(retries.getCount()).as("projection retries while draining").isZero();
    assertThat(deadLetters.getCount()).as("projection dead letters while draining").isZero();
    assertThat(successes.getCount())
        .as("events projected before the drain timeout")
        .isEqualTo(expectedEvents);
  }

  private void assertQueuedFixture(int admissionCount, List<UUID> expectedRunIds) {
    List<QueuedEvent> queued =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT id,
                               event::jsonb -> 'run' ->> 'runId' AS run_id,
                               admission_id
                        FROM open_lineage_queue
                        ORDER BY id
                        """)
                    .map(
                        (resultSet, context) ->
                            new QueuedEvent(
                                resultSet.getLong("id"),
                                UUID.fromString(resultSet.getString("run_id")),
                                resultSet.getObject("admission_id", Long.class)))
                    .list());
    assertThat(queued).hasSize(expectedRunIds.size());
    assertThat(queued).extracting(QueuedEvent::id).isSorted().doesNotHaveDuplicates();
    assertThat(queued).extracting(QueuedEvent::runId).containsExactlyElementsOf(expectedRunIds);

    Set<Long> admissionIds = new HashSet<>();
    for (int admission = 0; admission < admissionCount; admission++) {
      List<QueuedEvent> members =
          queued.subList(admission * EVENTS_PER_ADMISSION, (admission + 1) * EVENTS_PER_ADMISSION);
      Long admissionId = members.get(0).admissionId();
      assertThat(admissionId).isNotNull();
      assertThat(members)
          .allSatisfy(member -> assertThat(member.admissionId()).isEqualTo(admissionId));
      assertThat(admissionIds.add(admissionId)).isTrue();
    }

    QueueCounts counts = queueCounts();
    assertThat(counts)
        .isEqualTo(
            new QueueCounts(expectedRunIds.size(), expectedRunIds.size(), admissionCount, 0, 0));
    long rawEventCount =
        jdbi.withHandle(
            handle ->
                handle.createQuery("SELECT count(*) FROM lineage_events").mapTo(Long.class).one());
    assertThat(rawEventCount).isZero();
  }

  private void assertProjectedFixture(int admissionCount, int eventCount) {
    assertThat(queueCounts()).isEqualTo(new QueueCounts(0, 0, 0, 0, 0));
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

  private QueueCounts queueCounts() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT (SELECT count(*) FROM open_lineage_queue) AS queued,
                           (SELECT count(*) FROM open_lineage_queue_heads) AS heads,
                           (SELECT count(DISTINCT admission_id) FROM open_lineage_queue)
                               AS admissions,
                           (SELECT count(*) FROM open_lineage_queue WHERE admission_id IS NULL)
                               AS null_admissions,
                           (SELECT count(*) FROM open_lineage_dead_letters) AS dead_letters
                    """)
                .map(
                    (resultSet, context) ->
                        new QueueCounts(
                            resultSet.getLong("queued"),
                            resultSet.getLong("heads"),
                            resultSet.getLong("admissions"),
                            resultSet.getLong("null_admissions"),
                            resultSet.getLong("dead_letters")))
                .one());
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

  private static List<UUID> expectedRunIds(List<List<PreparedEvent>> admissions) {
    List<UUID> runIds = new ArrayList<>(admissions.size() * EVENTS_PER_ADMISSION);
    for (List<PreparedEvent> admission : admissions) {
      for (PreparedEvent event : admission) {
        try {
          runIds.add(
              UUID.fromString(
                  Utils.getMapper()
                      .readTree(event.eventJson())
                      .path("run")
                      .path("runId")
                      .asText()));
        } catch (Exception failure) {
          throw new IllegalArgumentException("benchmark fixture could not be read", failure);
        }
      }
    }
    return List.copyOf(runIds);
  }

  private String currentWalLsn() {
    return jdbi.withHandle(
        handle ->
            handle.createQuery("SELECT pg_current_wal_lsn()::text").mapTo(String.class).one());
  }

  private long walBytes(String before, String after) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT pg_wal_lsn_diff(CAST(:after AS pg_lsn), "
                        + "CAST(:before AS pg_lsn))::bigint")
                .bind("after", after)
                .bind("before", before)
                .mapTo(Long.class)
                .one());
  }

  private static String workerMetricName(String suffix) {
    return MetricRegistry.name(OpenLineageWorker.class, suffix);
  }

  private static double eventsPerSecond(int events, long elapsedNanos) {
    return events * 1_000_000_000.0 / elapsedNanos;
  }

  private static double nanosToMillis(long nanos) {
    return nanos / 1_000_000.0;
  }

  private static double median(List<Double> values) {
    List<Double> sorted = values.stream().sorted().toList();
    int middle = sorted.size() / 2;
    return (sorted.size() & 1) == 1
        ? sorted.get(middle)
        : (sorted.get(middle - 1) + sorted.get(middle)) / 2.0;
  }

  private static double minimum(List<Double> values) {
    return values.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
  }

  private static double maximum(List<Double> values) {
    return values.stream().mapToDouble(Double::doubleValue).max().orElseThrow();
  }

  private record QueuedEvent(long id, UUID runId, Long admissionId) {}

  private record QueueCounts(
      long queued, long heads, long admissions, long nullAdmissions, long deadLetters) {}

  private record ProjectionCounts(
      long rawEvents, long distinctRawRuns, long runs, long jobs, long datasets) {}

  private record CellResult(
      int projectionBatchSize,
      int admissionCount,
      int eventCount,
      long admissionNanos,
      long drainNanos,
      long admissionTransactions,
      long successfulProjectionClaimTransactions,
      long admissionWalBytes,
      long drainWalBytes) {
    private double admissionEventsPerSecond() {
      return eventsPerSecond(eventCount, admissionNanos);
    }

    private double drainEventsPerSecond() {
      return eventsPerSecond(eventCount, drainNanos);
    }

    private double sequentialPipelineEventsPerSecond() {
      return eventsPerSecond(eventCount, Math.addExact(admissionNanos, drainNanos));
    }
  }
}
