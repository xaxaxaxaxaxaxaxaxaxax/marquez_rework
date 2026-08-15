/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import marquez.db.models.MetricPoint;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.jackson2.Jackson2Plugin;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.testing.junit5.JdbiExtension;
import org.jdbi.v3.testing.junit5.tc.JdbiTestcontainersExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Opt-in native-table query benchmark; never enabled by the ordinary test suite. */
@Tag("IntegrationTests")
@EnabledIfSystemProperty(named = "runStatsQueryBenchmark", matches = "true")
@Testcontainers
class StatsQueryBenchmark {
  private static final DockerImageName POSTGRES_14 = DockerImageName.parse("postgres:14");
  private static final Instant START_AT = Instant.parse("2025-01-01T00:00:00Z");
  private static final Duration RANGE = Duration.ofDays(366);
  private static final Instant END_AT = START_AT.plus(RANGE);
  private static final Duration ROLLUP = Duration.ofHours(12);
  private static final int EXPECTED_BUCKETS = 732;
  private static final int TRIALS = Integer.getInteger("statsQueryBenchmark.trials", 7);
  private static final int LINEAGE_EVENT_COUNT =
      Integer.getInteger("statsQueryBenchmark.lineageEvents", 100_000);
  private static final int JOB_COUNT = Integer.getInteger("statsQueryBenchmark.jobs", 5_000);
  private static final int DATASET_COUNT =
      Integer.getInteger("statsQueryBenchmark.datasets", 20_000);
  private static final int SOURCE_COUNT = Integer.getInteger("statsQueryBenchmark.sources", 1_000);
  private static final UUID HOT_NAMESPACE_ID =
      UUID.fromString("11111111-1111-1111-1111-111111111111");
  private static final UUID OTHER_NAMESPACE_ID =
      UUID.fromString("22222222-2222-2222-2222-222222222222");
  private static final UUID RARE_NAMESPACE_ID =
      UUID.fromString("33333333-3333-3333-3333-333333333333");
  private static final UUID HOT_RUN_ID = UUID.fromString("44444444-4444-4444-4444-444444444444");
  private static final UUID RARE_RUN_ID = UUID.fromString("55555555-5555-5555-5555-555555555555");
  private static final String HOT_NAMESPACE = "stats-benchmark-hot";
  private static final String OTHER_NAMESPACE = "stats-benchmark-other";
  private static final String RARE_NAMESPACE = "stats-benchmark-rare";
  private static final String HOT_JOB = "stats-benchmark-hot-job";
  private static final String RARE_JOB = "stats-benchmark-rare-job";

  @Container
  @Order(1)
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGRES_14);

  @RegisterExtension
  @Order(2)
  static final JdbiExtension JDBI_EXTENSION =
      JdbiTestcontainersExtension.instance(POSTGRES)
          .withPlugin(new SqlObjectPlugin())
          .withPlugin(new PostgresPlugin())
          .withPlugin(new Jackson2Plugin())
          .withInitializer((source, handle) -> DbMigration.migrateDbOrError(source));

  private static Jdbi jdbi;
  private static StatsDao statsDao;

  @BeforeAll
  static void seedRepresentativeStocks() {
    assertThat(TRIALS).isPositive();
    assertThat(LINEAGE_EVENT_COUNT).isPositive();
    assertThat(JOB_COUNT).isPositive();
    assertThat(DATASET_COUNT).isPositive();
    assertThat(SOURCE_COUNT).isPositive();

    jdbi = JDBI_EXTENSION.getJdbi();
    statsDao = jdbi.onDemand(StatsDao.class);
    jdbi.useTransaction(StatsQueryBenchmark::seed);
    jdbi.useHandle(
        handle -> {
          handle.execute("ANALYZE lineage_events");
          handle.execute("ANALYZE jobs");
          handle.execute("ANALYZE datasets");
          handle.execute("ANALYZE sources");
        });
  }

  @Test
  void reportsChecksumGatedRepresentativeTimings() {
    List<BenchmarkCase> cases = benchmarkCases();
    Map<String, String> expectedChecksums = new LinkedHashMap<>();
    Map<String, List<Long>> timings = new LinkedHashMap<>();

    for (BenchmarkCase benchmarkCase : cases) {
      List<MetricPoint> warmup = benchmarkCase.query().get();
      assertThat(warmup).as(benchmarkCase.name() + " warm-up rows").hasSize(EXPECTED_BUCKETS);
      expectedChecksums.put(benchmarkCase.name(), checksum(warmup));
      timings.put(benchmarkCase.name(), new ArrayList<>(TRIALS));
    }

    for (int trial = 0; trial < TRIALS; trial++) {
      List<BenchmarkCase> ordered = new ArrayList<>(cases);
      if ((trial & 1) == 1) {
        Collections.reverse(ordered);
      }
      for (BenchmarkCase benchmarkCase : ordered) {
        long startedAt = System.nanoTime();
        List<MetricPoint> points = benchmarkCase.query().get();
        long elapsedNanos = System.nanoTime() - startedAt;

        assertThat(points).as(benchmarkCase.name() + " measured rows").hasSize(EXPECTED_BUCKETS);
        assertThat(checksum(points))
            .as(benchmarkCase.name() + " trial " + (trial + 1) + " checksum")
            .isEqualTo(expectedChecksums.get(benchmarkCase.name()));
        timings.get(benchmarkCase.name()).add(elapsedNanos);
      }
    }

    for (BenchmarkCase benchmarkCase : cases) {
      List<Long> samples = timings.get(benchmarkCase.name());
      System.out.printf(
          Locale.ROOT,
          "STATS_QUERY_BENCHMARK case=%s trials=%d lineage_events=%d jobs=%d datasets=%d "
              + "sources=%d buckets=%d median_ms=%.3f range_ms=%.3f..%.3f checksum=%s%n",
          benchmarkCase.name(),
          TRIALS,
          LINEAGE_EVENT_COUNT,
          JOB_COUNT,
          DATASET_COUNT,
          SOURCE_COUNT,
          EXPECTED_BUCKETS,
          nanosToMillis(median(samples)),
          nanosToMillis(minimum(samples)),
          nanosToMillis(maximum(samples)),
          expectedChecksums.get(benchmarkCase.name()));
    }
  }

  private static List<BenchmarkCase> benchmarkCases() {
    long bucketMillis = ROLLUP.toMillis();
    return List.of(
        new BenchmarkCase(
            "lineage-global",
            () -> statsDao.queryLineageEventsGlobal("START", START_AT, END_AT, bucketMillis)),
        new BenchmarkCase(
            "lineage-hot-namespace",
            () ->
                statsDao.queryLineageEventsForNamespace(
                    "START", HOT_NAMESPACE, START_AT, END_AT, bucketMillis)),
        new BenchmarkCase(
            "lineage-rare-job",
            () ->
                statsDao.queryLineageEventsForJob(
                    "START", RARE_NAMESPACE, RARE_JOB, START_AT, END_AT, bucketMillis)),
        new BenchmarkCase(
            "lineage-rare-run",
            () ->
                statsDao.queryLineageEventsForRun(
                    "START", RARE_RUN_ID, START_AT, END_AT, bucketMillis)),
        new BenchmarkCase(
            "jobs-global", () -> statsDao.queryJobsGlobal(START_AT, END_AT, bucketMillis)),
        new BenchmarkCase(
            "jobs-hot-namespace",
            () -> statsDao.queryJobsForNamespace(HOT_NAMESPACE, START_AT, END_AT, bucketMillis)),
        new BenchmarkCase(
            "datasets-global", () -> statsDao.queryDatasetsGlobal(START_AT, END_AT, bucketMillis)),
        new BenchmarkCase(
            "datasets-hot-namespace",
            () ->
                statsDao.queryDatasetsForNamespace(HOT_NAMESPACE, START_AT, END_AT, bucketMillis)),
        new BenchmarkCase(
            "sources-global", () -> statsDao.querySourcesGlobal(START_AT, END_AT, bucketMillis)));
  }

  private static void seed(Handle handle) {
    handle
        .createUpdate(
            """
            INSERT INTO namespaces (
                uuid, created_at, updated_at, name, current_owner_name, is_hidden)
            VALUES
                (:hotId, :createdAt, :createdAt, :hotName, 'owner', FALSE),
                (:otherId, :createdAt, :createdAt, :otherName, 'owner', FALSE),
                (:rareId, :createdAt, :createdAt, :rareName, 'owner', FALSE)
            """)
        .bind("hotId", HOT_NAMESPACE_ID)
        .bind("otherId", OTHER_NAMESPACE_ID)
        .bind("rareId", RARE_NAMESPACE_ID)
        .bind("createdAt", START_AT.minus(Duration.ofDays(30)))
        .bind("hotName", HOT_NAMESPACE)
        .bind("otherName", OTHER_NAMESPACE)
        .bind("rareName", RARE_NAMESPACE)
        .execute();

    handle
        .createUpdate(
            """
            INSERT INTO sources (uuid, type, created_at, updated_at, name, connection_url)
            SELECT md5('stats-query-benchmark-source:' || generated.value)::UUID,
                   'DB',
                   CAST(:startAt AS TIMESTAMPTZ) - INTERVAL '30 days'
                     + ((((generated.value::BIGINT * 7919) % :rangeSeconds)::DOUBLE PRECISION)
                         * INTERVAL '1 second'),
                   CAST(:startAt AS TIMESTAMPTZ) - INTERVAL '30 days'
                     + ((((generated.value::BIGINT * 7919) % :rangeSeconds)::DOUBLE PRECISION)
                         * INTERVAL '1 second'),
                   'stats-benchmark-source-' || generated.value,
                   'postgres://stats-benchmark/source-' || generated.value
            FROM generate_series(1, :sourceCount) AS generated(value)
            """)
        .bind("startAt", START_AT)
        .bind("rangeSeconds", RANGE.getSeconds())
        .bind("sourceCount", SOURCE_COUNT)
        .execute();

    handle
        .createUpdate(
            """
            INSERT INTO jobs (
                uuid, type, created_at, updated_at, namespace_uuid, namespace_name,
                simple_name, name)
            SELECT md5('stats-query-benchmark-job:' || generated.value)::UUID,
                   'BATCH',
                   CAST(:startAt AS TIMESTAMPTZ) - INTERVAL '30 days'
                     + ((((generated.value::BIGINT * 7919) % :rangeSeconds)::DOUBLE PRECISION)
                         * INTERVAL '1 second'),
                   CAST(:startAt AS TIMESTAMPTZ) - INTERVAL '30 days'
                     + ((((generated.value::BIGINT * 7919) % :rangeSeconds)::DOUBLE PRECISION)
                         * INTERVAL '1 second'),
                   CASE
                     WHEN generated.value % 1000 = 0 THEN CAST(:rareId AS UUID)
                     WHEN generated.value % 10 <> 0 THEN CAST(:hotId AS UUID)
                     ELSE CAST(:otherId AS UUID)
                   END,
                   CASE
                     WHEN generated.value % 1000 = 0 THEN :rareName
                     WHEN generated.value % 10 <> 0 THEN :hotName
                     ELSE :otherName
                   END,
                   'stats-benchmark-job-' || generated.value,
                   'stats-benchmark-job-' || generated.value
            FROM generate_series(1, :jobCount) AS generated(value)
            """)
        .bind("startAt", START_AT)
        .bind("rangeSeconds", RANGE.getSeconds())
        .bind("jobCount", JOB_COUNT)
        .bind("hotId", HOT_NAMESPACE_ID)
        .bind("otherId", OTHER_NAMESPACE_ID)
        .bind("rareId", RARE_NAMESPACE_ID)
        .bind("hotName", HOT_NAMESPACE)
        .bind("otherName", OTHER_NAMESPACE)
        .bind("rareName", RARE_NAMESPACE)
        .execute();

    handle
        .createUpdate(
            """
            INSERT INTO datasets (
                uuid, type, created_at, updated_at, namespace_uuid, namespace_name,
                source_uuid, source_name, name, physical_name)
            SELECT md5('stats-query-benchmark-dataset:' || generated.value)::UUID,
                   'DB_TABLE',
                   CAST(:startAt AS TIMESTAMPTZ) - INTERVAL '30 days'
                     + ((((generated.value::BIGINT * 7919) % :rangeSeconds)::DOUBLE PRECISION)
                         * INTERVAL '1 second'),
                   CAST(:startAt AS TIMESTAMPTZ) - INTERVAL '30 days'
                     + ((((generated.value::BIGINT * 7919) % :rangeSeconds)::DOUBLE PRECISION)
                         * INTERVAL '1 second'),
                   CASE
                     WHEN generated.value % 1000 = 0 THEN CAST(:rareId AS UUID)
                     WHEN generated.value % 10 <> 0 THEN CAST(:hotId AS UUID)
                     ELSE CAST(:otherId AS UUID)
                   END,
                   CASE
                     WHEN generated.value % 1000 = 0 THEN :rareName
                     WHEN generated.value % 10 <> 0 THEN :hotName
                     ELSE :otherName
                   END,
                   source.uuid,
                   source.name,
                   'stats-benchmark-dataset-' || generated.value,
                   'stats-benchmark-physical-' || generated.value
            FROM generate_series(1, :datasetCount) AS generated(value)
            CROSS JOIN (
                SELECT uuid, name
                FROM sources
                WHERE name = 'stats-benchmark-source-1') AS source
            """)
        .bind("startAt", START_AT)
        .bind("rangeSeconds", RANGE.getSeconds())
        .bind("datasetCount", DATASET_COUNT)
        .bind("hotId", HOT_NAMESPACE_ID)
        .bind("otherId", OTHER_NAMESPACE_ID)
        .bind("rareId", RARE_NAMESPACE_ID)
        .bind("hotName", HOT_NAMESPACE)
        .bind("otherName", OTHER_NAMESPACE)
        .bind("rareName", RARE_NAMESPACE)
        .execute();

    handle
        .createUpdate(
            """
            INSERT INTO lineage_events (
                event_time, event, event_type, run_uuid, job_name, job_namespace, producer)
            SELECT CAST(:startAt AS TIMESTAMPTZ)
                     + ((((generated.value::BIGINT * 7919) % :rangeSeconds)::DOUBLE PRECISION)
                         * INTERVAL '1 second'),
                   jsonb_build_object('benchmarkId', generated.value),
                   CASE generated.value % 4
                     WHEN 0 THEN 'START'
                     WHEN 1 THEN 'COMPLETE'
                     WHEN 2 THEN 'FAIL'
                     ELSE 'ABORT'
                   END,
                   CASE
                     WHEN generated.value % 1000 = 0 THEN CAST(:rareRunId AS UUID)
                     ELSE CAST(:hotRunId AS UUID)
                   END,
                   CASE
                     WHEN generated.value % 1000 = 0 THEN :rareJob
                     ELSE :hotJob
                   END,
                   CASE
                     WHEN generated.value % 1000 = 0 THEN :rareName
                     WHEN generated.value % 10 <> 0 THEN :hotName
                     ELSE :otherName
                   END,
                   'https://example.com/stats-query-benchmark'
            FROM generate_series(1, :lineageEventCount) AS generated(value)
            """)
        .bind("startAt", START_AT)
        .bind("rangeSeconds", RANGE.getSeconds())
        .bind("lineageEventCount", LINEAGE_EVENT_COUNT)
        .bind("hotRunId", HOT_RUN_ID)
        .bind("rareRunId", RARE_RUN_ID)
        .bind("hotJob", HOT_JOB)
        .bind("rareJob", RARE_JOB)
        .bind("hotName", HOT_NAMESPACE)
        .bind("otherName", OTHER_NAMESPACE)
        .bind("rareName", RARE_NAMESPACE)
        .execute();
  }

  private static String checksum(List<MetricPoint> points) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (MetricPoint point : points) {
        digest.update(point.getStartAt().toString().getBytes(UTF_8));
        digest.update((byte) 0);
        digest.update(point.getEndAt().toString().getBytes(UTF_8));
        digest.update((byte) 0);
        digest.update(Long.toString(point.getValue()).getBytes(UTF_8));
        digest.update((byte) '\n');
      }
      return java.util.HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException error) {
      throw new AssertionError("SHA-256 is required by the Java runtime", error);
    }
  }

  private static long median(List<Long> samples) {
    List<Long> ordered = samples.stream().sorted().toList();
    int middle = ordered.size() / 2;
    if ((ordered.size() & 1) == 1) {
      return ordered.get(middle);
    }
    return (ordered.get(middle - 1) + ordered.get(middle)) / 2;
  }

  private static long minimum(List<Long> samples) {
    return samples.stream().min(Comparator.naturalOrder()).orElseThrow();
  }

  private static long maximum(List<Long> samples) {
    return samples.stream().max(Comparator.naturalOrder()).orElseThrow();
  }

  private static double nanosToMillis(long nanos) {
    return nanos / 1_000_000.0;
  }

  private record BenchmarkCase(String name, Supplier<List<MetricPoint>> query) {}
}
