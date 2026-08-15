/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import marquez.common.Utils;
import marquez.common.models.DatasetType;
import marquez.common.models.JobType;
import marquez.db.models.MetricPoint;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Query;
import org.jdbi.v3.jackson2.Jackson2Plugin;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.testing.junit5.JdbiExtension;
import org.jdbi.v3.testing.junit5.tc.JdbiTestcontainersExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("DataAccessTests")
@Tag("IntegrationTests")
@Testcontainers
class StatsQueryVerificationTest {
  private static final DockerImageName POSTGRES_14 = DockerImageName.parse("postgres:14");
  private static final Instant START_AT = Instant.parse("2025-01-01T00:07:00Z");
  private static final Instant END_AT =
      START_AT.plus(Duration.ofHours(2)).plus(Duration.ofMinutes(37));
  private static final Duration ROLLUP = Duration.ofHours(1);
  private static final String HOT_NAMESPACE = "stats-verification-hot";
  private static final String COLD_NAMESPACE = "stats-verification-cold";
  private static final String EMPTY_NAMESPACE = "stats-verification-empty";
  private static final String HOT_JOB = "stats-verification-hot-job";
  private static final String OTHER_JOB = "stats-verification-other-job";
  private static final String EMPTY_JOB = "stats-verification-empty-job";
  private static final UUID HOT_RUN = id("hot-run");
  private static final UUID OTHER_RUN = id("other-run");
  private static final UUID EMPTY_RUN = id("empty-run");
  private static final List<String> EVENT_TYPES = List.of("START", "COMPLETE", "FAIL", "ABORT");
  private static final Set<String> PHYSICAL_SCAN_TYPES =
      Set.of(
          "Seq Scan",
          "Index Scan",
          "Index Only Scan",
          "Bitmap Heap Scan",
          "Tid Scan",
          "Tid Range Scan");

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
  private static Fixture fixture;

  @BeforeAll
  static void setUpFixture() {
    jdbi = JDBI_EXTENSION.getJdbi();
    statsDao = jdbi.onDemand(StatsDao.class);
    fixture = seedFixture(jdbi);
  }

  @Test
  void everySupportedQueryShapeMatchesAnIndependentFixedWidthOracle() {
    long bucketMillis = ROLLUP.toMillis();

    for (String eventType : EVENT_TYPES) {
      assertSeries(
          eventType + " global",
          flowOracle(eventType, observation -> true, START_AT, END_AT, bucketMillis),
          statsDao.queryLineageEventsGlobal(eventType, START_AT, END_AT, bucketMillis));
      assertSeries(
          eventType + " namespace",
          flowOracle(
              eventType,
              observation -> HOT_NAMESPACE.equals(observation.namespace()),
              START_AT,
              END_AT,
              bucketMillis),
          statsDao.queryLineageEventsForNamespace(
              eventType, HOT_NAMESPACE, START_AT, END_AT, bucketMillis));
      assertSeries(
          eventType + " job",
          flowOracle(
              eventType,
              observation ->
                  HOT_NAMESPACE.equals(observation.namespace())
                      && HOT_JOB.equals(observation.jobName()),
              START_AT,
              END_AT,
              bucketMillis),
          statsDao.queryLineageEventsForJob(
              eventType, HOT_NAMESPACE, HOT_JOB, START_AT, END_AT, bucketMillis));
      assertSeries(
          eventType + " run",
          flowOracle(
              eventType,
              observation -> HOT_RUN.equals(observation.runId()),
              START_AT,
              END_AT,
              bucketMillis),
          statsDao.queryLineageEventsForRun(eventType, HOT_RUN, START_AT, END_AT, bucketMillis));
      assertSeries(
          eventType + " empty namespace",
          flowOracle(
              eventType,
              observation -> EMPTY_NAMESPACE.equals(observation.namespace()),
              START_AT,
              END_AT,
              bucketMillis),
          statsDao.queryLineageEventsForNamespace(
              eventType, EMPTY_NAMESPACE, START_AT, END_AT, bucketMillis));
      assertSeries(
          eventType + " empty job",
          flowOracle(
              eventType,
              observation ->
                  EMPTY_NAMESPACE.equals(observation.namespace())
                      && EMPTY_JOB.equals(observation.jobName()),
              START_AT,
              END_AT,
              bucketMillis),
          statsDao.queryLineageEventsForJob(
              eventType, EMPTY_NAMESPACE, EMPTY_JOB, START_AT, END_AT, bucketMillis));
      assertSeries(
          eventType + " empty run",
          flowOracle(
              eventType,
              observation -> EMPTY_RUN.equals(observation.runId()),
              START_AT,
              END_AT,
              bucketMillis),
          statsDao.queryLineageEventsForRun(eventType, EMPTY_RUN, START_AT, END_AT, bucketMillis));
    }

    assertSeries(
        "jobs global",
        stockOracle(fixture.jobs(), observation -> true, START_AT, END_AT, bucketMillis),
        statsDao.queryJobsGlobal(START_AT, END_AT, bucketMillis));
    assertSeries(
        "jobs namespace",
        stockOracle(
            fixture.jobs(),
            observation -> HOT_NAMESPACE.equals(observation.namespace()),
            START_AT,
            END_AT,
            bucketMillis),
        statsDao.queryJobsForNamespace(HOT_NAMESPACE, START_AT, END_AT, bucketMillis));
    assertSeries(
        "jobs empty namespace",
        stockOracle(
            fixture.jobs(),
            observation -> EMPTY_NAMESPACE.equals(observation.namespace()),
            START_AT,
            END_AT,
            bucketMillis),
        statsDao.queryJobsForNamespace(EMPTY_NAMESPACE, START_AT, END_AT, bucketMillis));
    assertSeries(
        "datasets global",
        stockOracle(fixture.datasets(), observation -> true, START_AT, END_AT, bucketMillis),
        statsDao.queryDatasetsGlobal(START_AT, END_AT, bucketMillis));
    assertSeries(
        "datasets namespace",
        stockOracle(
            fixture.datasets(),
            observation -> HOT_NAMESPACE.equals(observation.namespace()),
            START_AT,
            END_AT,
            bucketMillis),
        statsDao.queryDatasetsForNamespace(HOT_NAMESPACE, START_AT, END_AT, bucketMillis));
    assertSeries(
        "datasets empty namespace",
        stockOracle(
            fixture.datasets(),
            observation -> EMPTY_NAMESPACE.equals(observation.namespace()),
            START_AT,
            END_AT,
            bucketMillis),
        statsDao.queryDatasetsForNamespace(EMPTY_NAMESPACE, START_AT, END_AT, bucketMillis));
    assertSeries(
        "sources global",
        stockOracle(fixture.sources(), observation -> true, START_AT, END_AT, bucketMillis),
        statsDao.querySourcesGlobal(START_AT, END_AT, bucketMillis));
    assertSeries(
        "cold lineage namespace",
        flowOracle(
            "START",
            observation -> COLD_NAMESPACE.equals(observation.namespace()),
            START_AT,
            END_AT,
            bucketMillis),
        statsDao.queryLineageEventsForNamespace(
            "START", COLD_NAMESPACE, START_AT, END_AT, bucketMillis));

    Instant oneBucketEnd = START_AT.plus(Duration.ofMinutes(30));
    assertSeries(
        "one partial bucket",
        flowOracle("START", observation -> true, START_AT, oneBucketEnd, bucketMillis),
        statsDao.queryLineageEventsGlobal("START", START_AT, oneBucketEnd, bucketMillis));

    Instant beforeEverySource = START_AT.minus(Duration.ofDays(4));
    Instant beforeEverySourceEnd = beforeEverySource.plus(Duration.ofHours(3));
    assertSeries(
        "sources empty window",
        stockOracle(
            fixture.sources(),
            observation -> true,
            beforeEverySource,
            beforeEverySourceEnd,
            bucketMillis),
        statsDao.querySourcesGlobal(beforeEverySource, beforeEverySourceEnd, bucketMillis));

    List<MetricPoint> partial =
        statsDao.queryLineageEventsForJob(
            "START", HOT_NAMESPACE, HOT_JOB, START_AT, END_AT, bucketMillis);
    assertThat(partial).hasSize(3);
    assertThat(partial.get(partial.size() - 1).getEndAt()).isEqualTo(END_AT);

    Instant thousandBucketEnd = START_AT.plus(Duration.ofMinutes(1000));
    long minuteMillis = Duration.ofMinutes(1).toMillis();
    assertSeries(
        "exactly 1000 buckets",
        flowOracle("START", observation -> true, START_AT, thousandBucketEnd, minuteMillis),
        statsDao.queryLineageEventsGlobal("START", START_AT, thousandBucketEnd, minuteMillis));

    Instant longRangeStart = START_AT.minus(Duration.ofDays(365));
    Instant longRangeEnd = longRangeStart.plus(Duration.ofDays(366));
    long dayMillis = Duration.ofDays(1).toMillis();
    assertSeries(
        "exactly 366 elapsed days",
        stockOracle(
            fixture.sources(), observation -> true, longRangeStart, longRangeEnd, dayMillis),
        statsDao.querySourcesGlobal(longRangeStart, longRangeEnd, dayMillis));
  }

  @Test
  void everyProductionPlanBoundsOutputAndScansItsBaseRelationOnce() throws Exception {
    int expectedBuckets = 3;
    long bucketMillis = ROLLUP.toMillis();
    List<PlanCase> cases =
        List.of(
            new PlanCase(
                "lineage global",
                StatsDao.QUERY_LINEAGE_EVENTS_GLOBAL_SQL,
                "lineage_events",
                query -> query.bind("eventType", "START")),
            new PlanCase(
                "lineage namespace",
                StatsDao.QUERY_LINEAGE_EVENTS_NAMESPACE_SQL,
                "lineage_events",
                query -> query.bind("eventType", "START").bind("namespace", HOT_NAMESPACE)),
            new PlanCase(
                "lineage job",
                StatsDao.QUERY_LINEAGE_EVENTS_JOB_SQL,
                "lineage_events",
                query ->
                    query
                        .bind("eventType", "START")
                        .bind("namespace", HOT_NAMESPACE)
                        .bind("jobName", HOT_JOB)),
            new PlanCase(
                "lineage run",
                StatsDao.QUERY_LINEAGE_EVENTS_RUN_SQL,
                "lineage_events",
                query -> query.bind("eventType", "START").bind("runId", HOT_RUN)),
            new PlanCase(
                "jobs global", StatsDao.QUERY_JOBS_GLOBAL_SQL, "jobs", UnaryOperator.identity()),
            new PlanCase(
                "jobs namespace",
                StatsDao.QUERY_JOBS_NAMESPACE_SQL,
                "jobs",
                query -> query.bind("namespace", HOT_NAMESPACE)),
            new PlanCase(
                "datasets global",
                StatsDao.QUERY_DATASETS_GLOBAL_SQL,
                "datasets",
                UnaryOperator.identity()),
            new PlanCase(
                "datasets namespace",
                StatsDao.QUERY_DATASETS_NAMESPACE_SQL,
                "datasets",
                query -> query.bind("namespace", HOT_NAMESPACE)),
            new PlanCase(
                "sources global",
                StatsDao.QUERY_SOURCES_GLOBAL_SQL,
                "sources",
                UnaryOperator.identity()));

    try (Handle handle = jdbi.open()) {
      handle.execute("ANALYZE lineage_events");
      handle.execute("ANALYZE jobs");
      handle.execute("ANALYZE datasets");
      handle.execute("ANALYZE sources");

      for (PlanCase planCase : cases) {
        JsonNode plan = explain(handle, planCase, START_AT, END_AT, bucketMillis);
        List<JsonNode> nodes = planNodes(plan);

        assertThat(plan.path("Actual Rows").asLong())
            .as(planCase.name() + " output rows")
            .isEqualTo(expectedBuckets);

        List<JsonNode> bucketGenerators =
            nodes.stream()
                .filter(node -> "Function Scan".equals(node.path("Node Type").asText()))
                .filter(node -> "generate_series".equals(node.path("Function Name").asText()))
                .toList();
        assertThat(bucketGenerators).as(planCase.name() + " bucket generator").hasSize(1);
        assertThat(bucketGenerators.get(0).path("Actual Rows").asLong())
            .as(planCase.name() + " generated buckets")
            .isEqualTo(expectedBuckets);
        assertThat(bucketGenerators.get(0).path("Actual Loops").asLong())
            .as(planCase.name() + " bucket generator loops")
            .isEqualTo(1);

        List<JsonNode> baseScans =
            nodes.stream()
                .filter(node -> planCase.relation().equals(node.path("Relation Name").asText()))
                .filter(node -> PHYSICAL_SCAN_TYPES.contains(node.path("Node Type").asText()))
                .toList();
        assertThat(baseScans).as(planCase.name() + " physical base scans").hasSize(1);
        assertThat(baseScans.get(0).path("Actual Loops").asLong())
            .as(planCase.name() + " must not rescan the base table per bucket")
            .isEqualTo(1);

        assertThat(nodes)
            .as(planCase.name() + " temp blocks")
            .allSatisfy(
                node -> {
                  assertThat(node.path("Temp Read Blocks").asLong()).isZero();
                  assertThat(node.path("Temp Written Blocks").asLong()).isZero();
                  assertThat(node.path("Sort Method").asText()).doesNotContain("external");
                });
      }
    }
  }

  private static JsonNode explain(
      Handle handle, PlanCase planCase, Instant startAt, Instant endAt, long bucketMillis)
      throws Exception {
    Query query =
        handle
            .createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, COSTS OFF, TIMING OFF, SUMMARY OFF) "
                    + planCase.sql())
            .bind("startAt", startAt)
            .bind("endAt", endAt)
            .bind("bucketMillis", bucketMillis);
    String json = planCase.bind().apply(query).mapTo(String.class).one();
    return Utils.getMapper().readTree(json).path(0).path("Plan");
  }

  private static List<JsonNode> planNodes(JsonNode root) {
    List<JsonNode> nodes = new ArrayList<>();
    collectPlanNodes(root, nodes);
    return nodes;
  }

  private static void collectPlanNodes(JsonNode node, List<JsonNode> nodes) {
    if (!node.isObject()) {
      return;
    }
    nodes.add(node);
    node.path("Plans").forEach(child -> collectPlanNodes(child, nodes));
  }

  private static void assertSeries(
      String description, List<MetricPoint> expected, List<MetricPoint> actual) {
    assertThat(actual).as(description).containsExactlyElementsOf(expected);
  }

  private static List<MetricPoint> flowOracle(
      String eventType,
      Predicate<FlowObservation> scope,
      Instant startAt,
      Instant endAt,
      long bucketMillis) {
    return buckets(startAt, endAt, bucketMillis).stream()
        .map(
            bucket ->
                new MetricPoint(
                    bucket.startAt(),
                    bucket.endAt(),
                    fixture.flows().stream()
                        .filter(observation -> eventType.equals(observation.eventType()))
                        .filter(scope)
                        .filter(observation -> !observation.observedAt().isBefore(bucket.startAt()))
                        .filter(observation -> observation.observedAt().isBefore(bucket.endAt()))
                        .count()))
        .toList();
  }

  private static List<MetricPoint> stockOracle(
      List<StockObservation> observations,
      Predicate<StockObservation> scope,
      Instant startAt,
      Instant endAt,
      long bucketMillis) {
    return buckets(startAt, endAt, bucketMillis).stream()
        .map(
            bucket ->
                new MetricPoint(
                    bucket.startAt(),
                    bucket.endAt(),
                    observations.stream()
                        .filter(scope)
                        .filter(observation -> observation.observedAt().isBefore(bucket.endAt()))
                        .count()))
        .toList();
  }

  private static List<Bucket> buckets(Instant startAt, Instant endAt, long bucketMillis) {
    List<Bucket> buckets = new ArrayList<>();
    Instant bucketStart = startAt;
    while (bucketStart.isBefore(endAt)) {
      Instant unboundedEnd = bucketStart.plusMillis(bucketMillis);
      Instant bucketEnd = unboundedEnd.isAfter(endAt) ? endAt : unboundedEnd;
      buckets.add(new Bucket(bucketStart, bucketEnd));
      bucketStart = bucketEnd;
    }
    return buckets;
  }

  private static Fixture seedFixture(Jdbi jdbi) {
    List<FlowObservation> flows = new ArrayList<>();
    for (int typeIndex = 0; typeIndex < EVENT_TYPES.size(); typeIndex++) {
      String eventType = EVENT_TYPES.get(typeIndex);
      flows.add(
          new FlowObservation(START_AT.minusMillis(1), eventType, HOT_NAMESPACE, HOT_JOB, HOT_RUN));
      flows.add(new FlowObservation(START_AT, eventType, HOT_NAMESPACE, HOT_JOB, HOT_RUN));
      flows.add(
          new FlowObservation(
              START_AT.plus(Duration.ofMinutes(30)).plusSeconds(typeIndex),
              eventType,
              HOT_NAMESPACE,
              HOT_JOB,
              HOT_RUN));
      flows.add(
          new FlowObservation(
              START_AT.plus(Duration.ofHours(1)), eventType, HOT_NAMESPACE, OTHER_JOB, OTHER_RUN));
      flows.add(
          new FlowObservation(
              START_AT.plus(Duration.ofHours(1)).plus(Duration.ofMinutes(15)),
              eventType,
              COLD_NAMESPACE,
              HOT_JOB,
              OTHER_RUN));
      flows.add(
          new FlowObservation(END_AT.minusMillis(1), eventType, HOT_NAMESPACE, HOT_JOB, HOT_RUN));
      flows.add(new FlowObservation(END_AT, eventType, HOT_NAMESPACE, HOT_JOB, HOT_RUN));
    }

    List<StockObservation> sources =
        List.of(
            new StockObservation(START_AT.minus(Duration.ofDays(2)), null),
            new StockObservation(START_AT, null),
            new StockObservation(START_AT.plus(Duration.ofHours(1)), null),
            new StockObservation(END_AT.minusMillis(1), null),
            new StockObservation(END_AT, null));
    List<StockObservation> jobs = stockFixture();
    List<StockObservation> datasets = stockFixture();

    jdbi.useHandle(
        handle -> {
          NamespaceDao namespaceDao = handle.attach(NamespaceDao.class);
          SourceDao sourceDao = handle.attach(SourceDao.class);
          JobDao jobDao = handle.attach(JobDao.class);
          DatasetDao datasetDao = handle.attach(DatasetDao.class);

          namespaceDao.upsertNamespaceRow(
              id("namespace-hot"),
              START_AT.minus(Duration.ofDays(3)),
              HOT_NAMESPACE,
              "owner",
              null);
          namespaceDao.upsertNamespaceRow(
              id("namespace-cold"),
              START_AT.minus(Duration.ofDays(3)),
              COLD_NAMESPACE,
              "owner",
              null);
          namespaceDao.upsertNamespaceRow(
              id("namespace-empty"),
              START_AT.minus(Duration.ofDays(3)),
              EMPTY_NAMESPACE,
              "owner",
              null);

          for (int index = 0; index < sources.size(); index++) {
            sourceDao.upsert(
                id("source-" + index),
                "DB",
                sources.get(index).observedAt(),
                "stats-verification-source-" + index,
                "postgres://stats-verification/source-" + index);
          }

          for (int index = 0; index < jobs.size(); index++) {
            StockObservation observation = jobs.get(index);
            String name = "stats-verification-job-" + index;
            jobDao.upsertJob(
                id("job-" + index),
                JobType.BATCH,
                observation.observedAt(),
                namespaceId(observation.namespace()),
                observation.namespace(),
                name,
                null,
                "stats://verification/" + name,
                null,
                null,
                null);
          }

          for (int index = 0; index < datasets.size(); index++) {
            StockObservation observation = datasets.get(index);
            String name = "stats-verification-dataset-" + index;
            datasetDao.upsert(
                id("dataset-" + index),
                DatasetType.DB_TABLE,
                observation.observedAt(),
                namespaceId(observation.namespace()),
                observation.namespace(),
                id("source-0"),
                "stats-verification-source-0",
                name,
                "stats-verification-physical-" + index);
          }

          assertThat(
                  handle
                      .createUpdate(
                          "UPDATE jobs SET is_hidden = TRUE WHERE uuid = CAST(:uuid AS UUID)")
                      .bind("uuid", id("job-0"))
                      .execute())
              .isEqualTo(1);
          assertThat(
                  handle
                      .createUpdate(
                          "UPDATE jobs SET symlink_target_uuid = CAST(:target AS UUID) "
                              + "WHERE uuid = CAST(:uuid AS UUID)")
                      .bind("target", id("job-0"))
                      .bind("uuid", id("job-1"))
                      .execute())
              .isEqualTo(1);
          assertThat(
                  handle
                      .createUpdate(
                          "UPDATE datasets SET is_hidden = TRUE, is_deleted = TRUE "
                              + "WHERE uuid = CAST(:uuid AS UUID)")
                      .bind("uuid", id("dataset-0"))
                      .execute())
              .isEqualTo(1);

          var eventBatch =
              handle.prepareBatch(
                  """
                  INSERT INTO lineage_events (
                      event_time, event, event_type, run_uuid, job_name, job_namespace, producer)
                  VALUES (
                      :observedAt, CAST(:event AS JSONB), :eventType, :runId,
                      :jobName, :namespace, :producer)
                  """);
          for (int index = 0; index < flows.size(); index++) {
            FlowObservation observation = flows.get(index);
            eventBatch
                .bind("observedAt", observation.observedAt())
                .bind("event", "{\"verificationId\":" + index + "}")
                .bind("eventType", observation.eventType())
                .bind("runId", observation.runId())
                .bind("jobName", observation.jobName())
                .bind("namespace", observation.namespace())
                .bind("producer", "https://example.com/stats-query-verification")
                .add();
          }
          eventBatch.execute();
        });

    return new Fixture(List.copyOf(flows), jobs, datasets, sources);
  }

  private static List<StockObservation> stockFixture() {
    return List.of(
        new StockObservation(START_AT.minus(Duration.ofDays(1)), HOT_NAMESPACE),
        new StockObservation(START_AT, HOT_NAMESPACE),
        new StockObservation(START_AT.plus(Duration.ofMinutes(30)), COLD_NAMESPACE),
        new StockObservation(START_AT.plus(Duration.ofHours(1)), HOT_NAMESPACE),
        new StockObservation(END_AT.minusMillis(1), HOT_NAMESPACE),
        new StockObservation(END_AT, HOT_NAMESPACE));
  }

  private static UUID namespaceId(String namespace) {
    if (HOT_NAMESPACE.equals(namespace)) {
      return id("namespace-hot");
    }
    if (COLD_NAMESPACE.equals(namespace)) {
      return id("namespace-cold");
    }
    throw new IllegalArgumentException("Unknown namespace: " + namespace);
  }

  private static UUID id(String value) {
    return UUID.nameUUIDFromBytes(("stats-query-verification:" + value).getBytes(UTF_8));
  }

  private record Fixture(
      List<FlowObservation> flows,
      List<StockObservation> jobs,
      List<StockObservation> datasets,
      List<StockObservation> sources) {}

  private record FlowObservation(
      Instant observedAt, String eventType, String namespace, String jobName, UUID runId) {}

  private record StockObservation(Instant observedAt, String namespace) {}

  private record Bucket(Instant startAt, Instant endAt) {}

  private record PlanCase(String name, String sql, String relation, UnaryOperator<Query> bind) {}
}
