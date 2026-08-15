/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.api.models.ApiModelGenerator.newRunEvents;
import static marquez.common.models.CommonModelGenerator.newJobName;
import static marquez.common.models.CommonModelGenerator.newNamespaceName;
import static marquez.db.models.DbModelGenerator.newDatasetRowWith;
import static marquez.db.models.DbModelGenerator.newJobRowWith;
import static marquez.db.models.DbModelGenerator.newNamespaceRow;
import static marquez.db.models.DbModelGenerator.newSourceRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import marquez.db.mappers.MetricPointRowMapper;
import marquez.db.models.DatasetRow;
import marquez.db.models.IntervalMetric;
import marquez.db.models.JobRow;
import marquez.db.models.LineageMetric;
import marquez.db.models.MetricPoint;
import marquez.db.models.NamespaceRow;
import marquez.db.models.SourceRow;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.jackson2.Jackson2Plugin;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.QueryTimeOut;
import org.jdbi.v3.testing.junit5.JdbiExtension;
import org.jdbi.v3.testing.junit5.tc.JdbiTestcontainersExtension;
import org.junit.jupiter.api.AfterEach;
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
public class StatsTest {
  static final DockerImageName POSTGRES_14 = DockerImageName.parse("postgres:14");

  @Container
  @Order(1)
  static final PostgreSQLContainer<?> DB_CONTAINER = new PostgreSQLContainer<>(POSTGRES_14);

  // Defined statically to significantly improve overall test execution.
  @RegisterExtension
  @Order(2)
  static final JdbiExtension jdbiExtension =
      JdbiTestcontainersExtension.instance(DB_CONTAINER)
          .withPlugin(new SqlObjectPlugin())
          .withPlugin(new PostgresPlugin())
          .withPlugin(new Jackson2Plugin())
          .withInitializer(
              (source, handle) -> {
                // Apply migrations.
                DbMigration.migrateDbOrError(source);
              });

  // Wraps test database connection.
  static TestingDb DB;

  @BeforeAll
  public static void setUpOnce() {
    // Wrap jdbi configured for running container.
    DB = TestingDb.newInstance(jdbiExtension.getJdbi());
  }

  @AfterEach
  public void tearDown() {
    try (final Handle handle = DB.open()) {
      handle.execute("DELETE FROM lineage_events");
      handle.execute("DELETE FROM job_versions");
      handle.execute("DELETE FROM jobs");
      handle.execute("DELETE FROM datasets");
      handle.execute("DELETE FROM sources");
      handle.execute("DELETE FROM namespaces");
    }
  }

  @Test
  void queryLineageEventsUsesAnchoredDenseHalfOpenBucketsAndReportedScopes() {
    Instant startAt = Instant.parse("2025-01-01T00:07:00Z");
    Instant endAt = Instant.parse("2025-01-01T00:50:00Z");
    Duration rollup = Duration.ofMinutes(17);
    UUID firstRun = UUID.fromString("11111111-1111-1111-1111-111111111111");
    UUID secondRun = UUID.fromString("22222222-2222-2222-2222-222222222222");

    insertLineageEvent(startAt, "START", "namespace-a", "job-a", firstRun, 1);
    insertLineageEvent(
        startAt.plus(Duration.ofMinutes(17)), "START", "namespace-a", "job-a", firstRun, 2);
    insertLineageEvent(
        startAt.plus(Duration.ofMinutes(33)).plusSeconds(59),
        "START",
        "namespace-a",
        "job-b",
        secondRun,
        3);
    insertLineageEvent(
        startAt.plus(Duration.ofMinutes(23)), "START", "namespace-b", "job-a", secondRun, 4);
    insertLineageEvent(endAt, "START", "namespace-a", "job-a", firstRun, 5);
    insertLineageEvent(
        startAt.plus(Duration.ofMinutes(23)), "FAIL", "namespace-a", "job-a", firstRun, 6);

    List<MetricPoint> global =
        DB.queryLineageEventsGlobal("START", startAt, endAt, rollup.toMillis());
    List<MetricPoint> namespace =
        DB.queryLineageEventsForNamespace(
            "START", "namespace-a", startAt, endAt, rollup.toMillis());
    List<MetricPoint> job =
        DB.queryLineageEventsForJob(
            "START", "namespace-a", "job-a", startAt, endAt, rollup.toMillis());
    List<MetricPoint> run =
        DB.queryLineageEventsForRun("START", firstRun, startAt, endAt, rollup.toMillis());
    List<MetricPoint> failures =
        DB.queryLineageEventsGlobal("FAIL", startAt, endAt, rollup.toMillis());

    Instant firstEnd = startAt.plus(rollup);
    Instant secondEnd = firstEnd.plus(rollup);
    assertThat(global)
        .containsExactly(
            new MetricPoint(startAt, firstEnd, 1),
            new MetricPoint(firstEnd, secondEnd, 3),
            new MetricPoint(secondEnd, endAt, 0));
    assertThat(namespace).extracting(MetricPoint::getValue).containsExactly(1L, 2L, 0L);
    assertThat(job).extracting(MetricPoint::getValue).containsExactly(1L, 1L, 0L);
    assertThat(run).extracting(MetricPoint::getValue).containsExactly(1L, 1L, 0L);
    assertThat(failures).extracting(MetricPoint::getValue).containsExactly(0L, 1L, 0L);
  }

  @Test
  void queryStocksProjectsCurrentlyRetainedRowsFromCreatedAt() {
    Instant startAt = Instant.parse("2025-02-01T00:00:00Z");
    Instant endAt = startAt.plus(Duration.ofHours(3));
    long bucketMillis = Duration.ofHours(1).toMillis();
    NamespaceRow firstNamespace = DB.upsert(newNamespaceRow());
    NamespaceRow secondNamespace = DB.upsert(newNamespaceRow());

    SourceRow baselineSource = DB.upsert(sourceAt(startAt.minusSeconds(1)));
    DB.upsert(sourceAt(startAt));
    DB.upsert(sourceAt(startAt.plus(Duration.ofHours(1))));
    DB.upsert(sourceAt(endAt));

    DB.upsert(
        newJobRowWith(startAt.minusSeconds(1), firstNamespace.getUuid(), firstNamespace.getName()));
    DB.upsert(newJobRowWith(startAt, firstNamespace.getUuid(), firstNamespace.getName()));
    DB.upsert(
        newJobRowWith(
            startAt.plus(Duration.ofHours(1)),
            secondNamespace.getUuid(),
            secondNamespace.getName()));
    DB.upsert(newJobRowWith(endAt, firstNamespace.getUuid(), firstNamespace.getName()));

    DB.upsert(
        newDatasetRowWith(
            startAt.minusSeconds(1),
            firstNamespace.getUuid(),
            firstNamespace.getName(),
            baselineSource.getUuid(),
            baselineSource.getName()));
    DB.upsert(
        newDatasetRowWith(
            startAt,
            firstNamespace.getUuid(),
            firstNamespace.getName(),
            baselineSource.getUuid(),
            baselineSource.getName()));
    DB.upsert(
        newDatasetRowWith(
            startAt.plus(Duration.ofHours(1)),
            secondNamespace.getUuid(),
            secondNamespace.getName(),
            baselineSource.getUuid(),
            baselineSource.getName()));
    DB.upsert(
        newDatasetRowWith(
            endAt,
            firstNamespace.getUuid(),
            firstNamespace.getName(),
            baselineSource.getUuid(),
            baselineSource.getName()));

    assertThat(DB.queryJobsGlobal(startAt, endAt, bucketMillis))
        .extracting(MetricPoint::getValue)
        .containsExactly(2L, 3L, 3L);
    assertThat(DB.queryJobsForNamespace(firstNamespace.getName(), startAt, endAt, bucketMillis))
        .extracting(MetricPoint::getValue)
        .containsExactly(2L, 2L, 2L);
    assertThat(DB.queryDatasetsGlobal(startAt, endAt, bucketMillis))
        .extracting(MetricPoint::getValue)
        .containsExactly(2L, 3L, 3L);
    assertThat(DB.queryDatasetsForNamespace(firstNamespace.getName(), startAt, endAt, bucketMillis))
        .extracting(MetricPoint::getValue)
        .containsExactly(2L, 2L, 2L);
    assertThat(DB.querySourcesGlobal(startAt, endAt, bucketMillis))
        .extracting(MetricPoint::getValue)
        .containsExactly(2L, 3L, 3L);
  }

  @Test
  void metricPointMapperPreservesBigintValues() {
    try (Handle handle = DB.open()) {
      MetricPoint point =
          handle
              .createQuery(
                  """
                  SELECT TIMESTAMPTZ '2025-01-01T00:00:00Z' AS start_at,
                         TIMESTAMPTZ '2025-01-01T01:00:00Z' AS end_at,
                         4000000000::BIGINT AS value
                  """)
              .map(new MetricPointRowMapper())
              .one();

      assertThat(point.getValue()).isEqualTo(4_000_000_000L);
    }
  }

  @Test
  void everyBespokeQueryHasFiveSecondTimeout() {
    List<java.lang.reflect.Method> queryMethods =
        Arrays.stream(StatsDao.class.getDeclaredMethods())
            .filter(method -> method.getName().startsWith("query"))
            .toList();

    assertThat(queryMethods).hasSize(9);
    assertThat(queryMethods)
        .allSatisfy(
            method -> {
              QueryTimeOut queryTimeOut = method.getAnnotation(QueryTimeOut.class);
              assertThat(queryTimeOut).isNotNull();
              assertThat(queryTimeOut.value()).isEqualTo(5);
            });
  }

  @Test
  public void testGetStatsForLineageEvents() {
    // (1) Configure OL.
    final URI olProducer = URI.create("https://test.com/test");
    final OpenLineage ol = new OpenLineage(olProducer);

    // (2) Add namespace and job for OL events.
    final String namespaceName = newNamespaceName().getValue();
    final String jobName = newJobName().getValue();

    // (3) Create some 1 hour old OL events.
    int hourEvents = 4;
    final Set<OpenLineage.RunEvent> hourEventSet =
        newRunEvents(
            ol, Instant.now().minus(1, ChronoUnit.HOURS), namespaceName, jobName, hourEvents);
    DB.insertAll(hourEventSet);

    // (4) Create some 2 day old OL events.
    int dayEvents = 2;
    final Set<OpenLineage.RunEvent> dayEventSet =
        newRunEvents(
            ol, Instant.now().minus(2, ChronoUnit.DAYS), namespaceName, jobName, dayEvents);
    DB.insertAll(dayEventSet);

    // (5) Create some 10 second old OL events.
    int secondEvents = 1;
    final Set<OpenLineage.RunEvent> secondEventSet =
        newRunEvents(
            ol, Instant.now().minus(10, ChronoUnit.SECONDS), namespaceName, jobName, secondEvents);
    DB.insertAll(secondEventSet);

    // (6) Materialize views to flush out view data.
    try (final Handle handle = DB.open()) {
      DbTestUtils.materializeViews(handle);
    } catch (Exception e) {
      fail("failed to apply dry run", e);
    }

    List<LineageMetric> lastDayLineageMetrics = DB.lastDayLineageMetrics();
    List<LineageMetric> lastWeekLineageMetrics = DB.lastWeekLineageMetrics("UTC");

    assertThat(lastDayLineageMetrics).isNotEmpty();
    assertThat(lastDayLineageMetrics.get(lastDayLineageMetrics.size() - 2).getComplete())
        .isEqualTo(hourEvents);
    assertThat(lastDayLineageMetrics.get(lastDayLineageMetrics.size() - 1).getComplete())
        .isEqualTo(secondEvents);

    assertThat(lastWeekLineageMetrics).isNotEmpty();
    assertThat(lastWeekLineageMetrics.get(lastWeekLineageMetrics.size() - 3).getComplete())
        .isEqualTo(dayEvents);
    assertThat(lastWeekLineageMetrics.get(lastWeekLineageMetrics.size() - 1).getComplete())
        .isEqualTo(secondEvents + hourEvents);
  }

  @Test
  public void testGetStatsForJobs() {

    // (1) Insert a new namespace.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());

    // (2) Insert a new job.
    final JobRow jobRow = DB.upsert(newJobRowWith(namespaceRow.getUuid(), namespaceRow.getName()));
    DB.upsert(jobRow);

    // (3) Retrieve last day and last week job metrics.
    List<IntervalMetric> intervalMetricsDay = DB.lastDayJobMetrics();
    assertThat(intervalMetricsDay).isNotEmpty();

    Optional<Integer> countDay =
        intervalMetricsDay.stream().map(IntervalMetric::getCount).reduce(Integer::sum);
    assertThat(countDay).isPresent();
    assertThat(countDay.get()).isEqualTo(1);

    List<IntervalMetric> intervalMetricsWeek = DB.lastWeekJobMetrics("UTC");
    assertThat(intervalMetricsWeek).isNotEmpty();

    Optional<Integer> countWeek =
        intervalMetricsWeek.stream().map(IntervalMetric::getCount).reduce(Integer::sum);
    assertThat(countWeek).isPresent();
    assertThat(countWeek.get()).isEqualTo(1);
  }

  @Test
  public void testGetStatsForDatasets() {
    // (1) Insert a new namespace.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());

    // (2) Insert a new source.
    final SourceRow sourceRow = DB.upsert(newSourceRow());
    DB.upsert(sourceRow);

    // (3) Insert a new dataset.
    final DatasetRow datasetRow =
        DB.upsert(
            newDatasetRowWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName()));
    DB.upsert(datasetRow);

    // (4) Retrieve last day and last week dataset metrics.
    List<IntervalMetric> intervalMetricsDay = DB.lastDayDatasetMetrics();
    assertThat(intervalMetricsDay).isNotEmpty();

    Optional<Integer> countDay =
        intervalMetricsDay.stream().map(IntervalMetric::getCount).reduce(Integer::sum);
    assertThat(countDay).isPresent();
    assertThat(countDay.get()).isEqualTo(1);

    List<IntervalMetric> intervalMetricsWeek = DB.lastWeekDatasetMetrics("UTC");
    assertThat(intervalMetricsWeek).isNotEmpty();

    Optional<Integer> countWeek =
        intervalMetricsWeek.stream().map(IntervalMetric::getCount).reduce(Integer::sum);
    assertThat(countWeek).isPresent();
    assertThat(countWeek.get()).isEqualTo(1);
  }

  @Test
  public void testGetStatsForSources() {

    // (1) Insert a new source.
    final SourceRow sourceRow = DB.upsert(newSourceRow());
    DB.upsert(sourceRow);

    // (2) Retrieve last day source metrics.
    List<IntervalMetric> intervalMetricsDay = DB.lastDaySourceMetrics();
    assertThat(intervalMetricsDay).isNotEmpty();

    Optional<Integer> countDay =
        intervalMetricsDay.stream().map(IntervalMetric::getCount).reduce(Integer::sum);
    assertThat(countDay).isPresent();
    assertThat(countDay.get()).isGreaterThanOrEqualTo(1);

    List<IntervalMetric> intervalMetricsWeek = DB.lastWeekSourceMetrics("UTC");

    Optional<Integer> countWeek =
        intervalMetricsWeek.stream().map(IntervalMetric::getCount).reduce(Integer::sum);
    assertThat(countWeek).isPresent();
    assertThat(countWeek.get()).isEqualTo(1);
  }

  private void insertLineageEvent(
      Instant eventTime,
      String eventType,
      String namespace,
      String jobName,
      UUID runId,
      int ordinal) {
    try (Handle handle = DB.open()) {
      handle
          .createUpdate(
              """
              INSERT INTO lineage_events (
                  event_time,
                  event,
                  event_type,
                  run_uuid,
                  job_name,
                  job_namespace,
                  producer,
                  _event_type)
              VALUES (
                  :eventTime,
                  jsonb_build_object('ordinal', :ordinal),
                  :eventType,
                  :runId,
                  :jobName,
                  :namespace,
                  'https://stats.test/producer',
                  NULL)
              """)
          .bind("eventTime", eventTime)
          .bind("eventType", eventType)
          .bind("runId", runId)
          .bind("jobName", jobName)
          .bind("namespace", namespace)
          .bind("ordinal", ordinal)
          .execute();
    }
  }

  private static SourceRow sourceAt(Instant createdAt) {
    SourceRow generated = newSourceRow();
    return new SourceRow(
        generated.getUuid(),
        generated.getType(),
        createdAt,
        createdAt,
        generated.getName(),
        generated.getConnectionUrl(),
        generated.getDescription().orElse(null));
  }
}
