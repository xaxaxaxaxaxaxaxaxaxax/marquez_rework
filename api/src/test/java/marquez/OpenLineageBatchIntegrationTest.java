/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.ZonedDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import marquez.api.JdbiUtils;
import marquez.common.Utils;
import marquez.db.OpenLineageQueueDao;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Tag("IntegrationTests")
public class OpenLineageBatchIntegrationTest extends BaseIntegrationTest {
  private static final URI RUN_EVENT_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/RunEvent");
  private static final URI JOB_EVENT_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/JobEvent");
  private static final URI DATASET_EVENT_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/DatasetEvent");
  private static final ZonedDateTime EVENT_TIME = ZonedDateTime.parse("2026-08-15T00:00:00Z");

  private final Set<UUID> frozenOrderingKeys = new LinkedHashSet<>();
  private Jdbi jdbi;

  @BeforeEach
  void setUpDatabaseAccess() {
    jdbi = Jdbi.create(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  @AfterEach
  void tearDownBatchState() {
    try {
      thawFrozenHeads();
      awaitOpenLineageProjection(jdbi);
    } finally {
      frozenOrderingKeys.clear();
      JdbiUtils.cleanDatabase(jdbi);
    }
  }

  @Test
  void mixedRunJobAndDatasetBatchEventuallyProjects() throws Exception {
    UUID testId = UUID.randomUUID();
    UUID runId = UUID.randomUUID();
    String producer = "https://example.com/open-lineage-batch/" + testId;
    String runJobName = "batch-run-job-" + testId;
    String jobEventName = "batch-job-event-" + testId;
    String datasetName = "batch-dataset-event-" + testId;

    LineageEvent runEvent = runEvent(runId, "START", EVENT_TIME, producer, runJobName);
    JobEvent jobEvent = jobEvent(EVENT_TIME.plusSeconds(1), producer, jobEventName);
    DatasetEvent datasetEvent = datasetEvent(EVENT_TIME.plusSeconds(2), producer, datasetName);

    HttpResponse<String> response =
        sendLineageBatch(List.of(runEvent, jobEvent, datasetEvent)).get(5, TimeUnit.SECONDS);

    assertThat(response.statusCode()).isEqualTo(204);
    assertThat(response.body()).isEmpty();

    awaitOpenLineageProjection(jdbi);

    assertThat(rawEventVariants(producer))
        .containsExactlyInAnyOrder("RUN_EVENT", "JOB_EVENT", "DATASET_EVENT");
    assertThat(client.getRun(runId.toString())).isNotNull();
    assertThat(client.getJob(NAMESPACE_NAME, jobEventName)).isNotNull();
    assertThat(client.getDataset(NAMESPACE_NAME, datasetName)).isNotNull();
  }

  @Test
  void invalidMiddleEventRejectsEntireBatchWithoutWrites() throws Exception {
    UUID runId = UUID.randomUUID();
    String producer = "https://example.com/open-lineage-invalid-batch/" + runId;
    String jobName = "invalid-middle-batch-" + runId;
    LineageEvent start = runEvent(runId, "START", EVENT_TIME, producer, jobName);
    LineageEvent invalid = runEvent(runId, "OTHER", EVENT_TIME.plusSeconds(1), producer, jobName);
    LineageEvent complete =
        runEvent(runId, "COMPLETE", EVENT_TIME.plusSeconds(2), producer, jobName);
    LineageEvent.RunFacet facets = new LineageEvent.RunFacet();
    facets.setFacet("custom", Map.of("nested", List.of(Map.of("value", String.valueOf('\0')))));
    invalid.getRun().setFacets(facets);
    UUID orderingKey = OpenLineageQueueDao.orderingKeyFor(start);

    HttpResponse<String> response =
        sendLineageBatch(List.of(start, invalid, complete)).get(5, TimeUnit.SECONDS);

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(admissionCounts(orderingKey, runId)).isEqualTo(new AdmissionCounts(0, 0, 0));
  }

  @Test
  void acceptedSameLaneBatchReturnsBeforeProjectionAndQueuesInRequestOrder() throws Exception {
    UUID runId = UUID.randomUUID();
    String producer = "https://example.com/open-lineage-ordered-batch/" + runId;
    String jobName = "ordered-batch-" + runId;
    LineageEvent predecessor =
        runEvent(runId, "PREDECESSOR", EVENT_TIME.minusSeconds(1), producer, jobName);
    LineageEvent start = runEvent(runId, "START", EVENT_TIME, producer, jobName);
    LineageEvent other = runEvent(runId, "OTHER", EVENT_TIME.plusSeconds(1), producer, jobName);
    LineageEvent complete =
        runEvent(runId, "COMPLETE", EVENT_TIME.plusSeconds(2), producer, jobName);
    UUID orderingKey = OpenLineageQueueDao.orderingKeyFor(predecessor);
    long predecessorId = seedFrozenHead(orderingKey, predecessor);

    try {
      HttpResponse<String> response =
          sendLineageBatch(List.of(start, other, complete)).get(5, TimeUnit.SECONDS);

      assertThat(response.statusCode()).isEqualTo(204);
      assertThat(response.body()).isEmpty();

      LaneAdmissionState state = laneAdmissionState(orderingKey, runId);
      assertThat(state.headEventId()).isEqualTo(predecessorId);
      assertThat(state.frozenHead()).isTrue();
      assertThat(state.rawCount()).isZero();
      assertThat(state.queuedEventTypes())
          .containsExactly("PREDECESSOR", "START", "OTHER", "COMPLETE");
    } finally {
      thawFrozenHead(orderingKey);
    }
  }

  private CompletableFuture<HttpResponse<String>> sendLineageBatch(
      List<? extends BaseEvent> events) {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + "/api/v1/lineage/batch"))
            .header("Content-Type", "application/json")
            .POST(BodyPublishers.ofString(Utils.toJson(events)))
            .build();

    return http2.sendAsync(request, BodyHandlers.ofString());
  }

  private LineageEvent runEvent(
      UUID runId, String eventType, ZonedDateTime eventTime, String producer, String jobName) {
    return LineageEvent.builder()
        .eventType(eventType)
        .eventTime(eventTime)
        .run(new LineageEvent.Run(runId.toString(), null))
        .job(LineageEvent.Job.builder().namespace(NAMESPACE_NAME).name(jobName).build())
        .inputs(List.of())
        .outputs(List.of())
        .producer(producer)
        .schemaURL(RUN_EVENT_SCHEMA)
        .build();
  }

  private JobEvent jobEvent(ZonedDateTime eventTime, String producer, String jobName) {
    return JobEvent.builder()
        .eventTime(eventTime)
        .job(LineageEvent.Job.builder().namespace(NAMESPACE_NAME).name(jobName).build())
        .inputs(List.of())
        .outputs(List.of())
        .producer(producer)
        .schemaURL(JOB_EVENT_SCHEMA)
        .build();
  }

  private DatasetEvent datasetEvent(ZonedDateTime eventTime, String producer, String datasetName) {
    return DatasetEvent.builder()
        .eventTime(eventTime)
        .dataset(LineageEvent.Dataset.builder().namespace(NAMESPACE_NAME).name(datasetName).build())
        .producer(producer)
        .schemaURL(DATASET_EVENT_SCHEMA)
        .build();
  }

  private long seedFrozenHead(UUID orderingKey, LineageEvent predecessor) {
    long eventId =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        WITH inserted AS (
                          INSERT INTO open_lineage_queue (ordering_key, event)
                          VALUES (:orderingKey, :eventJson)
                          RETURNING ordering_key, id
                        ), created_head AS (
                          INSERT INTO open_lineage_queue_heads (
                              ordering_key, event_id, available_at)
                          SELECT ordering_key, id, 'infinity'::timestamptz
                          FROM inserted
                          RETURNING event_id
                        )
                        SELECT event_id
                        FROM created_head
                        """)
                    .bind("orderingKey", orderingKey)
                    .bind("eventJson", Utils.toJson(predecessor))
                    .mapTo(Long.class)
                    .one());
    frozenOrderingKeys.add(orderingKey);
    return eventId;
  }

  private void thawFrozenHead(UUID orderingKey) {
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    "UPDATE open_lineage_queue_heads "
                        + "SET available_at = '-infinity'::timestamptz "
                        + "WHERE ordering_key = :orderingKey "
                        + "AND available_at = 'infinity'::timestamptz")
                .bind("orderingKey", orderingKey)
                .execute());
    frozenOrderingKeys.remove(orderingKey);
  }

  private void thawFrozenHeads() {
    for (UUID orderingKey : List.copyOf(frozenOrderingKeys)) {
      thawFrozenHead(orderingKey);
    }
  }

  private AdmissionCounts admissionCounts(UUID orderingKey, UUID runId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT
                      (SELECT count(*)
                         FROM open_lineage_queue
                        WHERE ordering_key = :orderingKey) AS live_count,
                      (SELECT count(*)
                         FROM open_lineage_queue_heads
                        WHERE ordering_key = :orderingKey) AS head_count,
                      (SELECT count(*)
                         FROM lineage_events
                        WHERE run_uuid = :runId) AS raw_count
                    """)
                .bind("orderingKey", orderingKey)
                .bind("runId", runId)
                .map(
                    (resultSet, context) ->
                        new AdmissionCounts(
                            resultSet.getLong("live_count"),
                            resultSet.getLong("head_count"),
                            resultSet.getLong("raw_count")))
                .one());
  }

  private LaneAdmissionState laneAdmissionState(UUID orderingKey, UUID runId) {
    return jdbi.withHandle(
        handle -> {
          Optional<HeadState> head =
              handle
                  .createQuery(
                      """
                      SELECT event_id,
                             available_at = 'infinity'::timestamptz AS frozen
                      FROM open_lineage_queue_heads
                      WHERE ordering_key = :orderingKey
                      """)
                  .bind("orderingKey", orderingKey)
                  .map(
                      (resultSet, context) ->
                          new HeadState(
                              resultSet.getLong("event_id"), resultSet.getBoolean("frozen")))
                  .findOne();
          List<String> eventTypes =
              handle
                  .createQuery(
                      """
                      SELECT event::jsonb ->> 'eventType'
                      FROM open_lineage_queue
                      WHERE ordering_key = :orderingKey
                      ORDER BY id
                      """)
                  .bind("orderingKey", orderingKey)
                  .mapTo(String.class)
                  .list();
          long rawCount =
              handle
                  .createQuery("SELECT count(*) FROM lineage_events WHERE run_uuid = :runId")
                  .bind("runId", runId)
                  .mapTo(Long.class)
                  .one();
          return new LaneAdmissionState(
              head.map(HeadState::eventId).orElse(null),
              head.map(HeadState::frozen).orElse(false),
              eventTypes,
              rawCount);
        });
  }

  private List<String> rawEventVariants(String producer) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT _event_type FROM lineage_events WHERE producer = :producer")
                .bind("producer", producer)
                .mapTo(String.class)
                .list());
  }

  private record AdmissionCounts(long liveCount, long headCount, long rawCount) {}

  private record HeadState(long eventId, boolean frozen) {}

  private record LaneAdmissionState(
      Long headEventId, boolean frozenHead, List<String> queuedEventTypes, long rawCount) {}
}
