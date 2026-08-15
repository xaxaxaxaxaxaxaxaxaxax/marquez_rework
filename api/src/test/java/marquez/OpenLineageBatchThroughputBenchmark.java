/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Opt-in admission-throughput benchmark for single-event requests versus 128-event batches.
 *
 * <p>Run with {@code -DrunOpenLineageBatchThroughputBenchmark=true}. New queue heads are scheduled
 * at infinity during the benchmark, preventing projection claims and writes. The running worker
 * still performs empty polls, so its small database and CPU cost remains in the measured HTTP
 * admission path. The client count, warm-up events, measured events per cell, and trial count can
 * be overridden with {@code openLineageBatchBenchmark.*} system properties.
 */
@Tag("IntegrationTests")
@EnabledIfSystemProperty(named = "runOpenLineageBatchThroughputBenchmark", matches = "true")
public class OpenLineageBatchThroughputBenchmark extends BaseIntegrationTest {
  private static final int BATCH_SIZE = 128;
  private static final int CLIENTS = Integer.getInteger("openLineageBatchBenchmark.clients", 4);
  private static final int WARMUP_EVENTS =
      Integer.getInteger("openLineageBatchBenchmark.warmupEvents", 8192);
  private static final int EVENTS_PER_CELL =
      Integer.getInteger("openLineageBatchBenchmark.eventsPerCell", 16384);
  private static final int TRIALS = Integer.getInteger("openLineageBatchBenchmark.trials", 7);
  private static final long CELL_TIMEOUT_SECONDS = 180;

  @Test
  void compareSingleAndBatch128AdmissionThroughput() throws Exception {
    assertThat(CLIENTS).isPositive();
    assertThat(TRIALS).isPositive();
    assertThat(WARMUP_EVENTS).isPositive();
    assertThat(WARMUP_EVENTS % BATCH_SIZE).isZero();
    assertThat(EVENTS_PER_CELL).isPositive();
    assertThat(EVENTS_PER_CELL % BATCH_SIZE).isZero();

    Jdbi jdbi = Jdbi.create(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    ExecutorService clients = Executors.newFixedThreadPool(CLIENTS);
    List<Double> singleThroughputs = new ArrayList<>(TRIALS);
    List<Double> batchThroughputs = new ArrayList<>(TRIALS);
    List<Double> pairedSpeedups = new ArrayList<>(TRIALS);

    boolean queueHeadsFrozen = false;
    try {
      freezeNewQueueHeads(jdbi);
      queueHeadsFrozen = true;
      runCell(jdbi, clients, 1, WARMUP_EVENTS, "warmup");
      runCell(jdbi, clients, BATCH_SIZE, WARMUP_EVENTS, "warmup");

      for (int trial = 1; trial <= TRIALS; trial++) {
        TimedCount single;
        TimedCount batch;
        String order;
        String fixtureKey = "trial-" + trial;
        if ((trial & 1) == 1) {
          order = "single,batch";
          single = runCell(jdbi, clients, 1, EVENTS_PER_CELL, fixtureKey);
          batch = runCell(jdbi, clients, BATCH_SIZE, EVENTS_PER_CELL, fixtureKey);
        } else {
          order = "batch,single";
          batch = runCell(jdbi, clients, BATCH_SIZE, EVENTS_PER_CELL, fixtureKey);
          single = runCell(jdbi, clients, 1, EVENTS_PER_CELL, fixtureKey);
        }

        double singleThroughput = single.perSecond();
        double batchThroughput = batch.perSecond();
        double pairedSpeedup = batchThroughput / singleThroughput;
        singleThroughputs.add(singleThroughput);
        batchThroughputs.add(batchThroughput);
        pairedSpeedups.add(pairedSpeedup);

        System.out.printf(
            Locale.ROOT,
            "OPENLINEAGE_BATCH_THROUGHPUT trial=%d order=%s clients=%d events=%d "
                + "single_events_per_second=%.1f batch128_events_per_second=%.1f "
                + "paired_speedup=%.2fx%n",
            trial,
            order,
            CLIENTS,
            EVENTS_PER_CELL,
            singleThroughput,
            batchThroughput,
            pairedSpeedup);
      }

      Distribution singleDistribution = Distribution.of(singleThroughputs);
      Distribution batchDistribution = Distribution.of(batchThroughputs);
      Distribution speedupDistribution = Distribution.of(pairedSpeedups);
      System.out.printf(
          Locale.ROOT,
          "OPENLINEAGE_BATCH_THROUGHPUT_SUMMARY clients=%d events_per_cell=%d trials=%d "
              + "single_median_events_per_second=%.1f single_range=%.1f..%.1f "
              + "batch128_median_events_per_second=%.1f batch128_range=%.1f..%.1f "
              + "paired_median_speedup=%.2fx%n",
          CLIENTS,
          EVENTS_PER_CELL,
          TRIALS,
          singleDistribution.median(),
          singleDistribution.minimum(),
          singleDistribution.maximum(),
          batchDistribution.median(),
          batchDistribution.minimum(),
          batchDistribution.maximum(),
          speedupDistribution.median());
    } finally {
      try {
        if (queueHeadsFrozen) {
          try {
            cleanQueue(jdbi);
          } finally {
            restoreQueueHeadDefault(jdbi);
          }
        }
      } finally {
        clients.shutdownNow();
        assertThat(clients.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      }
    }
  }

  private TimedCount runCell(
      Jdbi jdbi, ExecutorService clients, int batchSize, int eventCount, String fixtureKey)
      throws Exception {
    cleanQueue(jdbi);
    List<String> eventJsons = eventJsons(eventCount, fixtureKey);
    List<String> requestBodies = requestBodies(eventJsons, batchSize);
    List<List<HttpRequest>> assignments = requestAssignments(requestBodies, batchSize);
    int expectedStatus = batchSize == 1 ? 201 : 204;
    CountDownLatch ready = new CountDownLatch(CLIENTS);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Integer>> results = new ArrayList<>(CLIENTS);

    for (List<HttpRequest> assignment : assignments) {
      results.add(
          clients.submit(
              () -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) {
                  throw new AssertionError("timed out waiting to start benchmark cell");
                }
                int accepted = 0;
                for (HttpRequest request : assignment) {
                  HttpResponse<String> response = http2.send(request, BodyHandlers.ofString());
                  if (response.statusCode() != expectedStatus) {
                    throw new AssertionError(
                        "expected HTTP "
                            + expectedStatus
                            + " but received "
                            + response.statusCode()
                            + ": "
                            + response.body());
                  }
                  accepted++;
                }
                return accepted;
              }));
    }

    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
    long startedAt = System.nanoTime();
    start.countDown();
    int acceptedRequests = 0;
    for (Future<Integer> result : results) {
      acceptedRequests += result.get(CELL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    long elapsedNanos = System.nanoTime() - startedAt;

    assertThat(acceptedRequests).isEqualTo(requestBodies.size());
    QueueSnapshot expectedQueue =
        batchSize == 1
            ? QueueSnapshot.singular(eventCount)
            : QueueSnapshot.batch(eventCount, requestBodies.size(), batchSize);
    assertThat(QueueSnapshot.read(jdbi)).isEqualTo(expectedQueue);
    return new TimedCount(eventCount, elapsedNanos);
  }

  private List<List<HttpRequest>> requestAssignments(List<String> bodies, int batchSize) {
    URI endpoint =
        URI.create(baseUrl + (batchSize == 1 ? "/api/v1/lineage" : "/api/v1/lineage/batch"));
    List<List<HttpRequest>> assignments = new ArrayList<>(CLIENTS);
    for (int client = 0; client < CLIENTS; client++) {
      assignments.add(new ArrayList<>());
    }
    for (int request = 0; request < bodies.size(); request++) {
      assignments
          .get(request % CLIENTS)
          .add(
              HttpRequest.newBuilder()
                  .uri(endpoint)
                  .header("Content-Type", "application/json")
                  .POST(BodyPublishers.ofString(bodies.get(request)))
                  .build());
    }
    return assignments;
  }

  private static List<String> eventJsons(int eventCount, String fixtureKey) {
    List<String> events = new ArrayList<>(eventCount);
    for (int event = 0; event < eventCount; event++) {
      UUID runId = UUID.nameUUIDFromBytes((fixtureKey + ':' + event).getBytes(UTF_8));
      events.add(
          "{\"eventTime\":\"2026-08-15T00:00:00Z\","
              + "\"run\":{\"runId\":\""
              + runId
              + "\"},"
              + "\"job\":{\"namespace\":\"benchmark\",\"name\":\"batch-throughput\"},"
              + "\"producer\":\"https://example.com/marquez-batch-benchmark\","
              + "\"schemaURL\":\"https://openlineage.io/spec/2-0-0/"
              + "OpenLineage.json#/definitions/RunEvent\"}");
    }
    return events;
  }

  private static List<String> requestBodies(List<String> eventJsons, int batchSize) {
    if (batchSize == 1) {
      return eventJsons;
    }
    List<String> batches = new ArrayList<>(eventJsons.size() / batchSize);
    for (int offset = 0; offset < eventJsons.size(); offset += batchSize) {
      batches.add('[' + String.join(",", eventJsons.subList(offset, offset + batchSize)) + ']');
    }
    return batches;
  }

  private static void freezeNewQueueHeads(Jdbi jdbi) {
    jdbi.useHandle(
        handle ->
            handle.execute(
                "ALTER TABLE open_lineage_queue_heads ALTER COLUMN available_at "
                    + "SET DEFAULT 'infinity'::timestamptz"));
  }

  private static void restoreQueueHeadDefault(Jdbi jdbi) {
    jdbi.useHandle(
        handle ->
            handle.execute(
                "ALTER TABLE open_lineage_queue_heads ALTER COLUMN available_at "
                    + "SET DEFAULT date_trunc('milliseconds', clock_timestamp())"));
  }

  private static void cleanQueue(Jdbi jdbi) {
    jdbi.useHandle(
        handle ->
            handle.execute(
                "TRUNCATE open_lineage_queue_heads, open_lineage_queue, "
                    + "open_lineage_dead_letters RESTART IDENTITY"));
  }
}
