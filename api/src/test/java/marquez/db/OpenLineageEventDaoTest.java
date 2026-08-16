/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import marquez.common.Utils;
import marquez.db.OpenLineageEventDao.OpenLineageEventWrite;
import marquez.db.OpenLineageEventDao.SpecEventType;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class OpenLineageEventDaoTest {
  private static final Instant EVENT_TIME = Instant.parse("2026-08-16T01:02:03.456Z");
  private static final String PRODUCER = "https://example.com/producer";

  private static Jdbi jdbi;
  private static OpenLineageEventDao dao;

  @BeforeAll
  static void setUpOnce(Jdbi configuredJdbi) {
    jdbi = configuredJdbi;
    dao = jdbi.onDemand(OpenLineageEventDao.class);
  }

  @BeforeEach
  void clearEvents() {
    jdbi.useHandle(handle -> handle.execute("DELETE FROM lineage_events"));
  }

  @Test
  void singletonAndBulkWritesPreserveRawJsonMixedOrderAndNullableColumns() {
    UUID runUuid = UUID.randomUUID();
    String runJson = runJson(runUuid, "COMPLETE", "run-job");
    String datasetJson = "{\"marker\":\"dataset-singleton\"}";
    String jobJson = "{\"marker\":\"job-singleton\"}";

    dao.createLineageEvent("COMPLETE", EVENT_TIME, runUuid, "run-job", "run-ns", runJson, PRODUCER);
    dao.createDatasetEvent(EVENT_TIME.plusMillis(1), datasetJson, PRODUCER);
    dao.createJobEvent(EVENT_TIME.plusMillis(2), "job-event", "job-ns", jobJson, PRODUCER);

    assertThat(storedEvents())
        .containsExactly(
            stored("RUN_EVENT", "COMPLETE", runUuid, "run-job", "run-ns", runJson),
            stored("DATASET_EVENT", null, null, null, null, datasetJson),
            stored("JOB_EVENT", null, null, "job-event", "job-ns", jobJson));
    assertRunReads(runUuid, 3);
    clearEvents();

    UUID firstRunUuid = UUID.randomUUID();
    UUID secondRunUuid = UUID.randomUUID();
    String firstRunJson = runJson(firstRunUuid, "START", "first-run");
    String bulkDatasetJson = "{\"marker\":\"dataset-bulk\",\"sequence\":2}";
    String bulkJobJson = "{\"marker\":\"job-bulk\",\"sequence\":3}";
    String secondRunJson = runJson(secondRunUuid, "COMPLETE", "second-run");

    int inserted =
        dao.createLineageEvents(
            List.of(
                runWrite("START", firstRunUuid, "first-run", firstRunJson),
                OpenLineageEventWrite.dataset(EVENT_TIME, bulkDatasetJson, PRODUCER),
                OpenLineageEventWrite.job(EVENT_TIME, "job-event", "job-ns", bulkJobJson, PRODUCER),
                runWrite("COMPLETE", secondRunUuid, "second-run", secondRunJson)));

    assertThat(inserted).isEqualTo(4);
    assertThat(storedEvents())
        .containsExactly(
            stored("RUN_EVENT", "START", firstRunUuid, "first-run", "run-ns", firstRunJson),
            stored("DATASET_EVENT", null, null, null, null, bulkDatasetJson),
            stored("JOB_EVENT", null, null, "job-event", "job-ns", bulkJobJson),
            stored("RUN_EVENT", "COMPLETE", secondRunUuid, "second-run", "run-ns", secondRunJson));
  }

  @Test
  void bulkAndWriteFactoriesValidateShapeBeforeSql() {
    OpenLineageEventWrite write =
        OpenLineageEventWrite.dataset(EVENT_TIME, "{\"marker\":\"valid\"}", PRODUCER);
    List<OpenLineageEventWrite> containsNull = new java.util.ArrayList<>();
    containsNull.add(write);
    containsNull.add(null);
    List<OpenLineageEventWrite> changesSize =
        new AbstractList<>() {
          private int sizeCalls;

          @Override
          public OpenLineageEventWrite get(int index) {
            return write;
          }

          @Override
          public int size() {
            return sizeCalls++ == 0 ? 1 : 0;
          }
        };

    assertInvalid(null, "events are required");
    assertInvalid(List.of(), "events must not be empty");
    assertInvalid(
        Collections.nCopies(OpenLineageEventDao.MAX_EVENTS_PER_INSERT + 1, write),
        "events must not exceed 1000");
    assertInvalid(containsNull, "event is required");
    assertInvalid(changesSize, "events changed while being read");

    assertThat(storedEvents()).isEmpty();

    assertThat(runWrite(null, UUID.randomUUID(), "job", "{}").eventType()).isEmpty();
    assertThatThrownBy(
            () ->
                new OpenLineageEventWrite(
                    SpecEventType.DATASET_EVENT,
                    null,
                    EVENT_TIME,
                    null,
                    "unexpected-job",
                    null,
                    "{}",
                    PRODUCER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("jobName is not valid for DATASET_EVENT");
  }

  private static void assertRunReads(UUID runUuid, int totalCount) {
    var from = EVENT_TIME.minusSeconds(1).atZone(UTC);
    var through = EVENT_TIME.plusSeconds(1).atZone(UTC);
    assertThat(
            List.of(
                dao.findLineageEventsByRunUuid(runUuid),
                dao.getAllLineageEventsAsc(through, from, 10, 0),
                dao.getAllLineageEventsDesc(through, from, 10, 0)))
        .allSatisfy(
            events ->
                assertThat(events)
                    .singleElement()
                    .extracting(LineageEvent::getEventType)
                    .isEqualTo("COMPLETE"));
    assertThat(dao.getAllLineageTotalCount(through, from)).isEqualTo(totalCount);
  }

  private static String runJson(UUID runUuid, String eventType, String jobName) {
    return """
        {
          "eventType": "%s",
          "eventTime": "%s",
          "run": {"runId": "%s"},
          "job": {"namespace": "run-ns", "name": "%s"},
          "producer": "%s"
        }
        """
        .formatted(eventType, EVENT_TIME, runUuid, jobName, PRODUCER);
  }

  private static void assertInvalid(@Nullable List<OpenLineageEventWrite> events, String message) {
    assertThatThrownBy(() -> dao.createLineageEvents(events))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(message);
  }

  private static OpenLineageEventWrite runWrite(
      String eventType, UUID runUuid, String jobName, String json) {
    return OpenLineageEventWrite.run(
        eventType, EVENT_TIME, runUuid, jobName, "run-ns", json, PRODUCER);
  }

  private static List<JsonNode> storedEvents() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT jsonb_build_array(_event_type, event_type, run_uuid, job_name, "
                        + "job_namespace, event) FROM lineage_events ORDER BY ctid")
                .map((row, context) -> readJson(row.getString(1)))
                .list());
  }

  private static JsonNode stored(
      String specType, String eventType, UUID runUuid, String job, String namespace, String json) {
    return readJson(
        Utils.toJson(Arrays.asList(specType, eventType, runUuid, job, namespace, readJson(json))));
  }

  private static JsonNode readJson(String json) {
    return Utils.fromJson(json, new TypeReference<>() {});
  }
}
