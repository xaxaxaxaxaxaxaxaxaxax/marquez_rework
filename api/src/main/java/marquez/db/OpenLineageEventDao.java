/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import marquez.db.mappers.LineageEventMapper;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

/** Stores and retrieves the raw OpenLineage events received by Marquez. */
@RegisterRowMapper(LineageEventMapper.class)
public interface OpenLineageEventDao {
  int MAX_EVENTS_PER_INSERT = 1000;

  enum SpecEventType {
    RUN_EVENT,
    DATASET_EVENT,
    JOB_EVENT
  }

  /** A validated raw-event write whose JSON was serialized by the intake boundary exactly once. */
  record OpenLineageEventWrite(
      SpecEventType specEventType,
      @Nullable String eventType,
      Instant eventTime,
      @Nullable UUID runUuid,
      @Nullable String jobName,
      @Nullable String jobNamespace,
      String eventJson,
      String producer) {
    public OpenLineageEventWrite {
      require(specEventType, "specEventType");
      require(eventTime, "eventTime");
      requireText(eventJson, "eventJson");
      requireText(producer, "producer");

      switch (specEventType) {
        case RUN_EVENT -> {
          eventType = eventType == null ? "" : eventType;
          require(runUuid, "runUuid");
          require(jobName, "jobName");
          require(jobNamespace, "jobNamespace");
        }
        case JOB_EVENT -> {
          require(jobName, "jobName");
          require(jobNamespace, "jobNamespace");
          requireNull(eventType, "eventType", specEventType);
          requireNull(runUuid, "runUuid", specEventType);
        }
        case DATASET_EVENT -> {
          requireNull(eventType, "eventType", specEventType);
          requireNull(runUuid, "runUuid", specEventType);
          requireNull(jobName, "jobName", specEventType);
          requireNull(jobNamespace, "jobNamespace", specEventType);
        }
      }
    }

    public static OpenLineageEventWrite run(
        @Nullable String eventType,
        Instant eventTime,
        UUID runUuid,
        String jobName,
        String jobNamespace,
        String eventJson,
        String producer) {
      return new OpenLineageEventWrite(
          SpecEventType.RUN_EVENT,
          eventType,
          eventTime,
          runUuid,
          jobName,
          jobNamespace,
          eventJson,
          producer);
    }

    public static OpenLineageEventWrite job(
        Instant eventTime, String jobName, String jobNamespace, String eventJson, String producer) {
      return new OpenLineageEventWrite(
          SpecEventType.JOB_EVENT,
          null,
          eventTime,
          null,
          jobName,
          jobNamespace,
          eventJson,
          producer);
    }

    public static OpenLineageEventWrite dataset(
        Instant eventTime, String eventJson, String producer) {
      return new OpenLineageEventWrite(
          SpecEventType.DATASET_EVENT, null, eventTime, null, null, null, eventJson, producer);
    }

    private static void require(@Nullable Object value, String name) {
      if (value == null) {
        throw new IllegalArgumentException(name + " is required");
      }
    }

    private static void requireText(@Nullable String value, String name) {
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException(name + " is required");
      }
    }

    private static void requireNull(
        @Nullable Object value, String name, SpecEventType specEventType) {
      if (value != null) {
        throw new IllegalArgumentException(name + " is not valid for " + specEventType);
      }
    }
  }

  @SqlUpdate(
      """
      INSERT INTO lineage_events
          (event_type, event_time, run_uuid, job_name, job_namespace, event, producer, _event_type)
      VALUES
          (:eventType, :eventTime, :runUuid, :jobName, :jobNamespace,
           CAST(:eventJson AS jsonb), :producer, 'RUN_EVENT')
      """)
  void createLineageEvent(
      @Bind("eventType") @Nullable String eventType,
      @Bind("eventTime") Instant eventTime,
      @Bind("runUuid") UUID runUuid,
      @Bind("jobName") String jobName,
      @Bind("jobNamespace") String jobNamespace,
      @Bind("eventJson") String eventJson,
      @Bind("producer") String producer);

  @SqlUpdate(
      """
      INSERT INTO lineage_events (event_time, event, producer, _event_type)
      VALUES (:eventTime, CAST(:eventJson AS jsonb), :producer, 'DATASET_EVENT')
      """)
  void createDatasetEvent(
      @Bind("eventTime") Instant eventTime,
      @Bind("eventJson") String eventJson,
      @Bind("producer") String producer);

  @SqlUpdate(
      """
      INSERT INTO lineage_events
          (event_time, job_name, job_namespace, event, producer, _event_type)
      VALUES
          (:eventTime, :jobName, :jobNamespace,
           CAST(:eventJson AS jsonb), :producer, 'JOB_EVENT')
      """)
  void createJobEvent(
      @Bind("eventTime") Instant eventTime,
      @Bind("jobName") String jobName,
      @Bind("jobNamespace") String jobNamespace,
      @Bind("eventJson") String eventJson,
      @Bind("producer") String producer);

  /** Inserts a nonempty, bounded batch of mixed OpenLineage event types in request order. */
  default int createLineageEvents(List<OpenLineageEventWrite> events) {
    OpenLineageEventWrite[] snapshot = validatedSnapshot(events);
    int eventCount = snapshot.length;
    var specEventTypes = new String[eventCount];
    var eventTypes = new String[eventCount];
    var eventTimes = new String[eventCount];
    var runUuids = new UUID[eventCount];
    var jobNames = new String[eventCount];
    var jobNamespaces = new String[eventCount];
    var eventJsons = new String[eventCount];
    var producers = new String[eventCount];

    for (int index = 0; index < eventCount; index++) {
      OpenLineageEventWrite event = snapshot[index];
      specEventTypes[index] = event.specEventType().name();
      eventTypes[index] = event.eventType();
      eventTimes[index] = event.eventTime().toString();
      runUuids[index] = event.runUuid();
      jobNames[index] = event.jobName();
      jobNamespaces[index] = event.jobNamespace();
      eventJsons[index] = event.eventJson();
      producers[index] = event.producer();
    }

    long inserted =
        insertLineageEvents(
            specEventTypes,
            eventTypes,
            eventTimes,
            runUuids,
            jobNames,
            jobNamespaces,
            eventJsons,
            producers);
    if (inserted != eventCount) {
      throw new IllegalStateException(
          "Expected to insert " + eventCount + " OpenLineage events, but inserted " + inserted);
    }
    return eventCount;
  }

  private static OpenLineageEventWrite[] validatedSnapshot(
      @Nullable List<OpenLineageEventWrite> events) {
    if (events == null) {
      throw new IllegalArgumentException("events are required");
    }
    int eventCount = events.size();
    if (eventCount == 0) {
      throw new IllegalArgumentException("events must not be empty");
    }
    if (eventCount > MAX_EVENTS_PER_INSERT) {
      throw new IllegalArgumentException("events must not exceed " + MAX_EVENTS_PER_INSERT);
    }

    OpenLineageEventWrite[] snapshot = events.toArray(OpenLineageEventWrite[]::new);
    if (snapshot.length != eventCount || events.size() != eventCount) {
      throw new IllegalArgumentException("events changed while being read");
    }
    for (OpenLineageEventWrite event : snapshot) {
      if (event == null) {
        throw new IllegalArgumentException("event is required");
      }
    }
    return snapshot;
  }

  @SqlQuery(
      """
      WITH inserted AS (
        INSERT INTO lineage_events
            (event_type, event_time, run_uuid, job_name, job_namespace, event, producer, _event_type)
        SELECT event_type, CAST(event_time AS timestamptz), run_uuid, job_name, job_namespace,
               CAST(event_json AS jsonb), producer, spec_event_type
        FROM unnest(
            CAST(:specEventTypes AS varchar[]), CAST(:eventTypes AS varchar[]),
            CAST(:eventTimes AS varchar[]), CAST(:runUuids AS uuid[]),
            CAST(:jobNames AS varchar[]), CAST(:jobNamespaces AS varchar[]),
            CAST(:eventJsons AS varchar[]), CAST(:producers AS varchar[])
        ) WITH ORDINALITY AS ordered(
            spec_event_type, event_type, event_time, run_uuid,
            job_name, job_namespace, event_json, producer, ordinality)
        ORDER BY ordinality
        RETURNING 1
      )
      SELECT count(*)
      FROM inserted
      """)
  long insertLineageEvents(
      @Bind("specEventTypes") String[] specEventTypes,
      @Bind("eventTypes") String[] eventTypes,
      @Bind("eventTimes") String[] eventTimes,
      @Bind("runUuids") UUID[] runUuids,
      @Bind("jobNames") String[] jobNames,
      @Bind("jobNamespaces") String[] jobNamespaces,
      @Bind("eventJsons") String[] eventJsons,
      @Bind("producers") String[] producers);

  @SqlQuery(
      "SELECT event FROM lineage_events WHERE run_uuid = :runUuid AND _event_type = 'RUN_EVENT'")
  List<LineageEvent> findLineageEventsByRunUuid(@Bind("runUuid") UUID runUuid);

  @SqlQuery(
      """
      SELECT event
      FROM lineage_events AS event
      WHERE event.event_time < :before
        AND event.event_time >= :after
        AND event._event_type = 'RUN_EVENT'
      ORDER BY event.event_time DESC
      LIMIT :limit OFFSET :offset
      """)
  List<LineageEvent> getAllLineageEventsDesc(
      @Bind("before") ZonedDateTime before,
      @Bind("after") ZonedDateTime after,
      @Bind("limit") int limit,
      @Bind("offset") int offset);

  @SqlQuery(
      """
      SELECT event
      FROM lineage_events AS event
      WHERE event.event_time < :before
        AND event.event_time >= :after
        AND event._event_type = 'RUN_EVENT'
      ORDER BY event.event_time ASC
      LIMIT :limit OFFSET :offset
      """)
  List<LineageEvent> getAllLineageEventsAsc(
      @Bind("before") ZonedDateTime before,
      @Bind("after") ZonedDateTime after,
      @Bind("limit") int limit,
      @Bind("offset") int offset);

  @SqlQuery(
      """
      SELECT count(*)
      FROM lineage_events AS event
      WHERE event.event_time < :before
        AND event.event_time >= :after
      """)
  int getAllLineageTotalCount(
      @Bind("before") ZonedDateTime before, @Bind("after") ZonedDateTime after);
}
