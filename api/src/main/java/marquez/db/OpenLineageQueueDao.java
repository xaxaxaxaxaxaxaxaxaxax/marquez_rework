/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import marquez.common.Utils;
import marquez.db.models.OpenLineageQueueRow;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transactional;

/** PostgreSQL-backed durable intake queue for OpenLineage events. */
@RegisterConstructorMapper(OpenLineageQueueRow.class)
public interface OpenLineageQueueDao extends Transactional<OpenLineageQueueDao> {
  String LOCK_NEXT_DUE_SQL =
      """
      WITH candidate AS MATERIALIZED (
        SELECT head.ordering_key,
               head.event_id,
               head.attempt_count,
               head.last_error
        FROM open_lineage_queue_heads AS head
        WHERE head.available_at <= statement_timestamp()
        ORDER BY head.available_at
        FOR UPDATE OF head SKIP LOCKED
        LIMIT 1
      )
      SELECT queued.id,
             queued.ordering_key,
             queued.event AS event_json,
             queued.enqueued_at,
             LEAST(candidate.attempt_count::BIGINT + 1, 2147483647)::INTEGER AS attempt_count,
             candidate.last_error
      FROM candidate
      JOIN open_lineage_queue AS queued
        ON queued.ordering_key = candidate.ordering_key
       AND queued.id = candidate.event_id
      """;

  /** A fully validated queue admission serialized exactly once. */
  public record PreparedEvent(UUID orderingKey, String eventJson) {
    public PreparedEvent {
      requireOrderingKey(orderingKey);
      if (eventJson == null) {
        throw new IllegalArgumentException("eventJson is required");
      }
      requireNoNulInSerializedEvent(eventJson);
    }
  }

  public static PreparedEvent prepare(BaseEvent event) {
    UUID orderingKey = orderingKeyFor(event);
    return new PreparedEvent(orderingKey, Utils.toJson(event));
  }

  default long enqueue(BaseEvent event) {
    return enqueue(prepare(event));
  }

  default long enqueue(PreparedEvent event) {
    if (event == null) {
      throw new IllegalArgumentException("prepared event is required");
    }
    return enqueue(event.orderingKey(), event.eventJson());
  }

  /** Trusted low-level enqueue; callers must supply application-serialized JSON. */
  default long enqueue(UUID orderingKey, String eventJson) {
    requireOrderingKey(orderingKey);
    if (eventJson == null) {
      throw new IllegalArgumentException("eventJson is required");
    }
    if (isInTransaction()) {
      requireReadCommittedIsolation("OpenLineage enqueue");
    }
    return inTransaction(
        transactional -> {
          transactional.requireReadCommittedIsolation("OpenLineage enqueue");
          transactional.acquireOrderingKeyLock(orderingKey);
          return transactional.insertEventAndMaybeHeadAfterLock(orderingKey, eventJson);
        });
  }

  /** Internal transaction step; the protected mutation must follow on the same handle. */
  @SqlQuery(
      """
      SELECT 1
      FROM (
        SELECT pg_advisory_xact_lock(
            hashtextextended(
                'open_lineage_queue:' || CAST(:orderingKey AS TEXT),
                0))
      ) AS ordering_key_lock
      """)
  int acquireOrderingKeyLock(@Bind("orderingKey") UUID orderingKey);

  /**
   * Acquires the lane lock and reads the scheduling state of the exact head locked by the caller.
   * The scalar result is empty unless both the lock CTE was evaluated and that head is present.
   */
  @SqlQuery(
      """
      WITH ordering_lane_lock AS MATERIALIZED (
        SELECT pg_advisory_xact_lock(
            hashtextextended(
                'open_lineage_queue:' || CAST(:orderingKey AS TEXT),
                0))
      ), locked_head AS MATERIALIZED (
        SELECT head.refresh_due_on_advance
        FROM ordering_lane_lock
        CROSS JOIN open_lineage_queue_heads AS head
        WHERE head.ordering_key = :orderingKey
          AND head.event_id = :eventId
      )
      SELECT CASE
               WHEN (SELECT count(*) FROM ordering_lane_lock) = 1
                AND (SELECT count(*) FROM locked_head) = 1
               THEN (SELECT refresh_due_on_advance FROM locked_head)
               ELSE NULL
             END AS refresh_due_on_advance
      """)
  Optional<Boolean> acquireOrderingKeyLockAndReadRefreshDue(
      @Bind("orderingKey") UUID orderingKey, @Bind("eventId") long eventId);

  /**
   * Inserts one immutable payload and creates the lane head only when the lane was empty. The
   * caller must have acquired {@link #acquireOrderingKeyLock(UUID)} on the same handle.
   */
  @SqlQuery(
      """
      WITH inserted AS (
        INSERT INTO open_lineage_queue (ordering_key, event)
        VALUES (:orderingKey, :eventJson)
        RETURNING ordering_key, id
      ), created_head AS (
        INSERT INTO open_lineage_queue_heads (ordering_key, event_id)
        SELECT inserted.ordering_key, inserted.id
        FROM inserted
        WHERE NOT EXISTS (
          SELECT 1
          FROM open_lineage_queue_heads AS head
          WHERE head.ordering_key = :orderingKey
        )
        RETURNING event_id
      )
      SELECT id
      FROM inserted
      """)
  long insertEventAndMaybeHeadAfterLock(
      @Bind("orderingKey") UUID orderingKey, @Bind("eventJson") String eventJson);

  /** Locks one due lane head and returns its proposed attempt in the caller's transaction. */
  default Optional<OpenLineageQueueRow> lockNextDue() {
    requireAttachedReadCommittedTransaction("OpenLineage dequeue");
    return lockNextDueHead();
  }

  @SqlQuery(LOCK_NEXT_DUE_SQL)
  Optional<OpenLineageQueueRow> lockNextDueHead();

  /** Acknowledges the lane head already locked by the caller's projection transaction. */
  default void ackLocked(UUID orderingKey, long eventId) {
    requireLockedTransition(orderingKey, eventId);
    requireAttachedReadCommittedTransaction("OpenLineage acknowledgement");

    boolean refreshDueOnAdvance = lockOrderingLaneAndReadRefreshDue(orderingKey, eventId);
    int advanced = advanceLockedHeadUsingHint(orderingKey, eventId, refreshDueOnAdvance);
    if (advanced == 0) {
      requireOne(
          deleteLockedHead(orderingKey, eventId),
          "delete acknowledged OpenLineage queue head " + eventId);
    }
    requireOne(
        deleteEvent(orderingKey, eventId),
        "delete acknowledged OpenLineage queue payload " + eventId);
  }

  private boolean lockOrderingLaneAndReadRefreshDue(UUID orderingKey, long eventId) {
    return acquireOrderingKeyLockAndReadRefreshDue(orderingKey, eventId)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Expected exact locked OpenLineage queue head "
                        + eventId
                        + " after acquiring the ordering-lane lock"));
  }

  private int advanceLockedHeadUsingHint(
      UUID orderingKey, long eventId, boolean refreshDueOnAdvance) {
    int expected =
        refreshDueOnAdvance
            ? advanceLockedHeadRefreshingDue(orderingKey, eventId)
            : advanceLockedHeadPreservingDue(orderingKey, eventId);
    requireZeroOrOne(
        expected,
        (refreshDueOnAdvance ? "refresh" : "preserve")
            + " the OpenLineage scheduling quantum for "
            + eventId);
    if (expected == 1) {
      return 1;
    }

    int fallback =
        refreshDueOnAdvance
            ? advanceLockedHeadPreservingDue(orderingKey, eventId)
            : advanceLockedHeadRefreshingDue(orderingKey, eventId);
    requireZeroOrOne(
        fallback,
        (refreshDueOnAdvance ? "preserve" : "refresh")
            + " the OpenLineage scheduling quantum for "
            + eventId);
    return fallback;
  }

  /** First scheduling quantum: preserve the indexed due time and permit a HOT update. */
  @SqlUpdate(
      """
      WITH follower AS MATERIALIZED (
        SELECT queued.id
        FROM open_lineage_queue AS queued
        WHERE queued.ordering_key = :orderingKey
          AND queued.id > :eventId
        ORDER BY queued.id
        LIMIT 1
      )
      UPDATE open_lineage_queue_heads AS head
      SET event_id = follower.id,
          attempt_count = 0,
          refresh_due_on_advance = TRUE,
          last_error = NULL
      FROM follower
      WHERE head.ordering_key = :orderingKey
        AND head.event_id = :eventId
        AND head.refresh_due_on_advance = FALSE
      """)
  int advanceLockedHeadPreservingDue(
      @Bind("orderingKey") UUID orderingKey, @Bind("eventId") long eventId);

  /** Second scheduling quantum: refresh the due time and begin a new quantum. */
  @SqlUpdate(
      """
      WITH follower AS MATERIALIZED (
        SELECT queued.id
        FROM open_lineage_queue AS queued
        WHERE queued.ordering_key = :orderingKey
          AND queued.id > :eventId
        ORDER BY queued.id
        LIMIT 1
      )
      UPDATE open_lineage_queue_heads AS head
      SET event_id = follower.id,
          available_at = date_trunc('milliseconds', statement_timestamp()),
          attempt_count = 0,
          refresh_due_on_advance = FALSE,
          last_error = NULL
      FROM follower
      WHERE head.ordering_key = :orderingKey
        AND head.event_id = :eventId
        AND head.refresh_due_on_advance = TRUE
      """)
  int advanceLockedHeadRefreshingDue(
      @Bind("orderingKey") UUID orderingKey, @Bind("eventId") long eventId);

  @SqlUpdate(
      """
      DELETE FROM open_lineage_queue_heads
      WHERE ordering_key = :orderingKey
        AND event_id = :eventId
      """)
  int deleteLockedHead(@Bind("orderingKey") UUID orderingKey, @Bind("eventId") long eventId);

  @SqlUpdate(
      """
      DELETE FROM open_lineage_queue
      WHERE ordering_key = :orderingKey
        AND id = :eventId
      """)
  int deleteEvent(@Bind("orderingKey") UUID orderingKey, @Bind("eventId") long eventId);

  /** Schedules a database-time retry without advancing the locked lane. */
  default void retryLocked(
      UUID orderingKey, long eventId, int attemptCount, String error, long delayMillis) {
    requireLockedTransition(orderingKey, eventId);
    requirePositive(attemptCount, "attemptCount");
    requireNonNegative(delayMillis, "delayMillis");
    requireAttachedReadCommittedTransaction("OpenLineage retry");

    requireOne(
        retryLockedState(orderingKey, eventId, attemptCount, normalizeError(error), delayMillis),
        "schedule retry for OpenLineage queue payload " + eventId);
  }

  @SqlUpdate(
      """
      UPDATE open_lineage_queue_heads
      SET available_at =
              date_trunc(
                  'milliseconds',
                  statement_timestamp()
                      + (:delayMillis * INTERVAL '1 millisecond')
                      + INTERVAL '999 microseconds'),
          attempt_count = :attemptCount,
          refresh_due_on_advance = FALSE,
          last_error = left(CAST(:error AS TEXT), 4096)
      WHERE ordering_key = :orderingKey
        AND event_id = :eventId
        AND :attemptCount =
            LEAST(attempt_count::BIGINT + 1, 2147483647)::INTEGER
      """)
  int retryLockedState(
      @Bind("orderingKey") UUID orderingKey,
      @Bind("eventId") long eventId,
      @Bind("attemptCount") int attemptCount,
      @Bind("error") String error,
      @Bind("delayMillis") long delayMillis);

  /** Moves the locked head to dead-letter storage and advances its lane atomically. */
  default void deadLetterLocked(UUID orderingKey, long eventId, int attemptCount, String error) {
    requireLockedTransition(orderingKey, eventId);
    requirePositive(attemptCount, "attemptCount");
    requireAttachedReadCommittedTransaction("OpenLineage dead-letter transition");

    boolean refreshDueOnAdvance = lockOrderingLaneAndReadRefreshDue(orderingKey, eventId);
    requireOne(
        insertDeadLetterLocked(orderingKey, eventId, attemptCount, normalizeError(error)),
        "insert OpenLineage dead letter " + eventId);

    int advanced = advanceLockedHeadUsingHint(orderingKey, eventId, refreshDueOnAdvance);
    if (advanced == 0) {
      requireOne(
          deleteLockedHead(orderingKey, eventId),
          "delete dead-lettered OpenLineage queue head " + eventId);
    }
    requireOne(
        deleteEvent(orderingKey, eventId),
        "delete dead-lettered OpenLineage queue payload " + eventId);
  }

  @SqlUpdate(
      """
      INSERT INTO open_lineage_dead_letters (
          ordering_key,
          id,
          event,
          enqueued_at,
          attempt_count,
          last_error)
      SELECT queued.ordering_key,
             queued.id,
             queued.event,
             queued.enqueued_at,
             :attemptCount,
             left(CAST(:error AS TEXT), 4096)
      FROM open_lineage_queue_heads AS head
      JOIN open_lineage_queue AS queued
        ON queued.ordering_key = head.ordering_key
       AND queued.id = head.event_id
      WHERE head.ordering_key = :orderingKey
        AND head.event_id = :eventId
        AND :attemptCount =
            LEAST(head.attempt_count::BIGINT + 1, 2147483647)::INTEGER
      """)
  int insertDeadLetterLocked(
      @Bind("orderingKey") UUID orderingKey,
      @Bind("eventId") long eventId,
      @Bind("attemptCount") int attemptCount,
      @Bind("error") String error);

  default int purgeDeadBefore(Instant cutoff, int batchSize) {
    requireCutoffAndBatch(cutoff, batchSize);
    return purgeDeadBeforeBounded(cutoff, batchSize);
  }

  @SqlUpdate(
      """
      WITH doomed AS (
        SELECT dead_at, id
        FROM open_lineage_dead_letters
        WHERE dead_at < :cutoff
        ORDER BY dead_at, id
        FOR UPDATE SKIP LOCKED
        LIMIT :batchSize
      )
      DELETE FROM open_lineage_dead_letters AS dead
      USING doomed
      WHERE dead.dead_at = doomed.dead_at
        AND dead.id = doomed.id
      """)
  int purgeDeadBeforeBounded(@Bind("cutoff") Instant cutoff, @Bind("batchSize") int batchSize);

  default int countDeadBefore(Instant cutoff, int batchSize) {
    requireCutoffAndBatch(cutoff, batchSize);
    return countDeadBeforeBounded(cutoff, batchSize);
  }

  @SqlQuery(
      """
      SELECT count(*)
      FROM (
        SELECT 1
        FROM open_lineage_dead_letters
        WHERE dead_at < :cutoff
        ORDER BY dead_at, id
        LIMIT :batchSize
      ) AS bounded
      """)
  int countDeadBeforeBounded(@Bind("cutoff") Instant cutoff, @Bind("batchSize") int batchSize);

  static UUID orderingKeyFor(BaseEvent event) {
    validateEventIdentity(event);
    if (event instanceof LineageEvent lineageEvent) {
      return nameBasedOrderingKey(
          "run", Utils.openLineageRunUuid(lineageEvent.getRun().getRunId()).toString());
    }
    if (event instanceof JobEvent jobEvent) {
      return jobOrderingKey(jobEvent.getJob(), "job event");
    }
    if (event instanceof DatasetEvent datasetEvent) {
      LineageEvent.Dataset dataset = required(datasetEvent.getDataset(), "dataset event dataset");
      return nameBasedOrderingKey(
          "dataset", Utils.sanitizeOpenLineageNamespace(dataset.getNamespace()), dataset.getName());
    }
    throw new IllegalArgumentException("unsupported OpenLineage event type: " + event.getClass());
  }

  /** Validates every identity used by durable intake without changing its spelling. */
  static void validateEventIdentity(BaseEvent event) {
    if (event == null) {
      throw new IllegalArgumentException("event is required");
    }
    if (event instanceof LineageEvent lineageEvent) {
      LineageEvent.Run run = required(lineageEvent.getRun(), "lineage event run");
      requiredIdentity(run.getRunId(), "lineage event runId");
      validateJob(lineageEvent.getJob(), "lineage event");
      validateDatasets(lineageEvent.getInputs(), "lineage event input");
      validateDatasets(lineageEvent.getOutputs(), "lineage event output");
      validateParent(run.getFacets());
      return;
    }
    if (event instanceof JobEvent jobEvent) {
      validateJob(jobEvent.getJob(), "job event");
      validateDatasets(jobEvent.getInputs(), "job event input");
      validateDatasets(jobEvent.getOutputs(), "job event output");
      return;
    }
    if (event instanceof DatasetEvent datasetEvent) {
      validateDataset(
          required(datasetEvent.getDataset(), "dataset event dataset"), "dataset event");
      return;
    }
    throw new IllegalArgumentException("unsupported OpenLineage event type: " + event.getClass());
  }

  private static UUID jobOrderingKey(LineageEvent.Job job, String eventDescription) {
    job = validateJob(job, eventDescription);
    return nameBasedOrderingKey(
        "job", Utils.sanitizeOpenLineageNamespace(job.getNamespace()), job.getName());
  }

  private static LineageEvent.Job validateJob(LineageEvent.Job job, String eventDescription) {
    job = required(job, eventDescription + " job");
    requiredIdentity(job.getNamespace(), eventDescription + " namespace");
    requiredIdentity(job.getName(), eventDescription + " name");
    return job;
  }

  private static void validateParent(LineageEvent.RunFacet facets) {
    if (facets == null || facets.getParent() == null) {
      return;
    }
    LineageEvent.ParentRunFacet parent = facets.getParent();
    LineageEvent.RunLink run = required(parent.getRun(), "parent run facet run");
    LineageEvent.JobLink job = required(parent.getJob(), "parent run facet job");
    requiredIdentity(run.getRunId(), "parent run facet runId");
    requiredIdentity(job.getNamespace(), "parent run facet namespace");
    requiredIdentity(job.getName(), "parent run facet name");
  }

  private static void validateDatasets(
      List<LineageEvent.Dataset> datasets, String eventDescription) {
    if (datasets == null) {
      return;
    }
    for (LineageEvent.Dataset dataset : datasets) {
      validateDataset(required(dataset, eventDescription + " dataset"), eventDescription);
    }
  }

  private static void validateDataset(LineageEvent.Dataset dataset, String eventDescription) {
    requiredIdentity(dataset.getNamespace(), eventDescription + " namespace");
    requiredIdentity(dataset.getName(), eventDescription + " name");
  }

  private static UUID nameBasedOrderingKey(String domain, String... identityParts) {
    StringBuilder identity = new StringBuilder(domain);
    for (String identityPart : identityParts) {
      identity.append('\0').append(identityPart);
    }
    return UUID.nameUUIDFromBytes(identity.toString().getBytes(StandardCharsets.UTF_8));
  }

  /** Rejects active JSON NUL escapes while allowing a literal six-character backslash-u0000. */
  private static void requireNoNulInSerializedEvent(String eventJson) {
    int consecutiveBackslashes = 0;
    for (int index = 0; index < eventJson.length(); index++) {
      char current = eventJson.charAt(index);
      if (current == '\0') {
        throw nulInEvent();
      }
      if (current == '\\') {
        consecutiveBackslashes++;
        continue;
      }
      if (current == 'u'
          && (consecutiveBackslashes & 1) == 1
          && index + 4 < eventJson.length()
          && eventJson.charAt(index + 1) == '0'
          && eventJson.charAt(index + 2) == '0'
          && eventJson.charAt(index + 3) == '0'
          && eventJson.charAt(index + 4) == '0') {
        throw nulInEvent();
      }
      consecutiveBackslashes = 0;
    }
  }

  private static IllegalArgumentException nulInEvent() {
    return new IllegalArgumentException("OpenLineage event text values must not contain U+0000");
  }

  private void requireReadCommittedIsolation(String operation) {
    requireReadCommittedIsolation(getHandle(), operation);
  }

  private static void requireReadCommittedIsolation(Handle handle, String operation) {
    if (handle.getTransactionIsolationLevel() != TransactionIsolationLevel.READ_COMMITTED) {
      throw new IllegalStateException(operation + " requires READ COMMITTED isolation");
    }
  }

  private void requireAttachedReadCommittedTransaction(String operation) {
    if (!isInTransaction()) {
      throw new IllegalStateException(operation + " requires an attached transaction");
    }
    requireReadCommittedIsolation(operation);
  }

  private static void requireLockedTransition(UUID orderingKey, long eventId) {
    requireOrderingKey(orderingKey);
    if (eventId <= 0) {
      throw new IllegalArgumentException("eventId must be positive");
    }
  }

  private static void requireOrderingKey(UUID orderingKey) {
    if (orderingKey == null) {
      throw new IllegalArgumentException("orderingKey is required");
    }
  }

  private static void requireCutoffAndBatch(Instant cutoff, int batchSize) {
    if (cutoff == null) {
      throw new IllegalArgumentException("cutoff is required");
    }
    requirePositive(batchSize, "batchSize");
  }

  private static void requirePositive(long value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
  }

  private static void requireNonNegative(long value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must not be negative");
    }
  }

  private static void requireOne(int affectedRows, String action) {
    if (affectedRows != 1) {
      throw new IllegalStateException(
          "Expected to " + action + ", but affected " + affectedRows + " rows");
    }
  }

  private static void requireZeroOrOne(int affectedRows, String action) {
    if (affectedRows < 0 || affectedRows > 1) {
      throw new IllegalStateException(
          "Expected to " + action + " at most once, but affected " + affectedRows + " rows");
    }
  }

  private static String normalizeError(String error) {
    return error == null ? null : error.replace('\0', '\uFFFD');
  }

  private static <T> T required(T value, String description) {
    if (value == null) {
      throw new IllegalArgumentException(description + " is required");
    }
    return value;
  }

  private static String requiredIdentity(String value, String description) {
    return Utils.requireOpenLineageIdentity(value, description);
  }
}
