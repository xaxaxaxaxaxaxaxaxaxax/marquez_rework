/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
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
  int MAX_ADMISSION_EVENTS = 1000;

  String LOCK_NEXT_DUE_SQL =
      """
      WITH candidate AS MATERIALIZED (
        SELECT head.ordering_key,
               head.event_id,
               head.attempt_count
        FROM open_lineage_queue_heads AS head
        WHERE head.available_at <= statement_timestamp()
        ORDER BY head.available_at
        FOR UPDATE OF head SKIP LOCKED
        LIMIT 1
      )
      SELECT queued.id,
             queued.ordering_key,
             queued.event AS event_json,
             LEAST(candidate.attempt_count::BIGINT + 1, 2147483647)::INTEGER AS attempt_count,
             queued.admission_id
      FROM candidate
      JOIN open_lineage_queue AS queued
        ON queued.ordering_key = candidate.ordering_key
       AND queued.id = candidate.event_id
      """;

  String LOCK_NEXT_DUE_BATCH_SQL =
      """
      WITH seed AS MATERIALIZED (
        SELECT head.ordering_key,
               head.event_id,
               head.attempt_count,
               head.refresh_due_on_advance,
               queued.admission_id
        FROM open_lineage_queue_heads AS head
        JOIN open_lineage_queue AS queued
          ON queued.ordering_key = head.ordering_key
         AND queued.id = head.event_id
        WHERE head.available_at <= statement_timestamp()
        ORDER BY head.available_at
        FOR UPDATE OF head SKIP LOCKED
        LIMIT 1
      ), peer_heads AS MATERIALIZED (
        SELECT head.ordering_key,
               head.event_id,
               head.attempt_count,
               head.refresh_due_on_advance,
               queued.admission_id
        FROM seed
        JOIN open_lineage_queue AS queued
          ON queued.admission_id = seed.admission_id
        JOIN open_lineage_queue_heads AS head
          ON head.ordering_key = queued.ordering_key
         AND head.event_id = queued.id
        WHERE seed.admission_id IS NOT NULL
          AND (head.ordering_key <> seed.ordering_key OR head.event_id <> seed.event_id)
          AND head.available_at <= statement_timestamp()
        ORDER BY
          hashtextextended(
              'open_lineage_queue:' || CAST(head.ordering_key AS TEXT),
              0),
          head.ordering_key
        FOR UPDATE OF head SKIP LOCKED
        LIMIT GREATEST(CAST(:maxEvents AS INTEGER) - 1, 0)
      ), locked_heads AS MATERIALIZED (
        SELECT * FROM seed
        UNION ALL
        SELECT * FROM peer_heads
      ), claim_positions AS MATERIALIZED (
        SELECT candidate.id,
               locked.ordering_key,
               CASE
                 WHEN candidate.id = locked.event_id
                 THEN LEAST(locked.attempt_count::BIGINT + 1, 2147483647)::INTEGER
                 ELSE 1
               END AS attempt_count,
               CASE WHEN candidate.id = locked.event_id THEN 0 ELSE 1 END AS claim_priority
        FROM locked_heads AS locked
        CROSS JOIN LATERAL (
          SELECT queued.id,
                 queued.admission_id
          FROM open_lineage_queue AS queued
          WHERE queued.ordering_key = locked.ordering_key
            AND queued.id >= locked.event_id
          ORDER BY queued.id
          LIMIT CASE
            WHEN locked.admission_id IS NOT NULL
             AND locked.refresh_due_on_advance = FALSE
            THEN 2
            ELSE 1
          END
        ) AS candidate
        WHERE candidate.id = locked.event_id
           OR (locked.admission_id IS NOT NULL
               AND candidate.admission_id = locked.admission_id)
      ), selected AS MATERIALIZED (
        SELECT id,
               ordering_key,
               attempt_count,
               claim_priority
        FROM claim_positions
        ORDER BY claim_priority, id
        LIMIT CAST(:maxEvents AS INTEGER)
      )
      SELECT queued.id,
             queued.ordering_key,
             queued.event AS event_json,
             selected.attempt_count,
             queued.admission_id
      FROM selected
      JOIN open_lineage_queue AS queued
        ON queued.ordering_key = selected.ordering_key
       AND queued.id = selected.id
      ORDER BY queued.id
      """;

  /** A fully validated queue admission serialized exactly once. */
  public record PreparedEvent(UUID orderingKey, String eventJson) {
    public PreparedEvent {
      requireOrderingKey(orderingKey);
      eventJson = requireEventJson(eventJson);
    }
  }

  /** An immutable, columnar admission whose backing arrays remain private to this DAO. */
  public static final class PreparedAdmission {
    private final UUID[] orderingKeys;
    private final String[] eventJsons;

    private PreparedAdmission(UUID[] orderingKeys, String[] eventJsons) {
      this.orderingKeys = orderingKeys;
      this.eventJsons = eventJsons;
    }

    public int size() {
      return orderingKeys.length;
    }
  }

  public static PreparedEvent prepare(BaseEvent event) {
    UUID orderingKey = orderingKeyFor(event);
    return new PreparedEvent(orderingKey, Utils.toJson(event));
  }

  /** Validates and serializes an ordered admission without per-event wrapper allocation. */
  public static PreparedAdmission prepareAll(List<? extends BaseEvent> events) {
    if (events == null) {
      throw new IllegalArgumentException("events are required");
    }
    int eventCount = events.size();
    requireAdmissionLimit(eventCount, "events");

    UUID[] orderingKeys = new UUID[eventCount];
    String[] eventJsons = new String[eventCount];
    int index = 0;
    for (BaseEvent event : events) {
      if (index >= eventCount) {
        throw new IllegalArgumentException("events changed during preparation");
      }
      orderingKeys[index] = orderingKeyFor(event);
      eventJsons[index] = requireEventJson(Utils.toJson(event));
      index++;
    }
    requireStableAdmissionSize(index, eventCount, "events");
    return new PreparedAdmission(orderingKeys, eventJsons);
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

  /** Atomically enqueues an ordered batch of events already validated and serialized. */
  default int enqueueAll(List<PreparedEvent> events) {
    return enqueueAll(toPreparedAdmission(events));
  }

  private static PreparedAdmission toPreparedAdmission(List<PreparedEvent> events) {
    if (events == null) {
      throw new IllegalArgumentException("prepared events are required");
    }
    int eventCount = events.size();
    requireAdmissionLimit(eventCount, "prepared events");

    UUID[] orderingKeys = new UUID[eventCount];
    String[] eventJsons = new String[eventCount];
    int index = 0;
    for (PreparedEvent event : events) {
      if (index >= eventCount) {
        throw new IllegalArgumentException("prepared events changed during preparation");
      }
      if (event == null) {
        throw new IllegalArgumentException("prepared event is required");
      }
      orderingKeys[index] = event.orderingKey();
      eventJsons[index] = event.eventJson();
      index++;
    }
    requireStableAdmissionSize(index, eventCount, "prepared events");
    return new PreparedAdmission(orderingKeys, eventJsons);
  }

  /** Atomically enqueues a privately owned columnar admission without copying its members. */
  default int enqueueAll(PreparedAdmission admission) {
    if (admission == null) {
      throw new IllegalArgumentException("prepared admission is required");
    }
    int eventCount = admission.size();
    requireAdmissionLimit(eventCount, "prepared admission events");
    if (eventCount == 0) {
      return 0;
    }

    if (isInTransaction()) {
      requireReadCommittedIsolation("OpenLineage bulk enqueue");
    }
    return inTransaction(
        transactional -> {
          transactional.requireReadCommittedIsolation("OpenLineage bulk enqueue");
          transactional.acquireOrderingKeyLocks(admission.orderingKeys);
          long inserted =
              transactional.insertEventsAndMaybeHeadsAfterLocks(
                  admission.orderingKeys, admission.eventJsons);
          if (inserted != eventCount) {
            throw new IllegalStateException(
                "Expected to enqueue "
                    + eventCount
                    + " OpenLineage events, but inserted "
                    + inserted);
          }
          return eventCount;
        });
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

  /** Acquires every effective ordering-lane lock in one deterministic global order. */
  @SqlQuery(
      """
      WITH ordered_lock_keys AS MATERIALIZED (
        SELECT DISTINCT
               hashtextextended(
                   'open_lineage_queue:' || CAST(requested.ordering_key AS TEXT),
                   0) AS lock_key
        FROM unnest(CAST(:orderingKeys AS uuid[])) AS requested(ordering_key)
        ORDER BY lock_key
      ), acquired AS MATERIALIZED (
        SELECT pg_advisory_xact_lock(ordered.lock_key) AS ordering_key_lock
        FROM (
          SELECT lock_key
          FROM ordered_lock_keys
          ORDER BY lock_key
          OFFSET 0
        ) AS ordered
      )
      SELECT count(*)
      FROM acquired
      """)
  int acquireOrderingKeyLocks(@Bind("orderingKeys") UUID[] orderingKeys);

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

  /**
   * Inserts immutable payloads in request order and creates one head for each newly nonempty lane.
   * The caller must already hold every corresponding ordering-lane lock on the same handle.
   */
  @SqlQuery(
      """
      WITH admission AS MATERIALIZED (
        SELECT nextval('open_lineage_queue_admission_id_seq') AS admission_id
      ), inserted AS (
        INSERT INTO open_lineage_queue (ordering_key, event, admission_id)
        SELECT ordered.ordering_key,
               ordered.event_json,
               admission.admission_id
        FROM (
          SELECT requested.ordering_key,
                 requested.event_json,
                 requested.ordinality
          FROM unnest(
              CAST(:orderingKeys AS uuid[]),
              CAST(:eventJsons AS varchar[])
          ) WITH ORDINALITY AS requested(ordering_key, event_json, ordinality)
          ORDER BY requested.ordinality
          OFFSET 0
        ) AS ordered
        CROSS JOIN admission
        ORDER BY ordered.ordinality
        RETURNING ordering_key, id
      ), created_heads AS (
        INSERT INTO open_lineage_queue_heads (ordering_key, event_id)
        SELECT inserted.ordering_key, min(inserted.id)
        FROM inserted
        WHERE NOT EXISTS (
          SELECT 1
          FROM open_lineage_queue_heads AS head
          WHERE head.ordering_key = inserted.ordering_key
        )
        GROUP BY inserted.ordering_key
        ORDER BY inserted.ordering_key
        RETURNING event_id
      )
      SELECT count(*)
      FROM inserted
      """)
  long insertEventsAndMaybeHeadsAfterLocks(
      @Bind("orderingKeys") UUID[] orderingKeys, @Bind("eventJsons") String[] eventJsons);

  /** Locks one due lane head and returns its proposed attempt in the caller's transaction. */
  default Optional<OpenLineageQueueRow> lockNextDue() {
    requireAttachedReadCommittedTransaction("OpenLineage dequeue");
    return lockNextDueHead();
  }

  @SqlQuery(LOCK_NEXT_DUE_SQL)
  Optional<OpenLineageQueueRow> lockNextDueHead();

  /**
   * Locks a bounded, request-aware due slice. A limit of one deliberately uses the legacy claim
   * query unchanged. Larger claims contain one admission only, except that a null admission remains
   * an implicit singleton.
   */
  default List<OpenLineageQueueRow> lockNextDueBatch(int maxEvents) {
    requireClaimLimit(maxEvents);
    if (maxEvents == 1) {
      Optional<OpenLineageQueueRow> row = lockNextDue();
      return row.isPresent() ? List.of(row.get()) : List.of();
    }
    requireAttachedReadCommittedTransaction("OpenLineage batch dequeue");
    return lockNextDueBatchRows(maxEvents);
  }

  @SqlQuery(LOCK_NEXT_DUE_BATCH_SQL)
  List<OpenLineageQueueRow> lockNextDueBatchRows(@Bind("maxEvents") int maxEvents);

  /**
   * Acknowledges an ID-ordered all-success claim with one lane-lock and one transition statement.
   * Generated queue IDs make strict monotonicity both the ordering and uniqueness proof.
   */
  default void ackLockedAll(List<OpenLineageQueueRow> rows) {
    requireAttachedReadCommittedTransaction("OpenLineage batch acknowledgement");
    if (rows == null) {
      throw new IllegalArgumentException("locked rows are required");
    }
    if (rows.isEmpty()) {
      throw new IllegalArgumentException("locked rows must not be empty");
    }

    int rowCount = rows.size();
    UUID[] orderingKeys = new UUID[rowCount];
    long[] eventIds = new long[rowCount];
    Long admissionId = null;
    long previousEventId = 0;
    int index = 0;
    for (OpenLineageQueueRow row : rows) {
      if (index >= rowCount) {
        throw new IllegalArgumentException("locked rows changed during validation");
      }
      if (row == null) {
        throw new IllegalArgumentException("locked row is required");
      }
      requireLockedTransition(row.orderingKey(), row.id());
      if (row.id() <= previousEventId) {
        throw new IllegalArgumentException("locked rows must be strictly ordered by event ID");
      }
      if (index == 0) {
        admissionId = row.admissionId();
      } else if (!Objects.equals(admissionId, row.admissionId())) {
        throw new IllegalArgumentException("locked rows must belong to one admission");
      }
      orderingKeys[index] = row.orderingKey();
      eventIds[index] = row.id();
      previousEventId = row.id();
      index++;
    }
    requireStableAdmissionSize(index, rowCount, "locked rows");
    if (admissionId == null && rowCount != 1) {
      throw new IllegalArgumentException(
          "singleton queue rows cannot form a batch acknowledgement");
    }

    acquireOrderingKeyLocks(orderingKeys);
    long transitioned = acknowledgeLockedPrefixesAfterLaneLocks(orderingKeys, eventIds);
    requireExpected(transitioned, rowCount, "acknowledge OpenLineage queue payload prefixes");
  }

  @SqlQuery(
      """
      WITH acknowledged AS MATERIALIZED (
        SELECT requested.ordering_key,
               requested.event_id
        FROM unnest(
            CAST(:orderingKeys AS uuid[]),
            CAST(:eventIds AS bigint[])
        ) AS requested(ordering_key, event_id)
      ), lanes AS MATERIALIZED (
        SELECT acknowledged.ordering_key,
               array_agg(acknowledged.event_id ORDER BY acknowledged.event_id) AS event_ids,
               min(acknowledged.event_id) AS first_event_id,
               max(acknowledged.event_id) AS last_event_id,
               count(*) AS event_count
        FROM acknowledged
        GROUP BY acknowledged.ordering_key
      ), validated AS MATERIALIZED (
        SELECT lane.ordering_key,
               lane.event_count,
               head.event_id AS locked_event_id,
               head.refresh_due_on_advance,
               successor.id AS successor_id
        FROM lanes AS lane
        JOIN open_lineage_queue_heads AS head
          ON head.ordering_key = lane.ordering_key
        CROSS JOIN LATERAL (
          SELECT array_agg(prefix.id ORDER BY prefix.id) AS event_ids
          FROM (
            SELECT queued.id
            FROM open_lineage_queue AS queued
            WHERE queued.ordering_key = lane.ordering_key
              AND queued.id >= head.event_id
            ORDER BY queued.id
            LIMIT lane.event_count
          ) AS prefix
        ) AS current_prefix
        LEFT JOIN LATERAL (
          SELECT queued.id
          FROM open_lineage_queue AS queued
          WHERE queued.ordering_key = lane.ordering_key
            AND queued.id > lane.last_event_id
          ORDER BY queued.id
          LIMIT 1
        ) AS successor ON TRUE
        WHERE head.event_id = lane.first_event_id
          AND current_prefix.event_ids = lane.event_ids
          AND lane.event_count <=
              CASE WHEN head.refresh_due_on_advance THEN 1 ELSE 2 END
      ), preserved AS (
        UPDATE open_lineage_queue_heads AS head
        SET event_id = validated.successor_id,
            attempt_count = 0,
            refresh_due_on_advance = TRUE,
            last_error = NULL
        FROM validated
        WHERE head.ordering_key = validated.ordering_key
          AND head.event_id = validated.locked_event_id
          AND validated.successor_id IS NOT NULL
          AND validated.refresh_due_on_advance = FALSE
          AND validated.event_count = 1
        RETURNING head.ordering_key
      ), refreshed AS (
        UPDATE open_lineage_queue_heads AS head
        SET event_id = validated.successor_id,
            available_at = date_trunc('milliseconds', statement_timestamp()),
            attempt_count = 0,
            refresh_due_on_advance = FALSE,
            last_error = NULL
        FROM validated
        WHERE head.ordering_key = validated.ordering_key
          AND head.event_id = validated.locked_event_id
          AND validated.successor_id IS NOT NULL
          AND (validated.refresh_due_on_advance = TRUE OR validated.event_count = 2)
        RETURNING head.ordering_key
      ), emptied AS (
        DELETE FROM open_lineage_queue_heads AS head
        USING validated
        WHERE head.ordering_key = validated.ordering_key
          AND head.event_id = validated.locked_event_id
          AND validated.successor_id IS NULL
        RETURNING head.ordering_key
      ), transitioned AS MATERIALIZED (
        SELECT ordering_key FROM preserved
        UNION ALL
        SELECT ordering_key FROM refreshed
        UNION ALL
        SELECT ordering_key FROM emptied
      ), deleted AS (
        DELETE FROM open_lineage_queue AS queued
        USING acknowledged
        WHERE (SELECT count(*) FROM transitioned) = (SELECT count(*) FROM lanes)
          AND queued.ordering_key = acknowledged.ordering_key
          AND queued.id = acknowledged.event_id
        RETURNING queued.id
      )
      SELECT count(*)
      FROM deleted
      """)
  long acknowledgeLockedPrefixesAfterLaneLocks(
      @Bind("orderingKeys") UUID[] orderingKeys, @Bind("eventIds") long[] eventIds);

  /**
   * Advances or removes one exact head after its lane lock was acquired. The preserving branch
   * deliberately omits the indexed due-time column so PostgreSQL can use a HOT update.
   */
  @SqlQuery(
      """
      WITH validated AS MATERIALIZED (
        SELECT head.ordering_key,
               head.event_id AS locked_event_id,
               head.refresh_due_on_advance,
               successor.id AS successor_id
        FROM open_lineage_queue_heads AS head
        LEFT JOIN LATERAL (
          SELECT queued.id
          FROM open_lineage_queue AS queued
          WHERE queued.ordering_key = head.ordering_key
            AND queued.id > head.event_id
          ORDER BY queued.id
          LIMIT 1
        ) AS successor ON TRUE
        WHERE head.ordering_key = :orderingKey
          AND head.event_id = :eventId
      ), preserved AS (
        UPDATE open_lineage_queue_heads AS head
        SET event_id = validated.successor_id,
            attempt_count = 0,
            refresh_due_on_advance = TRUE,
            last_error = NULL
        FROM validated
        WHERE head.ordering_key = validated.ordering_key
          AND head.event_id = validated.locked_event_id
          AND validated.successor_id IS NOT NULL
          AND validated.refresh_due_on_advance = FALSE
        RETURNING head.ordering_key
      ), refreshed AS (
        UPDATE open_lineage_queue_heads AS head
        SET event_id = validated.successor_id,
            available_at = date_trunc('milliseconds', statement_timestamp()),
            attempt_count = 0,
            refresh_due_on_advance = FALSE,
            last_error = NULL
        FROM validated
        WHERE head.ordering_key = validated.ordering_key
          AND head.event_id = validated.locked_event_id
          AND validated.successor_id IS NOT NULL
          AND validated.refresh_due_on_advance = TRUE
        RETURNING head.ordering_key
      ), emptied AS (
        DELETE FROM open_lineage_queue_heads AS head
        USING validated
        WHERE head.ordering_key = validated.ordering_key
          AND head.event_id = validated.locked_event_id
          AND validated.successor_id IS NULL
        RETURNING head.ordering_key
      ), transitioned AS MATERIALIZED (
        SELECT ordering_key FROM preserved
        UNION ALL
        SELECT ordering_key FROM refreshed
        UNION ALL
        SELECT ordering_key FROM emptied
      ), deleted AS (
        DELETE FROM open_lineage_queue AS queued
        USING transitioned
        WHERE (SELECT count(*) FROM transitioned) = 1
          AND queued.ordering_key = :orderingKey
          AND queued.id = :eventId
        RETURNING queued.id
      )
      SELECT count(*)
      FROM deleted
      """)
  long finishLockedHeadAfterLaneLock(
      @Bind("orderingKey") UUID orderingKey, @Bind("eventId") long eventId);

  /** Acknowledges the lane head already locked by the caller's projection transaction. */
  default void ackLocked(UUID orderingKey, long eventId) {
    requireLockedTransition(orderingKey, eventId);
    requireAttachedReadCommittedTransaction("OpenLineage acknowledgement");

    acquireOrderingKeyLock(orderingKey);
    ackLockedAfterLaneLock(orderingKey, eventId);
  }

  /** Acknowledges an exact head after the caller acquired every mixed-claim lane lock in order. */
  default void ackLockedAfterLaneLock(UUID orderingKey, long eventId) {
    requireLockedTransition(orderingKey, eventId);
    requireAttachedReadCommittedTransaction("OpenLineage acknowledgement after lane lock");

    requireExpected(
        finishLockedHeadAfterLaneLock(orderingKey, eventId),
        1,
        "acknowledge OpenLineage queue payload " + eventId);
  }

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

    acquireOrderingKeyLock(orderingKey);
    deadLetterLockedAfterLaneLock(orderingKey, eventId, attemptCount, error);
  }

  /** Dead-letters an exact head after all mixed-claim lane locks were acquired in order. */
  default void deadLetterLockedAfterLaneLock(
      UUID orderingKey, long eventId, int attemptCount, String error) {
    requireLockedTransition(orderingKey, eventId);
    requirePositive(attemptCount, "attemptCount");
    requireAttachedReadCommittedTransaction("OpenLineage dead-letter transition after lane lock");

    requireOne(
        insertDeadLetterLocked(orderingKey, eventId, attemptCount, normalizeError(error)),
        "insert OpenLineage dead letter " + eventId);
    requireExpected(
        finishLockedHeadAfterLaneLock(orderingKey, eventId),
        1,
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
          last_error,
          admission_id)
      SELECT queued.ordering_key,
             queued.id,
             queued.event,
             queued.enqueued_at,
             :attemptCount,
             left(CAST(:error AS TEXT), 4096),
             queued.admission_id
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

  private static String requireEventJson(String eventJson) {
    if (eventJson == null) {
      throw new IllegalArgumentException("eventJson is required");
    }
    requireNoNulInSerializedEvent(eventJson);
    return eventJson;
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

  private static void requireClaimLimit(int maxEvents) {
    requirePositive(maxEvents, "maxEvents");
    if (maxEvents > MAX_ADMISSION_EVENTS) {
      throw new IllegalArgumentException("maxEvents must not exceed " + MAX_ADMISSION_EVENTS);
    }
  }

  private static void requireAdmissionLimit(int eventCount, String description) {
    if (eventCount > MAX_ADMISSION_EVENTS) {
      throw new IllegalArgumentException(description + " must not exceed " + MAX_ADMISSION_EVENTS);
    }
  }

  private static void requireStableAdmissionSize(
      int actualCount, int expectedCount, String description) {
    if (actualCount != expectedCount) {
      throw new IllegalArgumentException(description + " changed while being read");
    }
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

  private static void requireExpected(long actualRows, long expectedRows, String action) {
    if (actualRows != expectedRows) {
      throw new IllegalStateException(
          "Expected to " + action + " for " + expectedRows + " row(s), but affected " + actualRows);
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
