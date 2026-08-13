# Metrics

| Metric                           | Type    | Tags                                                       | Description                         |
|----------------------------------|---------|------------------------------------------------------------|-------------------------------------|
| `marquez_namespace_total`        | _count_ |                                                            | Total number of namespaces.         |
| `marquez_source_total`           | _count_ | `source_type`                                              | Total number of sources.            |
| `marquez_dataset_total`          | _count_ | `namespace_name`, <br> `dataset_type`                      | Total number of datasets.           |
| `marquez_dataset_versions_total` | _count_ | `namespace_name`, <br> `dataset_type`, <br> `dataset_name` | Total number of dataset versions.   |
| `marquez_job_total`              | _count_ | `namespace_name`, <br> `job_type`                          | Total number of jobs.               |
| `marquez_job_versions_total`     | _count_ | `namespace_name`, <br> `job_type`, <br> `job_name`         | Total number of job versions.       |
| `marquez_job_runs_active`        | _gauge_ |                                                            | Total number of active job runs.    |
| `marquez_job_runs_completed`     | _gauge_ |                                                            | Total number of completed job runs. |

## Durable OpenLineage intake

The durable intake worker publishes Codahale metrics under
`marquez.service.OpenLineageWorker`. The most useful base names are:

| Metric suffix | Type | Description |
|---------------|------|-------------|
| `running`, `coordinator_alive` | _gauge_ | Local coordinator run-loop state and thread liveness. A fatal coordinator error sets both to zero while durable HTTP admission remains available. |
| `processor_capacity`, `available_processor_capacity` | _gauge_ | Configured processor slots and slots not currently assigned a drain task. |
| `in_flight` | _counter_ | Processor drain tasks currently running. A task can process several events sequentially, with one transaction and one locked head at a time. |
| `selected`, `succeeded`, `retried`, `dead_lettered` | _meter_ | `selected` is marked after a due head is locked. The other meters are marked only after the corresponding success, retry, or dead-letter transaction commits. |
| `poll_failed`, `task_failed`, `coordinator_failed`, `state_transition_failed`, `post_commit_failed` | _meter_ | Failures requiring operator attention. `task_failed` means a fatal processor-task error escaped. If a row was selected, its transaction may have rolled back, or its commit result may be indeterminate if the database connection failed while returning it. `coordinator_failed` records a fatal local run-loop failure; the worker deliberately does not restart itself. `post_commit_failed` counts failed best-effort search-index or run-transition-listener callbacks after queue acknowledgement; these side effects have no durable delivery guarantee, may be missing, concurrent, or out of order, and are not retried by the intake queue. A fatal post-commit callback increments both `post_commit_failed` and `task_failed`. |
| `forced_shutdown`, `shutdown_incomplete` | _meter_ | `forced_shutdown` counts escalation from graceful drain to task interruption and synchronous JDBC connection abort. `shutdown_incomplete` counts a stop that returns after its two deadline-bounded executor waits with a coordinator or processor thread, registered connection, or connection-abort failure still present. Synchronous driver abort between the waits has no application-enforced deadline. |
| `poll_empty` | _counter_ | Polls for which no due, unlocked head was selected. |
| `poll_duration`, `processing_duration`, `post_commit_duration` | _timer_ | Head-selection query latency, processing work while the head is locked (ending before the outer commit), and best-effort post-commit publishing latency. |

These are Codahale metric names without labels. Do not treat the suffixes as a labeled Prometheus
family. The exporter normalizes each registered name and adds its type-specific suffixes.
`selected` can exceed `succeeded + retried + dead_lettered`: cancellation, a confirmed
whole-transaction rollback, or process loss can occur after selection without an exported committed
outcome. A connection failure while returning a commit result is indeterminate: projection and queue
acknowledgement may already be committed even though no success meter was published. A process can
also disappear before exporting the corresponding failure.

These worker metrics are per application instance; use a sum for event rates and failures, but
inspect lifecycle and capacity gauges per instance.

The Dropwizard health check named `open-lineage-worker` is healthy only after the worker starts and
while its coordinator is live and draining is enabled, no fatal task or coordinator failure has
occurred, and queue polling has not failed three consecutive times. A successful poll clears the
persistent-poll condition; a caught projection failure that commits retry or dead-letter state is a
healthy worker outcome. Shutdown makes the check unhealthy. Durable HTTP admission can still
succeed while this check is unhealthy, so alert on it before backlog age grows.

Database backlog and storage observations are intentionally not collected during application
requests or Prometheus scrapes. Run the following PostgreSQL queries at low frequency, preferably
no more than once per minute, from an operator connection. Always use a statement timeout so an
observation cannot compete indefinitely with intake:

```sql
BEGIN;
SET LOCAL statement_timeout = '5s';

-- Approximate row counts and vacuum progress for all durable-intake relations.
SELECT relname,
       n_live_tup AS approximate_rows,
       n_dead_tup AS approximate_dead_tuples,
       n_tup_upd AS updated_tuples,
       n_tup_hot_upd AS hot_updated_tuples,
       last_autovacuum,
       autovacuum_count
FROM pg_stat_user_tables
WHERE schemaname = current_schema()
  AND relname IN (
      'open_lineage_queue',
      'open_lineage_queue_heads',
      'open_lineage_dead_letters')
ORDER BY relname;

-- Heap, index, TOAST, and total retained allocation.
SELECT c.relname,
       pg_relation_size(c.oid) AS heap_bytes,
       pg_indexes_size(c.oid) AS index_bytes,
       CASE WHEN c.reltoastrelid = 0 THEN 0
            ELSE pg_total_relation_size(c.reltoastrelid)
       END AS toast_bytes,
       pg_total_relation_size(c.oid) AS total_bytes
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = current_schema()
  AND c.relkind = 'r'
  AND c.relname IN (
      'open_lineage_queue',
      'open_lineage_queue_heads',
      'open_lineage_dead_letters')
ORDER BY c.relname;

COMMIT;
```

The row estimates may lag current state, but avoid queue-scale `count(*)` scans. Inspect exact age
and due-state only during diagnosis and with the same timeout:

```sql
BEGIN;
SET LOCAL statement_timeout = '5s';

SELECT statement_timestamp() - min(enqueued_at) AS oldest_live_age
FROM open_lineage_queue;

SELECT count(*) AS lane_count,
       count(*) FILTER (
           WHERE available_at <= statement_timestamp()) AS due_lanes,
       count(*) FILTER (
           WHERE available_at > statement_timestamp()) AS scheduled_retry_lanes,
       statement_timestamp() - min(available_at) FILTER (
           WHERE available_at <= statement_timestamp()) AS oldest_due_age
FROM open_lineage_queue_heads;

SELECT statement_timestamp() - dead_at AS oldest_dead_letter_age
FROM open_lineage_dead_letters
ORDER BY dead_at, id
LIMIT 1;

COMMIT;
```

The oldest-live and due-state queries may scan their relations and should not be promoted into
scrape-time metrics without production evidence for an index that offsets their intake write cost.
`available_at` is the indexed retry and scheduling clock. Successor promotion uses a durable
one-bit quantum: after head creation, retry, or an indexed refresh, the first successful promotion
omits `available_at` from its update and sets `refresh_due_on_advance`; the next successful
promotion refreshes `available_at` to millisecond-floored database time and clears the bit. Retry
also refreshes the clock and clears the bit. Acknowledgement and dead-letter advancement use the
same alternation. The clock is therefore refreshed no later than every second successful
promotion; this is a write quantum, not a guarantee that another lane runs between promotions.

Each poll requests one head (`B = 1`) with `FOR UPDATE SKIP LOCKED`. If `L` earlier due heads are
locked by other transactions, the live-candidate scan is `O(B + L)`, or `O(1 + L)`. With `W`
processors holding at most one head each, `L <= W - 1`. Dead or invisible index entries add
physical work beyond this live-row bound, so compare low-frequency deltas from the due index:

```sql
SELECT indexrelname,
       idx_scan,
       idx_tup_read,
       idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = current_schema()
  AND indexrelname = 'open_lineage_queue_heads_due_idx';
```

Use `pg_stat_activity` to find old queue transactions, idle transactions, connection-pool
pressure, and lock waits. Narrow `application_name` to the value used by the deployment when it is
shared with other clients:

```sql
SELECT pid,
       application_name,
       state,
       now() - xact_start AS xact_age,
       now() - query_start AS query_age,
       wait_event_type,
       wait_event,
       backend_xid,
       backend_xmin,
       pg_blocking_pids(pid) AS blocking_pids,
       left(query, 160) AS query
FROM pg_stat_activity
WHERE datname = current_database()
  AND backend_type = 'client backend'
  AND xact_start IS NOT NULL
ORDER BY xact_start;
```

Inspect relation locks and waits involving queue relations with:

```sql
SELECT l.pid,
       a.application_name,
       l.locktype,
       l.mode,
       l.granted,
       l.relation::regclass AS relation,
       l.page,
       l.tuple,
       l.transactionid,
       pg_blocking_pids(l.pid) AS blocking_pids,
       now() - a.xact_start AS xact_age
FROM pg_locks AS l
JOIN pg_stat_activity AS a USING (pid)
WHERE a.datname = current_database()
  AND (l.relation IN (
           to_regclass('open_lineage_queue'),
           to_regclass('open_lineage_queue_heads'))
       OR cardinality(pg_blocking_pids(l.pid)) > 0)
ORDER BY l.granted, a.xact_start;
```

PostgreSQL stores an uncontended row lock in the tuple header, so `pg_locks` is not a count of all
heads currently locked by workers. Tuple locks normally appear there only when a process is
waiting; `pg_stat_activity`, `pg_blocking_pids`, and transaction age are the authoritative
operational signals. Reading other sessions' full query text may require `pg_read_all_stats` or
equivalent privileges.

Selection, relational projection, and acknowledgement or failure transition share one
READ COMMITTED transaction and one locked head. A caught projection failure rolls back to its
savepoint, then commits a retry or dead letter and increments the persisted caught-failure count
once. A confirmed whole transaction rollback, including connection loss before commit or a database
crash that aborts the transaction, consumes no `maxAttempts` attempt and makes the unchanged head
eligible again after PostgreSQL releases it. If the connection fails while returning the commit
result, the outcome is indeterminate: projection and acknowledgement may already be committed, and
operators must inspect durable state rather than infer replay eligibility from `task_failed`.

Live queued events are never deleted by age. When `dbRetention` is configured, each completed
retention invocation that reaches the dead-letter phase removes at most one
`numberOfRowsPerBatch` batch of dead letters older than `retentionDays`. Each application replica
schedules its own fixed-rate retention job. Long-running legacy metadata retention can delay the
dead-letter phase, and an overdue fixed-rate iteration may begin immediately after the preceding
one completes. Therefore, `numberOfRowsPerBatch` multiplied by replicas is not a hard wall-clock
maximum for a frequency-sized interval. Database failures are logged and retried on the next
scheduled iteration. If scheduled retention is not configured and no one-off invocation is made,
dead letters remain indefinitely. The one-off `db-retention --dry-run` command does not delete
retained rows, but it creates or replaces its persistent PostgreSQL estimation helper and requires
DDL permission.

Migration V77 installs queue-specific autovacuum thresholds because the live and head relations
churn continuously: a scale factor of `0.02`, with dead-tuple thresholds of `5000` for live/dead
payload relations and `1000` for heads; the same threshold and scale factor apply to their TOAST
relations. The heads heap uses fillfactor 80 to reserve same-page space for HOT-eligible head
promotions. PostgreSQL makes autovacuum eligible when the estimated dead tuples exceed the
configured threshold plus the configured scale factor multiplied by the relation's estimated tuple
count. In concrete terms, the trigger is
`5000 + 0.02 × tuples` for `open_lineage_queue` and `open_lineage_dead_letters`, and
`1000 + 0.02 × tuples` for `open_lineage_queue_heads`, with the corresponding threshold and scale
factor applied independently to each relation's TOAST table. Treat these as starting values,
observe dead tuples and autovacuum duration under a
representative enqueue, drain, retry, dead-letter, and purge workload, and adjust only with
workload evidence. Ordinary vacuum makes freed space reusable but does not reduce a relation's
allocated high-water size.

----
SPDX-License-Identifier: Apache-2.0
Copyright 2018-2023 contributors to the Marquez project.
