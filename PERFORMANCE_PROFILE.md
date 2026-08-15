# Cumulative performance profile

Profile date: 2026-08-15

This is an indicative, correctness-gated PostgreSQL 14 profile of the rework against the
unmodified repository at `/tmp/marquez-rework-upstream`. It is not a production capacity test.

## Method

- Java 17, PostgreSQL 14 in a fresh Testcontainers database for each variant.
- Deterministic fixture: 5,000 jobs, 25,000 job versions, 10,000 runs, 20,000 datasets,
  100,030 job-version I/O mappings, 40,000 column-lineage rows, facets, fields, and tags.
- One warm-up and seven measured operations per case.
- Four-way comparison: baseline/rework code crossed with V74/V75 indexes.
- Additional leave-one-out profiles for the run and column-lineage indexes.
- Every baseline/rework operation produced the same result checksum.
- Measurements include DAO latency, Jdbi statement count, exact `EXPLAIN (ANALYZE, BUFFERS)`,
  WAL deltas, relation sizes, `pgstatindex`, `VACUUM`, and `REINDEX`.

## Cumulative result

The primary matched native-schema comparison was:

| Operation | Baseline median | Rework median | Speed-up | SQL calls/op |
|---|---:|---:|---:|---:|
| List 100 jobs with runs | 15,224.6 ms | 29.9 ms | 508.6x | 301 -> 2 |
| List 100 runs for a 5,001-run job | 138.3 ms | 16.5 ms | 8.4x | 1 -> 1 |
| Job lineage, depth 10 | 304.8 ms | 4.34 ms | 70.2x | 1 -> 1 |
| Lineage service, depth 10 | 323.5 ms | 12.6 ms | 25.7x | 25 -> 4 |
| Replace 1,000 job-version input edges | 572.5 ms | 38.4 ms | 14.9x | 2,000 -> 2 |
| Upsert/read back 32 x 4 column edges | 14.1 ms | 8.62 ms | 1.64x | 64 -> 2 |

The exact SQL plans improved from 490.2 to 1.89 ms for job paging, 131.2 to 5.70 ms for
run paging, and 313.3 to 1.31 ms for job lineage.

## Index footprint and trim

The first nine-index design added 16,613,376 bytes (15.84 MiB) to 83,222,528 bytes of
pre-existing user indexes in this fixture, a 20% increase. The largest additions were:

| Index | Population | Fresh size |
|---|---:|---:|
| Full-history mapping with included job-version UUID | 100,030 | 7.32 MiB |
| Column latest-pair ordering | 40,000 | 2.84 MiB |
| Current job mapping | 20,030 | 1.45 MiB |
| Current dataset mapping | 20,030 | 1.42 MiB |
| Job-to-tag reverse lookup | 15,000 | 1.08 MiB |
| Run ordering | 10,000 | 0.84 MiB |

Leave-one-out profiling found:

- The run-order index was not selected at either 5,001 or 50,001 runs for the hot job. At
  50,001 runs it used 5.05 MiB and the median was 28.5 ms with it versus 26.3 ms without it.
- The column latest-pair index improved a 40,000-row depth-5 read from 621 to 581 ms (6.5%),
  but increased 128-edge write WAL by 18.5%, prevented HOT updates of `updated_at`, and touched
  about 40,283 shared blocks instead of a 690-block sequential scan plus sort.
- Removing `INCLUDE (job_version_uuid)` from the history mapping index reduced it from
  7.32 MiB to 1.99 MiB on this five-versions-per-key fixture by reducing tuple width and allowing
  PostgreSQL B-tree deduplication.

The final V75 migration therefore has seven indexes: three current mapping access paths, one
narrow full-history mapping lookup, two visible-job pagination indexes, and one job-tag reverse
lookup. It adds 7,168,000 bytes (6.84 MiB), saving 9.01 MiB or 56.9% versus the first design.
The identical-load WAL increase falls from 9.5% to 6.1% versus V74.

V76 additionally drops four pre-existing indexes that are exact left-prefix duplicates of wider
survivors: `jobs_symlink_target_uuid_index`, `datasetversion_datasetid_idx`,
`job_facets_job_uuid_index`, and `runs_created_at_index`. In the small intake cells this recovered
40-106 KiB. Those cells are 8 KiB page-quantized; the durable benefit is linear with the affected
tables. For example, the removed run index occupied 408 KiB at 10,000 runs in the large fixture.

## Mapping-index churn

The stress case repeatedly replaced 1,000 current mappings for one hot job, 100 versions per
cycle. This is intentionally harsher than a typical job.

After two churn-plus-vacuum cycles, the four mapping indexes in the trimmed design occupied
28.97 MiB. Reindexing reduced them to 5.36 MiB: 5.4x inflation with 81.5% of the allocated bytes
reclaimable. `VACUUM` made dead pages/tuples reusable but did not shrink the files.

The current-dataset index plateaued at 9.20 MiB after the first cycle. The current-job index grew
from 7.19 to 15.01 MiB across the second cycle, so equal-key version churn is the main ongoing
risk. The narrow history index grew from 3.52 to 4.75 MiB while its live history grew by another
100,000 mappings.

Operationally, monitor `pg_relation_size`, `pg_stat_user_indexes`, dead tuples, and B-tree leaf
density. If the current indexes continue to exceed roughly twice their compact/live expectation
and cache or latency suffers, use `REINDEX INDEX CONCURRENTLY` with enough capacity for both
copies and replication WAL. Autovacuum should be tuned to the update rate of this table; it will
not return the high-water allocation to the filesystem.

The smallest useful sizing model is:

```text
expected index bytes = fixed B-tree pages + live population * compact bytes per entry
bloat ratio          = pg_relation_size(index) / expected index bytes
```

The fixture calibrations are about 76 bytes/current-job edge, 74 bytes/current-dataset edge,
21 bytes/full-history edge where repeated keys permit B-tree deduplication, 76 bytes/job-tag edge,
77 bytes/visible job for the global index, and 108 bytes/visible job for the namespace index.
Actual history tuples can be wider when keys repeat less. The symlink index was too sparse to
calibrate. Thus the full-history index is the largest unbounded linear-growth risk; the current
mapping and visible-job indexes have bounded live populations but the highest churn/bloat risk.

Warn when the post-vacuum bloat ratio remains at least 1.5 across two samples or B-tree leaf
density falls below 60%. Plan a concurrent reindex at 2.0 when disk, cache, or latency is affected;
continuing growth or a ratio of 4.0 is urgent. The observed mapping ratio of 5.4 qualifies. Track
the populations of current edges, current symlink edges, all historical edges, visible jobs, and
job-tag associations alongside bytes; zero `idx_scan` alone is not evidence that a write-supporting
or foreign-key-supporting index is redundant.

## `POST /lineage` intake profile

The intake comparison exercised the actual HTTP endpoint with four closed-loop clients. It used
48 fresh PostgreSQL 14 databases: four workloads, four cumulative cells, and three interleaved
trials. U74 is pristine upstream at schema V74; P74 uses the final code at the identical schema;
P75 adds the seven indexes; P76 adds the four-index cleanup. Every request returned 201, every
invariant passed, all paired semantic-state SHA-256 digests matched, and the U74/P74 schema and
index fingerprints were identical. Search was disabled to isolate the relational/raw-event path;
the single-bulk OpenSearch change is covered by focused tests rather than these timings.

| Workload | Request p50, U74 -> P76 | Request p95, U74 -> P76 | Requests/s | Tracked SQL executions/request | LSN bytes/request |
|---|---:|---:|---:|---:|---:|
| M0: required-only event | 23.3 -> 22.8 ms (-3%) | 29.7 -> 31.5 ms (+4%) | 166 -> 170 (1.04x) | 12.0 -> 16.1 | 2,540 -> 2,629 (+3%) |
| M1: START/COMPLETE, 6 datasets, 8 fields | 54.6 -> 32.1 ms (-39%) | 592.9 -> 64.0 ms (-89%) | 13.3 -> 101.3 (7.47x) | 152.6 -> 112.5 (-26%) | 29,094 -> 29,406 (+2%) |
| M3: M1 plus 32 fields and 256 column edges | 57.0 -> 35.1 ms (-30%) | 1,855 -> 106 ms (-94%) | 4.32 -> 68.2 (15.9x) | 924.7 -> 749.2 (-19%) | 201,876 -> 179,377 (-11%) |
| HOT: identical COMPLETE replay | 513.7 -> 51.7 ms (-90%) | 538.5 -> 370.8 ms (-32%) | 7.73 -> 37.8 (4.88x) | 243.1 -> 155.0 (-36%) | 30,689 -> 30,659 (flat) |

The speed-ups are medians of the three seed-paired ratios, while displayed absolute values are
medians of the three cell trials. The code-only P74/U74 comparison already produced 7.43x M1,
16.3x M3, and 4.81x HOT throughput, so the substantive intake gains come from transaction and
batching changes rather than the indexes. M0 is neutral; its four extra tracked executions are
transaction-isolation metadata rather than data-path queries.

V75's intake cost is measurable. Relative to P74 it increased total LSN growth per event by about
7% for M0, 13% for M1, 2% for M3, and 6% for HOT. The two `updated_at` job indexes also reduced the
measured jobs-table HOT-update ratio from 0.55-0.94 to zero. V76 recovered 40-106 KiB of index
footprint in these cells, but showed no repeatable latency or WAL change at this scale. No further
drop is justified: the remaining indexes each serve a distinct measured read, invalidation, or
foreign-key access path.

The PostgreSQL 14 collector did not publish stable `xact_commit` deltas from all pooled backends,
so commit counts are excluded. `pg_stat_statements.track=all` also double-counts nested trigger
work and, under HOT replay, the same job-row lock wait; total LSN movement is therefore the primary
WAL measure and endpoint latency is the primary contention measure. Three trials are indicative,
not a production capacity test, but every large endpoint gain was directionally consistent across
all paired trials.

## Second-round intake write batching

A second comparison isolates the additional `/lineage` write-path changes from commit `6232e5eb`
at native P76. It used 32 new PostgreSQL 14 databases: the four workloads, baseline and candidate,
and three interleaved seed-paired trials per workload, extended to seven for M3. All requests
returned 201, every invariant passed, and all 16 paired semantic-state hashes matched. Search
remained disabled. Absolute values below are the median trial values; percentages and multipliers
are medians of the paired ratios.

| Workload | Request p50 | Request p95 | Requests/s | SQL calls/request | SQL ms/request | LSN bytes/request |
|---|---:|---:|---:|---:|---:|---:|
| M0: required-only | 23.5 -> 23.2 ms (-9.1%) | 34.4 -> 33.2 ms (-11.7%) | 156.9 -> 168.1 (1.11x) | 16.1 -> 11.1 (-31.1%) | 0.53 -> 0.52 (-3.5%) | 2,631 -> 2,629 (flat) |
| M1: 6 datasets, 8 fields | 31.3 -> 31.9 ms (+1.1%) | 65.4 -> 63.1 ms (-8.5%) | 104.3 -> 108.5 (1.04x) | 112.5 -> 74.0 (-34.2%) | 2.62 -> 2.39 (-8.9%) | 29,696 -> 29,933 (+0.8%) |
| M3: 32 fields, 256 column edges | 41.5 -> 36.8 ms (-15.7%) | 119.8 -> 109.2 ms (-4.1%) | 59.7 -> 64.6 (1.06x) | 749.2 -> 638.2 (-14.8%) | 10.11 -> 10.30 (+2.5%) | 179,493 -> 179,165 (flat) |
| HOT: identical COMPLETE replay | 53.6 -> 47.7 ms (-11.1%) | 318.8 -> 260.0 ms (-16.3%) | 37.9 -> 40.0 (1.04x) | 155.0 -> 83.0 (-46.4%) | 126.69 -> 119.60 (-7.4%) | 29,869 -> 30,281 (+2.2%) |

The main reduction comes from set-based job-version I/O writes and flushing dataset fields,
version-field mappings, current versions, and run-input mappings in bounded side-level batches.
The smallest useful call-count model is:

```text
field-mapping calls per side: F -> ceil(F / 1000)
column-lineage calls:         1 run-field lookup + ceil(E / 1000) physical inserts
```

Here `F` is the side's field-mapping count and `E` is its resolved physical column-edge count.
Arrays are bounded at 1,000 pairs; duplicate inputs use set semantics, while occurrence-sensitive
dataset projection remains ordered. One outer transaction covers job, run, both dataset sides,
facets, mappings, and column lineage. A canonical base-job row lock also serializes concurrent
primary-name and alias events before run and dataset locks.

A one-statement logical column resolver was benchmarked and rejected. On M3 it reduced one SQL
call but raised database execution time per request by about 58%, p95 by about 8%, and left
throughput slightly worse. The final path deliberately retains the extra cached run-field lookup:
that lookup cost about 0.4 ms per lineage event, while the logical resolver cost about 20 ms. This
is why M3 SQL time remains effectively neutral despite 14.8% fewer executions, while p50, p95,
and throughput improve and WAL remains flat.

## Third-round intake transaction compaction

The third comparison freezes `b0e32478` as the baseline and isolates the next intake-only change
set at native P76. It used 24 fresh PostgreSQL 14 databases: four workloads, two variants, and
three seed-paired trials per workload. All requests returned 201, every invariant passed, and all
12 paired semantic-state SHA-256 digests matched. Absolute values below are medians of the three
trials; percentages are medians of the paired candidate/baseline ratios.

| Workload | Request p50 | Request p95 | Requests/s | SQL calls/request | SQL ms/request | LSN bytes/request |
|---|---:|---:|---:|---:|---:|---:|
| M0: required-only | 25.3 -> 24.9 ms (-0.5%) | 32.8 -> 32.6 ms (-1.7%) | 154.5 -> 156.5 (+1.3%) | 11.1 -> 9.1 (-18.3%) | 0.51 -> 0.56 (+10.5%) | 2,632 -> 2,627 (flat) |
| M1: 6 datasets, 8 fields | 34.3 -> 34.2 ms (-6.9%) | 66.5 -> 62.0 ms (-5.3%) | 96.8 -> 105.8 (+7.0%) | 74.0 -> 61.5 (-16.9%) | 2.52 -> 2.44 (-3.4%) | 29,643 -> 29,929 (+1.0%) |
| M3: 32 fields, 256 column edges | 40.9 -> 38.1 ms (-6.2%) | 114.6 -> 106.5 ms (-6.6%) | 64.1 -> 66.2 (+8.8%) | 638.2 -> 625.6 (-2.0%) | 9.76 -> 9.36 (-4.0%) | 178,902 -> 179,740 (+0.5%) |
| HOT: identical COMPLETE replay | 65.7 -> 54.3 ms (-25.4%) | 319.5 -> 222.4 ms (-23.5%) | 35.0 -> 42.5 (+15.9%) | 83.0 -> 62.0 (-25.3%) | 133.86 -> 109.92 (-16.4%) | 29,721 -> 30,091 (+1.2%) |

The changes compact existing transactional work rather than alter commit boundaries. A
listener-free nonterminal event no longer materializes an unused run snapshot; appending a run
state and linking its start/end pointer uses one statement; missing input and output mappings are
invalidated together; and both-sided current job-version mappings share bounded invalidation and
activation statements. Namespace lookup is read-first, while primary-symlink resolution and base
dataset upserts use bounded, occurrence-reconstructing batches. Dataset facets reuse bounded list
views and intake calls transaction-assuming DAO cores, avoiding redundant transaction wrappers.

For `D` ordinary primary datasets on one side and `U` distinct physical column edges, the smallest
useful statement model is:

```text
primary-symlink resolution calls per side: 2D -> ceil(D / 1000)
base-dataset upsert calls per side:         D -> ceil(D / 1000)
physical column writes:                    ceil(U / 1000)
```

The fast dataset path is deliberately conditional. Alias/symlink facets, repeated primary
identities, or distinct names resolving to a repeated dataset UUID retain the sequential path so
first-occurrence, intermediate-row, input-before-output, and rollback behavior remain compatible.
Physical column edges are normalized once across the event, globally ordered by PostgreSQL's
unsigned UUID order, and use last-occurrence transformation metadata before bounded writes.

A fixed-shape six-array `unnest` writer for physical column edges was separately tested and
rejected. Across the same three M3 seeds it reduced SQL execution time by 5.3%, but endpoint p50
regressed 10.3%, p95 improved only 1.0%, and throughput changed by +0.1%. The final path therefore
retains the bind-bean writer. No V77 schema change was made: the remaining candidate indexes lack
leave-one-out evidence, serve read or foreign-key paths, and isolated drops would not restore HOT
eligibility while other indexed run-state and timestamp columns remain.

## Durable asynchronous intake

The fourth profile compares the synchronous `7dc1343d` endpoint with the first durable
asynchronous intake candidate. These latency figures are retained as historical admission and
projection baselines; that candidate serialized Lineage events by job and predates the final
run-causal queue described below. It also used a two-transaction lease protocol, so its worker-side
measurements are not a performance claim for the landed transaction-scoped row-lock protocol.
The HTTP contract remains: POST returns 201 after a PostgreSQL queue insert commits, and a managed
worker projects it asynchronously. The landed worker selects and row-locks one due head, performs
the relational projection, and acknowledges or records a caught failure in one READ COMMITTED
transaction. External listeners and search run after that database commit and remain best effort.

The HTTP comparison used 36 fresh PostgreSQL 14 databases: four workloads, three cells, and three
interleaved paired trials. `SYNC_7DC` is the exact clean pre-queue revision; `DURABLE_RUNNING` admits
and projects concurrently; `DURABLE_STOPPED` admits the whole backlog with its worker stopped and
then drains it after an application restart. All measured requests returned 201. All 36 cells
drained with zero live rows, dead letters, or unexpected retries. Normalized semantic digests and
per-type counts matched, and dangling/current-version, dataset-facet, and input-version integrity
checks remained zero.

| Workload | Admission p50, sync -> running | Admission p95, sync -> running | Admission requests/s | Running end-to-end requests/s | Paired end-to-end ratio |
|---|---:|---:|---:|---:|---:|
| M0: required-only | 25.7 -> 12.3 ms | 31.5 -> 18.0 ms | 150.5 -> 305.4 | 126.8 | 0.875x |
| M1: 6 datasets, 8 fields | 32.4 -> 12.5 ms | 58.8 -> 17.9 ms | 111.7 -> 301.8 | 95.8 | 0.869x |
| M3: 32 fields, 256 column edges | 40.1 -> 16.4 ms | 119.5 -> 26.0 ms | 59.8 -> 227.8 | 56.8 | 0.947x |
| HOT: identical COMPLETE replay | 53.7 -> 17.5 ms | 285.8 -> 26.2 ms | 42.3 -> 204.5 | 32.9 | 0.875x |

Displayed values are medians of the three cell trials; the final column is the median paired
candidate/baseline ratio. Durable admission was 2.1-5.1x faster and its p95 was 50-91% lower.
Projection completion retained 87-95% of synchronous throughput. With projection stopped,
admission p50 was 9.4-15.4 ms and throughput was 245-400 requests/s; its end-to-end time includes a
new application startup and is not a steady-state projection comparison.

Queue WAL at the 201 boundary was 549 bytes/request for M0, 713 for M1, 2,196 for M3, and 849 for
HOT. After drain, the durable path added about 9-10 tracked SQL executions/request. Total LSN bytes
were 63% higher for the tiny M0 event, where queue metadata dominates, but 17%, 3%, and 23% lower
for M1, M3, and HOT respectively because the queue work is smaller than the write-path savings.

The final queue has exactly one live head per ordering key. Lineage events use a `run` domain over
the canonical UUID produced by `Utils.openLineageRunUuid(runId)`; Job and Dataset events use
separate sanitized namespace/name domains. A key-local transaction advisory lock orders
concurrent inserts before identity allocation. Polls read only due heads through the
single-column `available_at` index and use `FOR UPDATE SKIP LOCKED`; selection, projection, and ACK
or caught-failure transition share the same transaction. Commit makes the projection and queue
transition visible together; rollback releases the lock and leaves the event unchanged. Different
runs of one job can be selected independently, while events for one run remain FIFO.
Shared job and dataset snapshots converge by `(UTC event time, SHA-256 of the exact queued JSON)`
instead of relying on scheduler-wide job serialization.
`refresh_due_on_advance` is durable q2 scheduling state. Promotions alternate between a
HOT-eligible update that omits indexed `available_at` and an update that refreshes it to
millisecond-floored database time, bounding clock preservation to two successful promotions.
Retry refreshes the clock and resets the bit. This reduces indexed head updates; it does not
promise that another lane runs between the two promotions.

The head lock is acquired before the projection savepoint. A caught projection failure rolls back
partial projection writes to that savepoint and commits either retry state or a dead letter;
`maxAttempts` counts only those committed caught failures. Before commit, an uncaught database
error, lost connection, process crash, or PostgreSQL crash that aborts the transaction consumes no
attempt and leaves the head eligible when PostgreSQL releases it. A connection or process failure
while the commit result is being returned is indeterminate: projection and acknowledgement may
already be committed, so the worker fails health rather than inferring replay eligibility. Database
projection and queue state are atomic, but commit observation, best-effort post-commit listeners,
and asynchronous failover remain at-least-once boundaries.

Each active event pins one JDBC connection for its transaction. Size the pool for all configured
processors plus independent HTTP capacity, and monitor old `pg_stat_activity.xact_start` values:
a long transaction holds its row lock and can delay vacuum cleanup. `statement_timeout` bounds an
individual SQL statement, while `idle_in_transaction_session_timeout` bounds idle gaps; neither is
a general PG14 total-transaction deadline. Apply database timeouts only to a dedicated queue
role/session. There is no per-event application cancellation deadline; forced shutdown relies on
task interruption followed by synchronous connection abort to roll back unfinished transactions.

Worker shutdown has two intervals of `shutdownGracePeriodMillis`. The first drains cooperatively.
If it expires or shutdown is interrupted, the forced phase interrupts remaining tasks and invokes
JDBC `Connection.abort` synchronously for every registered connection before the second
deadline-bounded executor wait. The two waits are bounded, but the synchronous driver abort calls
between them have no application-enforced deadline and can extend total stop time if a driver
blocks. A completed abort rolls back the open transaction and releases its head lock.
`shutdown_incomplete` means a coordinator or processor thread, registered connection, or
connection-abort failure was still present after that process; there is no delayed queue-recovery
timer.

The smallest useful model is:

```text
H                     = number of nonempty ordering keys
live selectable rows  = H
B                     = requested poll batch; production fixes B = 1
L                     = earlier due heads locked by concurrent transactions
D                     = dead or invisible index entries encountered by PostgreSQL
live-row poll work    = O(B + L)
physical index work   = O(B + L + D)
same-run concurrency  = 1
queue bytes           = fixed pages + sum(serialized payload bytes + row/index overhead) + O(H)
```

With `W` processors holding at most one head each, `L <= W - 1`; production therefore has a live
candidate bound of `O(W)` for `B = 1`, independent of queue cardinality. A transaction held by a
slow processor keeps its lane locked and lets later due lanes pass it until commit or rollback.
Vacuum and the due-index statistics determine `D`, so the live-row bound is not a promise of
constant physical page reads in a bloated relation.

The replaced anti-predecessor selection scanned deferred followers. With 100,000 due followers behind
one unavailable hot head plus 100 independent heads, it took 308.49 ms and visited 100,099 due
rows/probes. The compact-head plan returned the independent work in 0.842 ms while visiting 101
head-index rows, a 366x reduction. At 1,000 followers it improved 3.882 to 1.581 ms. A 100,100-row
tiny-payload fixture occupied 9.68 MiB of heap and 6.10 MiB of indexes, 15.82 MiB total including
auxiliary storage.

The job-domain predecessor reduced canonical job-row lock waits: in one M1 profile its three
`JobDao.upsertJob` totals fell from 1.67-1.80 seconds to 40.5-42.8 ms per 120 requests. A paired JFR
diagnostic also showed 213 -> 154 execution samples and 77.1 -> 61.5 ms total GC pause. It achieved
that by serializing independent runs, however, and the four-job profile increased drain time and
reduced end-to-end throughput. The final design keeps run-local FIFO and makes shared projection
state monotonic, accepting ordinary row-lock contention without turning the queue into a per-job
scheduler.

### Million-event queue gate

The final scale gate uses frozen JOBKEY and run-causal source snapshots with PostgreSQL durability
settings intact. It measures 100,000 and 1,000,000-row HOT, MANY, MIX, and BLOCKED fixtures;
fixed-work poll/follower/head/enqueue plans; storage and WAL slopes; an exact eight-worker
100,000-event transaction-stress prefix at a one-million-row high-water mark; lossless bulk
reclamation of the remaining rows; vacuum/refill reuse; real worker sensitivity; HTTP admission
and drain; and one JFR diagnostic. Bulk reclamation is explicitly excluded from throughput and WAL
comparisons. Results are recorded only after the fail-closed source manifest and DAO/schema/worker
adapter review match the production files.

### Batch admission throughput

The opt-in `OpenLineageBatchThroughputBenchmark` compares the actual HTTP admission paths with
four closed-loop clients. Each cell submits 16,384 small required-only RunEvents with distinct run
IDs, so every event exercises a distinct ordering lock and creates a distinct queue head. The
single cell sends one event per `POST /lineage`; the batch cell sends 128 events per
`POST /lineage/batch`. Request construction is outside the timed interval. New heads are scheduled
at infinity during each cell so the asynchronous projector cannot consume CPU or database capacity;
this is an admission-throughput measurement, not projection throughput.

The Java 17/PostgreSQL 14 run used 8,192 warm-up events per path followed by seven measured cells
per path. Cell order alternated, the queue was truncated between cells, and every response status,
queue row, unique-lane head, and zero-dead-letter invariant was checked.

| Events/request | HTTP route | Median events/s | Cell range | Median requests/s |
|---:|---|---:|---:|---:|
| 1 | `POST /lineage` | 3,084.5 | 2,216.6–3,154.4 | 3,084.5 |
| 128 | `POST /lineage/batch` | 42,223.0 | 39,721.5–44,812.4 | 329.9 |

The median paired throughput gain was **13.88x**. The first single-event cell was colder at 2,216.6
events/s; the other six were 3,009.5–3,154.4 events/s, so the seven-cell median is not driven by
that outlier. Batch throughput varied by about +/-6% around its median.

The smallest useful throughput model is:

```text
event throughput = request throughput * events per request
```

Batching does not approach the theoretical 128x ceiling because parsing, validation,
serialization, identity allocation, queue-row writes, and per-lane lock/head work remain
per-event. It nevertheless removes 127 HTTP exchanges and 127 queue transactions per 128 events.
This fixture deliberately uses all-distinct lanes; repeated events for the same run deduplicate
advisory-lock and head work and can have a different throughput profile. Results are indicative for
one local machine and do not establish production capacity.

Run the benchmark explicitly; it is disabled in normal test execution:

```text
JAVA_TOOL_OPTIONS=-DrunOpenLineageBatchThroughputBenchmark=true \
  ./gradlew --rerun-tasks :api:testIntegration \
  --tests marquez.OpenLineageBatchThroughputBenchmark
```

## Native bespoke stats-query profile

The bounded `/api/v1/stats/query` path was measured on Java 17 and PostgreSQL 14 with 100,000
lineage events, 5,000 jobs, 20,000 datasets, and 1,000 sources. Each case returned 732 fixed
12-hour buckets across the maximum 366-day range. One warm-up preceded seven alternating-order
measurements, and every result passed a SHA-256 stability checksum before its timing was printed.

| Query shape | Median | Seven-run range |
|---|---:|---:|
| Lineage, global | 12.82 ms | 12.03–14.13 ms |
| Lineage, hot namespace | 13.34 ms | 12.14–14.23 ms |
| Lineage, rare job | 2.38 ms | 1.55–3.04 ms |
| Lineage, rare run | 2.21 ms | 1.21–3.09 ms |
| Jobs, global | 3.12 ms | 2.23–4.35 ms |
| Jobs, hot namespace | 3.15 ms | 2.36–4.78 ms |
| Datasets, global | 4.75 ms | 3.97–5.61 ms |
| Datasets, hot namespace | 5.33 ms | 4.73–5.86 ms |
| Sources, global | 2.47 ms | 1.83–2.77 ms |

An independent deterministic verification runs every production SQL shape through `EXPLAIN
(ANALYZE, BUFFERS)` and checks one bucket generator, one physical scan of the selected base
relation with one loop, at most 1,000 output rows, and no temporary spill. It also compares every
series to a Java oracle across exact boundaries, empty/cold scopes, a clipped one-bucket range,
1,000 buckets, and 366 days. No schema, index, materialized-view, or ingestion write was added.

For `B <= 1000` output buckets, `R` candidate lineage rows visited by the chosen bounded plan, and
`N` retained rows visited in the selected stock relation, the implementation model is:

```text
flow query work   = O(R + B)
stock query work  = O(N + B)
aggregation state = O(B)
SQL statements    = 1
```

These are warm-cache, single-client figures. They verify large headroom beneath the five-second
statement timeout for this fixture, not production capacity or connection-pool behavior under
concurrency. Run the opt-in benchmark explicitly:

```text
JAVA_TOOL_OPTIONS="-DrunStatsQueryBenchmark=true -Dapi.version=1.40" \
  ./gradlew :api:test --tests marquez.db.StatsQueryBenchmark
```

## Caveats

- Latencies are warm-cache, single-client medians from one machine, not confidence intervals over
  multiple hosts or JVM forks.
- Absolute index bytes depend on UUID distribution, tuple width, fill state, history per key, and
  PostgreSQL version. Extrapolations are approximate.
- The symlink partial index was nearly empty in the size fixture; correctness tests cover its
  access path, but a symlink-heavy production profile remains useful.
- The column latest-pair index may be worth restoring for an explicitly read-heavy deployment.
  The observed latency-only break-even is approximately one depth-5 read per 26 populated
  128-edge writes, before pricing WAL, vacuum, and cold-cache effects.
