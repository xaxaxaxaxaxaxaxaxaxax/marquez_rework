# Cumulative performance profile

Profile date: 2026-08-11

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
