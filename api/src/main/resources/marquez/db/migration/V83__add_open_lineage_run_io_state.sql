/* SPDX-License-Identifier: Apache-2.0 */

-- Forward-only, per-side run I/O snapshots. Deliberately do not backfill this table: absence of a
-- side remains the compatibility signal to read that side from the cumulative legacy facts.
CREATE TABLE open_lineage_run_io_state (
    run_uuid UUID NOT NULL REFERENCES runs(uuid) ON DELETE CASCADE,
    io_type VARCHAR(6) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_key BYTEA NOT NULL,
    dataset_version_uuids UUID[] NOT NULL,
    PRIMARY KEY (run_uuid, io_type),
    CONSTRAINT open_lineage_run_io_state_type CHECK (io_type IN ('INPUT', 'OUTPUT')),
    CONSTRAINT open_lineage_run_io_state_event_key CHECK (octet_length(event_key) = 32),
    CONSTRAINT open_lineage_run_io_state_occurrences CHECK (
        array_position(dataset_version_uuids, NULL) IS NULL)
);

-- Orders terminal job-version linkage independently for each run. Do not backfill: a null pair is
-- the compatibility state for a run whose existing linkage predates durable OpenLineage ordering.
ALTER TABLE runs
    ADD COLUMN open_lineage_job_version_time TIMESTAMPTZ,
    ADD COLUMN open_lineage_job_version_key BYTEA,
    ADD CONSTRAINT runs_open_lineage_job_version_order_pair CHECK (
        (open_lineage_job_version_time IS NULL) =
        (open_lineage_job_version_key IS NULL)
        AND (open_lineage_job_version_key IS NULL OR
             octet_length(open_lineage_job_version_key) = 32))
        NOT VALID;
