/* SPDX-License-Identifier: Apache-2.0 */

-- Bound dead tuples produced by queue churn and reserve head-page space for
-- HOT head-promotion updates.
CREATE TABLE open_lineage_queue (
    ordering_key UUID NOT NULL,
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    event TEXT NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (ordering_key, id)
) WITH (
    autovacuum_vacuum_threshold = 5000,
    autovacuum_vacuum_scale_factor = 0.02,
    toast.autovacuum_vacuum_threshold = 5000,
    toast.autovacuum_vacuum_scale_factor = 0.02
);

CREATE TABLE open_lineage_queue_heads (
    ordering_key UUID PRIMARY KEY,
    event_id BIGINT NOT NULL,
    available_at TIMESTAMPTZ(3) NOT NULL
        DEFAULT date_trunc('milliseconds', clock_timestamp()),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    refresh_due_on_advance BOOLEAN NOT NULL DEFAULT FALSE,
    last_error TEXT,
    FOREIGN KEY (ordering_key, event_id)
        REFERENCES open_lineage_queue (ordering_key, id)
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_scale_factor = 0.02,
    toast.autovacuum_vacuum_threshold = 1000,
    toast.autovacuum_vacuum_scale_factor = 0.02
);

CREATE INDEX open_lineage_queue_heads_due_idx
    ON open_lineage_queue_heads (available_at);

CREATE TABLE open_lineage_dead_letters (
    ordering_key UUID NOT NULL,
    id BIGINT NOT NULL,
    event TEXT NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL,
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    last_error TEXT,
    dead_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (dead_at, id)
) WITH (
    autovacuum_vacuum_threshold = 5000,
    autovacuum_vacuum_scale_factor = 0.02,
    toast.autovacuum_vacuum_threshold = 5000,
    toast.autovacuum_vacuum_scale_factor = 0.02
);
