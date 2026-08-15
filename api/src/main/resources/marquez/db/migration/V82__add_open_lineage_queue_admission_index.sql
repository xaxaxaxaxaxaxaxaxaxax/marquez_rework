/* SPDX-License-Identifier: Apache-2.0 */

-- Flyway runs this migration outside a transaction because every statement uses CONCURRENTLY.
-- This requires flyway.group=false (the project default). IF NOT EXISTS allows a repaired migration
-- to resume safely if an earlier concurrent build completed before Flyway recorded the migration.

-- Batch membership is bounded at durable admission. The payload heap supplies ordering_key and
-- event data, so a single deduplicatable key is sufficient for peer-head discovery.
CREATE INDEX CONCURRENTLY IF NOT EXISTS open_lineage_queue_admission_idx
    ON open_lineage_queue (admission_id)
    WHERE admission_id IS NOT NULL;
