/* SPDX-License-Identifier: Apache-2.0 */

-- Flyway runs this migration outside a transaction because every statement uses CONCURRENTLY.
-- This requires flyway.group=false (the project default). IF EXISTS allows a repaired migration
-- to resume safely if an earlier concurrent drop completed before a later statement failed.

DROP INDEX CONCURRENTLY IF EXISTS jobs_symlink_target_uuid_index;

DROP INDEX CONCURRENTLY IF EXISTS datasetversion_datasetid_idx;

DROP INDEX CONCURRENTLY IF EXISTS job_facets_job_uuid_index;

DROP INDEX CONCURRENTLY IF EXISTS runs_created_at_index;
