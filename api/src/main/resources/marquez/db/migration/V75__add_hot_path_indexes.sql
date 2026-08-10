/* SPDX-License-Identifier: Apache-2.0 */

-- Flyway runs this migration outside a transaction because every statement uses CONCURRENTLY.
-- This requires flyway.group=false (the project default). If a concurrent build fails, drop any
-- invalid index, repair the Flyway history, and retry. IF NOT EXISTS checks names, not definitions.

-- Current job-version I/O edges are invalidated by job or symlink target. The covering columns also
-- support the split canonical-job-to-dataset branches of the reachable-lineage query.
CREATE INDEX CONCURRENTLY IF NOT EXISTS job_versions_io_mapping_current_job_idx
    ON job_versions_io_mapping (job_uuid, io_type)
    INCLUDE (dataset_uuid)
    WHERE is_current_job_version IS TRUE;

CREATE INDEX CONCURRENTLY IF NOT EXISTS job_versions_io_mapping_current_symlink_idx
    ON job_versions_io_mapping (job_symlink_target_uuid, io_type)
    INCLUDE (dataset_uuid)
    WHERE is_current_job_version IS TRUE
      AND job_symlink_target_uuid IS NOT NULL;

-- Traverse from a current dataset edge to its canonical job without scanning historical mappings.
CREATE INDEX CONCURRENTLY IF NOT EXISTS job_versions_io_mapping_current_dataset_idx
    ON job_versions_io_mapping (dataset_uuid)
    INCLUDE (job_uuid, job_symlink_target_uuid)
    WHERE is_current_job_version IS TRUE;

-- Preserve full-history dataset-side lookups and support the dataset_uuid foreign key.
CREATE INDEX CONCURRENTLY IF NOT EXISTS job_versions_io_mapping_dataset_io_idx
    ON job_versions_io_mapping (dataset_uuid, io_type);

-- Page visible jobs in stable global or namespace-scoped order before enriching the selected rows.
CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_visible_updated_at_uuid_idx
    ON jobs (updated_at DESC, uuid DESC)
    WHERE is_hidden IS FALSE
      AND symlink_target_uuid IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_visible_namespace_updated_at_uuid_idx
    ON jobs (namespace_uuid, updated_at DESC, uuid DESC)
    WHERE is_hidden IS FALSE
      AND symlink_target_uuid IS NULL;

-- Resolve tags only for jobs on the selected page.
CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_tag_mapping_job_uuid_idx
    ON jobs_tag_mapping (job_uuid)
    INCLUDE (tag_uuid);
