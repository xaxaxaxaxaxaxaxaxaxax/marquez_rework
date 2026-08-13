/* SPDX-License-Identifier: Apache-2.0 */

ALTER TABLE jobs
    VALIDATE CONSTRAINT jobs_open_lineage_snapshot_order_pair;
ALTER TABLE jobs
    VALIDATE CONSTRAINT jobs_open_lineage_current_run_order_pair;
ALTER TABLE jobs
    VALIDATE CONSTRAINT jobs_open_lineage_current_version_order_pair;
ALTER TABLE datasets
    VALIDATE CONSTRAINT datasets_open_lineage_snapshot_order_pair;
ALTER TABLE datasets
    VALIDATE CONSTRAINT datasets_open_lineage_current_version_order_pair;
ALTER TABLE job_versions
    VALIDATE CONSTRAINT job_versions_open_lineage_latest_run_order_pair;
