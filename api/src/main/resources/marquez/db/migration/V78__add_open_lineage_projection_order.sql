/* SPDX-License-Identifier: Apache-2.0 */

-- TRUE identifies a neutral parent-run identity that an observed event may promote in place.
-- NULL deliberately covers real, promoted, and unclassified pre-V78 runs. There is no safe legacy
-- discriminator, so this migration does not guess which existing rows were synthetic parents.
-- The nullable marker has no index or common-row payload.
ALTER TABLE runs
    ADD COLUMN open_lineage_parent_placeholder BOOLEAN;

-- These nullable pairs order only projections shared by independently claimable queue lanes.
-- A null pair denotes a legacy write. Ordered SQL derives a baseline from the existing row or
-- pointed object before installing the first explicit watermark.
ALTER TABLE jobs
    ADD COLUMN open_lineage_snapshot_time TIMESTAMPTZ,
    ADD COLUMN open_lineage_snapshot_key BYTEA,
    ADD COLUMN open_lineage_current_run_time TIMESTAMPTZ,
    ADD COLUMN open_lineage_current_run_key BYTEA,
    ADD COLUMN open_lineage_current_version_time TIMESTAMPTZ,
    ADD COLUMN open_lineage_current_version_key BYTEA,
    ADD CONSTRAINT jobs_open_lineage_snapshot_order_pair CHECK (
        (open_lineage_snapshot_time IS NULL) = (open_lineage_snapshot_key IS NULL)
        AND (open_lineage_snapshot_key IS NULL OR octet_length(open_lineage_snapshot_key) = 32))
        NOT VALID,
    ADD CONSTRAINT jobs_open_lineage_current_run_order_pair CHECK (
        (open_lineage_current_run_time IS NULL) = (open_lineage_current_run_key IS NULL)
        AND (open_lineage_current_run_key IS NULL OR octet_length(open_lineage_current_run_key) = 32))
        NOT VALID,
    ADD CONSTRAINT jobs_open_lineage_current_version_order_pair CHECK (
        (open_lineage_current_version_time IS NULL) =
        (open_lineage_current_version_key IS NULL)
        AND (open_lineage_current_version_key IS NULL OR
             octet_length(open_lineage_current_version_key) = 32))
        NOT VALID;

ALTER TABLE datasets
    ADD COLUMN open_lineage_snapshot_time TIMESTAMPTZ,
    ADD COLUMN open_lineage_snapshot_key BYTEA,
    ADD COLUMN open_lineage_current_version_time TIMESTAMPTZ,
    ADD COLUMN open_lineage_current_version_key BYTEA,
    ADD CONSTRAINT datasets_open_lineage_snapshot_order_pair CHECK (
        (open_lineage_snapshot_time IS NULL) = (open_lineage_snapshot_key IS NULL)
        AND (open_lineage_snapshot_key IS NULL OR octet_length(open_lineage_snapshot_key) = 32))
        NOT VALID,
    ADD CONSTRAINT datasets_open_lineage_current_version_order_pair CHECK (
        (open_lineage_current_version_time IS NULL) =
        (open_lineage_current_version_key IS NULL)
        AND (open_lineage_current_version_key IS NULL OR
             octet_length(open_lineage_current_version_key) = 32))
        NOT VALID;

ALTER TABLE job_versions
    ADD COLUMN open_lineage_latest_run_time TIMESTAMPTZ,
    ADD COLUMN open_lineage_latest_run_key BYTEA,
    ADD CONSTRAINT job_versions_open_lineage_latest_run_order_pair CHECK (
        (open_lineage_latest_run_time IS NULL) = (open_lineage_latest_run_key IS NULL)
        AND (open_lineage_latest_run_key IS NULL OR
             octet_length(open_lineage_latest_run_key) = 32))
        NOT VALID;
