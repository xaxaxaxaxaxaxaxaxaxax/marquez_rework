/* SPDX-License-Identifier: Apache-2.0 */

-- A non-null value identifies one atomically admitted HTTP batch. Singleton intake deliberately
-- remains null so its row and index footprint are unchanged. The identifier is internal: order is
-- carried by the existing queue identity, and remaining membership is derived from live rows.
CREATE SEQUENCE open_lineage_queue_admission_id_seq
    AS BIGINT
    CACHE 64
    NO CYCLE;

ALTER TABLE open_lineage_queue
    ADD COLUMN admission_id BIGINT;

ALTER SEQUENCE open_lineage_queue_admission_id_seq
    OWNED BY open_lineage_queue.admission_id;

-- Preserve the request boundary after a terminal transition for diagnosis and retention export.
-- Dead letters do not need a membership index.
ALTER TABLE open_lineage_dead_letters
    ADD COLUMN admission_id BIGINT;
