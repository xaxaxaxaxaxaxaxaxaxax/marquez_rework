/* SPDX-License-Identifier: Apache-2.0 */

LOCK TABLE jobs IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE dataset_symlinks IN ACCESS EXCLUSIVE MODE;
LOCK TABLE datasets IN SHARE ROW EXCLUSIVE MODE;

-- The new null-watermark fallback is updated_at. Preserve legacy deletions by advancing every
-- preexisting hidden row before queued historical events can replay.
UPDATE jobs
SET updated_at = GREATEST(
    COALESCE(updated_at, '-infinity'::timestamptz), statement_timestamp())
WHERE is_hidden IS TRUE;

UPDATE datasets
SET updated_at = GREATEST(
    COALESCE(updated_at, '-infinity'::timestamptz), statement_timestamp())
WHERE is_hidden IS TRUE;

-- datasets(namespace_uuid, name) is the authoritative canonical identity. Repoint a conflicting
-- symlink deterministically to that row, then demote every other alias for the same dataset.
INSERT INTO dataset_symlinks (
    dataset_uuid, name, namespace_uuid, type, is_primary, created_at, updated_at)
SELECT d.uuid, d.name, d.namespace_uuid, NULL, TRUE, d.created_at, d.updated_at
FROM datasets AS d
ORDER BY d.namespace_uuid, d.name, d.uuid
ON CONFLICT (namespace_uuid, name) DO UPDATE
SET dataset_uuid = EXCLUDED.dataset_uuid,
    is_primary = TRUE,
    type = NULL,
    updated_at = GREATEST(
        COALESCE(dataset_symlinks.updated_at, '-infinity'::timestamptz),
        COALESCE(EXCLUDED.updated_at, '-infinity'::timestamptz))
WHERE (dataset_symlinks.dataset_uuid IS DISTINCT FROM EXCLUDED.dataset_uuid
       OR dataset_symlinks.is_primary IS DISTINCT FROM TRUE
       OR dataset_symlinks.type IS NOT NULL
       OR dataset_symlinks.updated_at IS DISTINCT FROM GREATEST(
           COALESCE(dataset_symlinks.updated_at, '-infinity'::timestamptz),
           COALESCE(EXCLUDED.updated_at, '-infinity'::timestamptz)));

UPDATE dataset_symlinks AS alias
SET is_primary = FALSE
FROM datasets AS canonical,
     dataset_symlinks AS canonical_primary
WHERE alias.dataset_uuid = canonical.uuid
  AND canonical_primary.namespace_uuid = canonical.namespace_uuid
  AND canonical_primary.name = canonical.name
  AND canonical_primary.dataset_uuid = canonical.uuid
  AND canonical_primary.is_primary IS TRUE
  AND (alias.namespace_uuid, alias.name) IS DISTINCT FROM
      (canonical.namespace_uuid, canonical.name)
  AND alias.is_primary IS DISTINCT FROM FALSE;
