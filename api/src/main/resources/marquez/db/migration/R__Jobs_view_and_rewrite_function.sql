CREATE OR REPLACE VIEW jobs_view
AS
SELECT j.uuid,
       j.name,
       j.namespace_name,
       j.simple_name    AS simple_name,
       j.parent_job_uuid,
       p.name::text AS parent_job_name,
       j.type,
       j.created_at,
       j.updated_at,
       j.namespace_uuid,
       j.description,
       j.current_version_uuid,
       j.current_job_context_uuid,
       j.current_location,
       j.current_inputs,
       j.symlink_target_uuid,
       j.parent_job_uuid::char(36) AS parent_job_uuid_string,
       j.aliases,
       j.current_run_uuid,
       j.open_lineage_snapshot_time,
       j.open_lineage_snapshot_key,
       j.open_lineage_current_run_time,
       j.open_lineage_current_run_key,
       j.open_lineage_current_version_time,
       j.open_lineage_current_version_key
FROM jobs j
LEFT JOIN jobs p ON j.parent_job_uuid=p.uuid
WHERE j.is_hidden IS FALSE AND j.symlink_target_uuid IS NULL;

CREATE OR REPLACE FUNCTION rewrite_jobs_fqn_table() RETURNS TRIGGER AS
$$
DECLARE
    job_uuid uuid;
    job_updated_at timestamp with time zone;
    new_symlink_target_uuid uuid;
    old_symlink_target_uuid uuid;
    inserted_job jobs_view%rowtype;
    full_name varchar;
BEGIN
    full_name = NEW.name;
    IF NEW.parent_job_uuid IS NOT NULL THEN
        SELECT p.name || '.' || NEW.name INTO full_name
        FROM jobs p
        WHERE p.uuid=NEW.parent_job_uuid;
    END IF;
    INSERT INTO jobs (uuid, type, created_at, updated_at, namespace_uuid, name, simple_name, description,
                      current_version_uuid, namespace_name, current_job_context_uuid,
                      current_location, current_inputs, symlink_target_uuid, parent_job_uuid, current_run_uuid,
                      is_hidden, open_lineage_snapshot_time, open_lineage_snapshot_key,
                      open_lineage_current_run_time, open_lineage_current_run_key,
                      open_lineage_current_version_time, open_lineage_current_version_key)
    SELECT NEW.uuid,
           NEW.type,
           NEW.created_at,
           NEW.updated_at,
           NEW.namespace_uuid,
           full_name,
           NEW.name,
           NEW.description,
           NEW.current_version_uuid,
           NEW.namespace_name,
           NULL,
           NEW.current_location,
           NEW.current_inputs,
           NEW.symlink_target_uuid,
           NEW.parent_job_uuid,
           NEW.current_run_uuid,
           false,
           NEW.open_lineage_snapshot_time,
           NEW.open_lineage_snapshot_key,
           NEW.open_lineage_current_run_time,
           NEW.open_lineage_current_run_key,
           NEW.open_lineage_current_version_time,
           NEW.open_lineage_current_version_key
    ON CONFLICT (namespace_uuid, name)
        DO UPDATE SET updated_at = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL THEN
                            GREATEST(
                              COALESCE(jobs.updated_at, '-infinity'::timestamptz),
                              COALESCE(EXCLUDED.updated_at, statement_timestamp()))
                          WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                                   EXCLUDED.open_lineage_snapshot_key) >
                               ROW(COALESCE(jobs.open_lineage_snapshot_time, jobs.updated_at),
                                   COALESCE(jobs.open_lineage_snapshot_key,
                                            decode(repeat('00', 32), 'hex')))
                            THEN GREATEST(
                              COALESCE(jobs.updated_at, '-infinity'::timestamptz),
                              COALESCE(EXCLUDED.updated_at, statement_timestamp()))
                          ELSE jobs.updated_at
                          END,
                      parent_job_uuid = COALESCE(jobs.parent_job_uuid,
                                                 EXCLUDED.parent_job_uuid),
                      simple_name = CASE
                          WHEN jobs.parent_job_uuid IS NULL
                               AND EXCLUDED.parent_job_uuid IS NOT NULL
                            THEN EXCLUDED.simple_name
                          ELSE jobs.simple_name
                          END,
                      type = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL OR
                               ROW(EXCLUDED.open_lineage_snapshot_time,
                                   EXCLUDED.open_lineage_snapshot_key) >
                               ROW(COALESCE(jobs.open_lineage_snapshot_time, jobs.updated_at),
                                   COALESCE(jobs.open_lineage_snapshot_key,
                                            decode(repeat('00', 32), 'hex')))
                            THEN EXCLUDED.type ELSE jobs.type END,
                      description = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL OR
                               ROW(EXCLUDED.open_lineage_snapshot_time,
                                   EXCLUDED.open_lineage_snapshot_key) >
                               ROW(COALESCE(jobs.open_lineage_snapshot_time, jobs.updated_at),
                                   COALESCE(jobs.open_lineage_snapshot_key,
                                            decode(repeat('00', 32), 'hex')))
                            THEN EXCLUDED.description ELSE jobs.description END,
                      current_location = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL OR
                               ROW(EXCLUDED.open_lineage_snapshot_time,
                                   EXCLUDED.open_lineage_snapshot_key) >
                               ROW(COALESCE(jobs.open_lineage_snapshot_time, jobs.updated_at),
                                   COALESCE(jobs.open_lineage_snapshot_key,
                                            decode(repeat('00', 32), 'hex')))
                            THEN EXCLUDED.current_location ELSE jobs.current_location END,
                      current_inputs = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL OR
                               ROW(EXCLUDED.open_lineage_snapshot_time,
                                   EXCLUDED.open_lineage_snapshot_key) >
                               ROW(COALESCE(jobs.open_lineage_snapshot_time, jobs.updated_at),
                                   COALESCE(jobs.open_lineage_snapshot_key,
                                            decode(repeat('00', 32), 'hex')))
                            THEN EXCLUDED.current_inputs ELSE jobs.current_inputs END,
                      -- update the symlink target if null. otherwise, keep the old value
                      symlink_target_uuid      = COALESCE(jobs.symlink_target_uuid,
                                                          EXCLUDED.symlink_target_uuid),
                      current_run_uuid = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL
                            THEN COALESCE(EXCLUDED.current_run_uuid, jobs.current_run_uuid)
                          ELSE jobs.current_run_uuid END,
                      is_hidden = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL OR
                               ROW(EXCLUDED.open_lineage_snapshot_time,
                                   EXCLUDED.open_lineage_snapshot_key) >
                               ROW(COALESCE(jobs.open_lineage_snapshot_time, jobs.updated_at),
                                   COALESCE(jobs.open_lineage_snapshot_key,
                                            decode(repeat('00', 32), 'hex')))
                            THEN false ELSE jobs.is_hidden END,
                      open_lineage_snapshot_time = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL THEN NULL
                          WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                                   EXCLUDED.open_lineage_snapshot_key) >
                               ROW(COALESCE(jobs.open_lineage_snapshot_time, jobs.updated_at),
                                   COALESCE(jobs.open_lineage_snapshot_key,
                                            decode(repeat('00', 32), 'hex')))
                            THEN EXCLUDED.open_lineage_snapshot_time
                          ELSE jobs.open_lineage_snapshot_time END,
                      open_lineage_snapshot_key = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL THEN NULL
                          WHEN ROW(EXCLUDED.open_lineage_snapshot_time,
                                   EXCLUDED.open_lineage_snapshot_key) >
                               ROW(COALESCE(jobs.open_lineage_snapshot_time, jobs.updated_at),
                                   COALESCE(jobs.open_lineage_snapshot_key,
                                            decode(repeat('00', 32), 'hex')))
                            THEN EXCLUDED.open_lineage_snapshot_key
                          ELSE jobs.open_lineage_snapshot_key END,
                      open_lineage_current_run_time = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL THEN NULL
                          ELSE jobs.open_lineage_current_run_time END,
                      open_lineage_current_run_key = CASE
                          WHEN EXCLUDED.open_lineage_snapshot_time IS NULL THEN NULL
                          ELSE jobs.open_lineage_current_run_key END
                      -- the SELECT statement below will get the OLD symlink_target_uuid in case of update and the NEW
                      -- version in case of insert
    RETURNING uuid,
        updated_at,
        symlink_target_uuid,
        (SELECT symlink_target_uuid FROM jobs j2 WHERE j2.uuid=jobs.uuid)
        INTO job_uuid, job_updated_at, new_symlink_target_uuid, old_symlink_target_uuid;


    -- update the jobs table when updating a job's symlink target
    IF (new_symlink_target_uuid IS NOT NULL AND new_symlink_target_uuid IS DISTINCT FROM old_symlink_target_uuid) THEN
        RAISE INFO 'Updating jobs aliases and symlinks due to % to job % (%)', TG_OP, NEW.name, job_uuid;
        WITH RECURSIVE
            jobs_symlink AS (SELECT j.uuid, j.uuid AS link_target_uuid, j.symlink_target_uuid
                             FROM jobs j
                             -- include only jobs that have symlinks pointing to them to keep this table small
                             INNER JOIN jobs js ON js.symlink_target_uuid=j.uuid
                             WHERE j.symlink_target_uuid IS NULL
                             UNION
                             SELECT j.uuid, jn.link_target_uuid, j.symlink_target_uuid
                             FROM jobs j
                             INNER JOIN jobs_symlink jn ON j.symlink_target_uuid = jn.uuid),
            aliases AS (SELECT s.link_target_uuid,
                               ARRAY_AGG(DISTINCT f.name) AS aliases
                        FROM jobs_symlink s
                        INNER JOIN jobs f ON f.uuid = s.uuid
                        GROUP BY s.link_target_uuid)
        UPDATE jobs
        SET aliases = j.aliases, symlink_target_uuid=j.link_target_uuid
        FROM (
                 SELECT j.uuid,
                        CASE WHEN j.uuid=s.link_target_uuid THEN NULL ELSE s.link_target_uuid END AS link_target_uuid,
                        a.aliases
                 FROM jobs j
                 LEFT JOIN jobs_symlink s ON s.uuid=j.uuid
                 LEFT JOIN aliases a ON a.link_target_uuid = j.uuid
             ) j
        WHERE jobs.uuid=j.uuid;
        UPDATE job_versions_io_mapping
        SET job_symlink_target_uuid=j.symlink_target_uuid
        FROM jobs j
        WHERE job_versions_io_mapping.job_uuid=j.uuid AND j.uuid = NEW.uuid;
    END IF;
    SELECT * INTO inserted_job FROM jobs_view
    WHERE uuid=job_uuid OR (new_symlink_target_uuid IS NOT NULL AND uuid=new_symlink_target_uuid);
    -- A losing ordered write after a legacy delete deliberately leaves the row hidden. Return the
    -- affected base row anyway so durable intake can observe the lost snapshot CAS and ACK it.
    IF NOT FOUND THEN
        SELECT j.uuid,
               j.name,
               j.namespace_name,
               j.simple_name,
               j.parent_job_uuid,
               p.name::text,
               j.type,
               j.created_at,
               j.updated_at,
               j.namespace_uuid,
               j.description,
               j.current_version_uuid,
               j.current_job_context_uuid,
               j.current_location,
               j.current_inputs,
               j.symlink_target_uuid,
               j.parent_job_uuid::char(36),
               j.aliases,
               j.current_run_uuid,
               j.open_lineage_snapshot_time,
               j.open_lineage_snapshot_key,
               j.open_lineage_current_run_time,
               j.open_lineage_current_run_key,
               j.open_lineage_current_version_time,
               j.open_lineage_current_version_key
        INTO inserted_job
        FROM jobs AS j
        LEFT JOIN jobs AS p ON p.uuid = j.parent_job_uuid
        WHERE j.uuid = COALESCE(new_symlink_target_uuid, job_uuid);
    END IF;
    return inserted_job;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_symlinks ON jobs_view;

CREATE TRIGGER update_symlinks
    INSTEAD OF UPDATE OR INSERT
    ON jobs_view
    FOR EACH ROW
EXECUTE FUNCTION rewrite_jobs_fqn_table();
