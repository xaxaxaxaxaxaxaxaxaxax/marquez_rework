/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.OpenLineageDao.DEFAULT_NAMESPACE_OWNER;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URL;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.JobName;
import marquez.common.models.JobType;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.JobVersionDao.IoType;
import marquez.db.JobVersionDao.JobDataset;
import marquez.db.JobVersionDao.JobDatasetMapper;
import marquez.db.mappers.JobMapper;
import marquez.db.mappers.JobRowMapper;
import marquez.db.mappers.RunMapper;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.service.models.Job;
import marquez.service.models.JobMeta;
import marquez.service.models.Run;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.postgresql.util.PGobject;

@RegisterRowMapper(JobRowMapper.class)
@RegisterRowMapper(JobMapper.class)
@RegisterRowMapper(JobDatasetMapper.class)
@RegisterRowMapper(RunMapper.class)
public interface JobDao extends BaseDao {

  String FIND_ALL_SQL_PREFIX =
      """
        WITH jobs_view_page AS MATERIALIZED (
          SELECT
            j.*
          FROM
            jobs_view AS j
          LEFT JOIN runs AS r
            ON r.uuid = j.current_run_uuid
          WHERE
      """;

  String FIND_ALL_GLOBAL_SCOPE = "            TRUE\n";

  String FIND_ALL_NAMESPACE_SCOPE =
      """
            j.namespace_uuid = (
              SELECT n.uuid
              FROM namespaces AS n
              WHERE n.name = :namespaceName
            )
      """;

  String FIND_ALL_SQL_SUFFIX =
      """
          AND
            (r.current_run_state IN (<lastRunStates>) OR r.uuid IS NULL)
          ORDER BY
            j.updated_at DESC,
            j.uuid DESC
          LIMIT :limit OFFSET :offset
        ),
        job_versions_temp AS (
          SELECT
            jv.uuid,
            jv.latest_run_uuid
          FROM
            job_versions AS jv
          INNER JOIN jobs_view_page AS j
            ON j.current_version_uuid = jv.uuid
        ),
        facets_temp AS (
          SELECT
            jf.run_uuid,
            JSON_AGG(jf.facet ORDER BY jf.lineage_event_time ASC) AS facets
          FROM
            job_facets_view AS jf
          INNER JOIN job_versions_temp AS jv
            ON jv.latest_run_uuid = jf.run_uuid
          GROUP BY
            jf.run_uuid
        ),
        job_tags AS (
          SELECT
            j.uuid,
            ARRAY_AGG(t.name ORDER BY t.name) AS tags
          FROM
            jobs_view_page AS j
          INNER JOIN jobs_tag_mapping AS jtm
            ON jtm.job_uuid = j.uuid
          INNER JOIN tags AS t
            ON jtm.tag_uuid = t.uuid
          GROUP BY
            j.uuid
        )
        SELECT
          j.*,
          f.facets,
          COALESCE(jt.tags, ARRAY[]::VARCHAR[]) AS tags
        FROM
          jobs_view_page AS j
        LEFT OUTER JOIN job_versions_temp AS jv
          ON jv.uuid = j.current_version_uuid
        LEFT OUTER JOIN facets_temp AS f
          ON f.run_uuid = jv.latest_run_uuid
        LEFT OUTER JOIN job_tags AS jt
          ON j.uuid = jt.uuid
        ORDER BY
          j.updated_at DESC,
          j.uuid DESC
      """;

  String FIND_ALL_GLOBAL_SQL = FIND_ALL_SQL_PREFIX + FIND_ALL_GLOBAL_SCOPE + FIND_ALL_SQL_SUFFIX;

  String FIND_ALL_NAMESPACE_SQL =
      FIND_ALL_SQL_PREFIX + FIND_ALL_NAMESPACE_SCOPE + FIND_ALL_SQL_SUFFIX;

  @SqlQuery(
      """
        SELECT EXISTS (
          SELECT 1 FROM jobs_view AS j
          WHERE j.namespace_name = :namespaceName AND
          j.name = :jobName)
      """)
  boolean exists(String namespaceName, String jobName);

  @SqlUpdate(
      """
        UPDATE jobs
        SET updated_at = :updatedAt,
            current_version_uuid = :currentVersionUuid
        WHERE uuid = :rowUuid
      """)
  void updateVersionFor(UUID rowUuid, Instant updatedAt, UUID currentVersionUuid);

  @SqlQuery(
      """
        WITH job_versions_facets AS (
            SELECT
                f.job_version_uuid
            ,   JSON_AGG(f.facet) as facets
            FROM
                job_facets f
            LEFT JOIN
                jobs_view j on j.current_version_uuid = f.job_version_uuid
            WHERE
                j.namespace_name=:namespaceName AND (j.name=:jobName OR :jobName = ANY(j.aliases))
            GROUP BY
                job_version_uuid
        ),
        job_tags as (
        SELECT
            j.uuid
        ,   ARRAY_AGG(t.name) as tags
        FROM
            jobs j
        INNER JOIN
            jobs_tag_mapping jtm
        ON
            jtm.job_uuid = j.uuid
        AND
            j.simple_name = :jobName
        AND
            j.namespace_name = :namespaceName
        INNER JOIN
            tags t
        ON
            jtm.tag_uuid = t.uuid
        GROUP BY
          j.uuid
        )
        SELECT
            j.*
        ,   facets
        ,   jt.tags as tags
        FROM
            jobs_view j
        LEFT OUTER JOIN
            job_versions_facets f
        ON
            j.current_version_uuid = f.job_version_uuid
        LEFT OUTER JOIN
            job_tags jt
        ON
            j.uuid = jt.uuid
        WHERE
            j.namespace_name = :namespaceName
        AND
            (j.name = :jobName OR :jobName = ANY(j.aliases))
      """)
  Optional<Job> findJobByName(String namespaceName, String jobName);

  @SqlUpdate(
      """
        UPDATE jobs
        SET is_hidden = true
        WHERE namespace_name = :namespaceName
        AND name = :name
      """)
  void delete(String namespaceName, String name);

  @SqlUpdate(
      """
      UPDATE jobs
      SET is_hidden = true
      FROM namespaces n
      WHERE jobs.namespace_uuid = n.uuid
      AND n.name = :namespaceName
      """)
  void deleteByNamespaceName(String namespaceName);

  default Optional<Job> findWithDatasetsAndRun(String namespaceName, String jobName) {
    Optional<Job> job = findJobByName(namespaceName, jobName);
    job.ifPresent(
        j -> {
          List<Run> runs = createRunDao().findByLatestJob(namespaceName, jobName, 10, 0);
          this.setJobData(runs, j);
          this.setJobDataset(
              createJobVersionDao().findCurrentInputOutputDatasetsFor(namespaceName, jobName), j);
        });
    return job;
  }

  @SqlQuery(
      """
        SELECT j.*, n.name AS namespace_name
        FROM jobs_view AS j
        INNER JOIN namespaces AS n ON j.namespace_uuid = n.uuid
        WHERE j.uuid=:jobUuid
      """)
  Optional<JobRow> findJobByUuidAsRow(UUID jobUuid);

  @SqlQuery(
      """
        SELECT j.*, n.name AS namespace_name
        FROM jobs_view AS j
        INNER JOIN namespaces AS n ON j.namespace_uuid = n.uuid
        WHERE j.namespace_name=:namespaceName AND
          (j.name=:jobName OR :jobName = ANY(j.aliases))
      """)
  Optional<JobRow> findJobByNameAsRow(String namespaceName, String jobName);

  @SqlQuery(FIND_ALL_GLOBAL_SQL)
  List<Job> findAllGlobal(
      @BindList("lastRunStates") List<RunState> lastRunStates, int limit, int offset);

  @SqlQuery(FIND_ALL_NAMESPACE_SQL)
  List<Job> findAllForNamespace(
      String namespaceName,
      @BindList("lastRunStates") List<RunState> lastRunStates,
      int limit,
      int offset);

  default List<Job> findAll(
      String namespaceName, List<RunState> lastRunStates, int limit, int offset) {
    return namespaceName == null
        ? findAllGlobal(lastRunStates, limit, offset)
        : findAllForNamespace(namespaceName, lastRunStates, limit, offset);
  }

  @SqlQuery("SELECT count(*) FROM jobs_view AS j WHERE symlink_target_uuid IS NULL")
  int count();

  @SqlQuery(
      """
      select
          count(*)
      from
          runs
      where
          namespace_name = :namespaceName
      and
          job_name = :job
      ;
      """)
  int countJobRuns(String namespaceName, String job);

  @SqlQuery(
      """
      SELECT count(*)
      FROM jobs_view AS j
      WHERE j.namespace_uuid = (
        SELECT n.uuid
        FROM namespaces AS n
        WHERE n.name = :namespaceName
      )
      AND j.symlink_target_uuid IS NULL
      """)
  int countForNamespace(String namespaceName);

  default int countFor(String namespaceName) {
    return namespaceName == null ? count() : countForNamespace(namespaceName);
  }

  default List<Job> findAllWithRun(
      String namespaceName, List<RunState> lastRunStates, int limit, int offset) {
    List<Job> jobs = findAll(namespaceName, lastRunStates, limit, offset);
    Set<UUID> currentRunUuids =
        jobs.stream()
            .map(Job::getCurrentRunUuid)
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());
    if (currentRunUuids.isEmpty()) {
      return jobs;
    }

    Map<UUID, Run> runsByUuid =
        createRunDao().findRunsByUuids(currentRunUuids).stream()
            .collect(Collectors.toMap(run -> run.getId().getValue(), run -> run));
    jobs.forEach(
        job ->
            job.getCurrentRunUuid()
                .map(runsByUuid::get)
                .ifPresent(run -> setJobData(List.of(run), job)));
    return jobs;
  }

  default void setJobDataset(List<JobDataset> datasets, Job j) {
    Optional.of(
            datasets.stream()
                .filter(d -> d.ioType().equals(IoType.INPUT))
                .map(
                    ds ->
                        new DatasetId(NamespaceName.of(ds.namespace()), DatasetName.of(ds.name())))
                .collect(Collectors.toSet()))
        .filter(s -> !s.isEmpty())
        .ifPresent(s -> j.setInputs(s));

    Optional.of(
            datasets.stream()
                .filter(d -> d.ioType().equals(IoType.OUTPUT))
                .map(
                    ds ->
                        new DatasetId(NamespaceName.of(ds.namespace()), DatasetName.of(ds.name())))
                .collect(Collectors.toSet()))
        .filter(s -> !s.isEmpty())
        .ifPresent(s -> j.setOutputs(s));
  }

  default void setJobData(List<Run> runs, Job j) {
    if (runs.isEmpty()) {
      return;
    }

    Run latestRun = runs.get(0);
    j.setLatestRun(latestRun);
    j.setLatestRuns(runs);
    j.setInputs(
        latestRun.getInputDatasetVersions().stream()
            .map(input -> input.getDatasetVersionId())
            .map(version -> new DatasetId(version.getNamespace(), version.getName()))
            .collect(Collectors.toSet()));
    j.setOutputs(
        latestRun.getOutputDatasetVersions().stream()
            .map(output -> output.getDatasetVersionId())
            .map(version -> new DatasetId(version.getNamespace(), version.getName()))
            .collect(Collectors.toSet()));
  }

  default JobRow upsertJobMeta(
      NamespaceName namespaceName, JobName jobName, JobMeta jobMeta, ObjectMapper mapper) {
    return upsertJobMeta(namespaceName, jobName, null, jobMeta, mapper);
  }

  default JobRow upsertJobMeta(
      NamespaceName namespaceName,
      JobName jobName,
      UUID symlinkTargetUuid,
      JobMeta jobMeta,
      ObjectMapper mapper) {
    Instant createdAt = Instant.now();
    NamespaceRow namespace =
        createNamespaceDao()
            .upsertNamespaceRow(
                UUID.randomUUID(), createdAt, namespaceName.getValue(), DEFAULT_NAMESPACE_OWNER);
    return upsertJob(
        UUID.randomUUID(),
        jobMeta.getType(),
        createdAt,
        namespace.getUuid(),
        namespace.getName(),
        jobName.getValue(),
        jobMeta.getDescription().orElse(null),
        toUrlString(jobMeta.getLocation().orElse(null)),
        symlinkTargetUuid,
        toJson(jobMeta.getInputs(), mapper),
        jobMeta.getRunId().map(RunId::getValue).orElse(null));
  }

  default String toUrlString(URL url) {
    if (url == null) {
      return null;
    }
    return url.toString();
  }

  default PGobject toJson(Set<DatasetId> dataset, ObjectMapper mapper) {
    try {
      PGobject jsonObject = new PGobject();
      jsonObject.setType("json");
      jsonObject.setValue(mapper.writeValueAsString(dataset));
      return jsonObject;
    } catch (Exception e) {
      return null;
    }
  }

  default JobRow upsertJob(
      UUID uuid,
      JobType type,
      Instant now,
      UUID namespaceUuid,
      String namespaceName,
      String name,
      String description,
      String location,
      UUID symlinkTargetId,
      PGobject inputs) {
    return upsertJob(
        uuid,
        type,
        now,
        namespaceUuid,
        namespaceName,
        name,
        description,
        location,
        symlinkTargetId,
        inputs,
        null);
  }

  /*
   * Note: following SQL never executes. There is database trigger on `jobs_view`
   * that replaces following SQL
   * with rewrite_jobs_fqn_table plpgsql function. Code of that function is at
   * R__1 migration file.
   */
  @SqlQuery(
      """
        INSERT INTO jobs_view AS j (
          uuid,
          type,
          created_at,
          updated_at,
          namespace_uuid,
          namespace_name,
          name,
          description,
          current_location,
          current_inputs,
          symlink_target_uuid,
          parent_job_uuid_string,
          current_run_uuid
        ) VALUES (
          :uuid,
          :type,
          :now,
          :now,
          :namespaceUuid,
          :namespaceName,
          :name,
          :description,
          :location,
          :inputs,
          :symlinkTargetId,
          '',
          :currentRunUuid
        ) RETURNING *
      """)
  JobRow upsertJob(
      UUID uuid,
      JobType type,
      Instant now,
      UUID namespaceUuid,
      String namespaceName,
      String name,
      String description,
      String location,
      UUID symlinkTargetId,
      PGobject inputs,
      UUID currentRunUuid);

  /*
   * Note: following SQL never executes. There is database trigger on `jobs_view`
   * that replaces following SQL
   * with rewrite_jobs_fqn_table plpgsql function. Code of that function is at
   * R__1 migration file.
   */
  @SqlQuery(
      """
        INSERT INTO jobs_view AS j (
          uuid,
          parent_job_uuid,
          type,
          created_at,
          updated_at,
          namespace_uuid,
          namespace_name,
          name,
          description,
          current_location,
          current_inputs,
          symlink_target_uuid,
          current_run_uuid
        ) VALUES (
          :uuid,
          :parentJobUuid,
          :type,
          :now,
          :now,
          :namespaceUuid,
          :namespaceName,
          :name,
          :description,
          :location,
          :inputs,
          :symlinkTargetId,
          :currentRunUuid
        )
        RETURNING *
      """)
  JobRow upsertJob(
      UUID uuid,
      UUID parentJobUuid,
      JobType type,
      Instant now,
      UUID namespaceUuid,
      String namespaceName,
      String name,
      String description,
      String location,
      UUID symlinkTargetId,
      PGobject inputs,
      UUID currentRunUuid);

  @SqlUpdate(
      """
      WITH new_tag AS (
      INSERT INTO tags (uuid, created_at, updated_at, name, description)
      SELECT
        :uuid,
        :now,
        :now,
        :tagName,
        NULL
      WHERE
          NOT EXISTS (SELECT 1 FROM tags WHERE name = :tagName)
      RETURNING uuid
      ),
      existing_tag AS (
          SELECT uuid FROM tags WHERE name = :tagName
      ),
      job AS (
        SELECT
          uuid
        FROM
          jobs
        WHERE
          simple_name = :jobName
        and
          namespace_name = :namespaceName
      )
      INSERT INTO jobs_tag_mapping (job_uuid, tag_uuid, tagged_at)
      SELECT
          (SELECT uuid FROM job)
      ,   COALESCE((SELECT uuid FROM new_tag), (SELECT uuid FROM existing_tag))
      ,   :now
      ON CONFLICT DO NOTHING
      ;
      """)
  void updateJobTagsNow(
      String namespaceName, String jobName, String tagName, Instant now, UUID uuid);

  default void updateJobTags(String namespaceName, String jobName, String tagName) {
    Instant now = Instant.now();
    UUID uuid = UUID.randomUUID();
    updateJobTagsNow(namespaceName, jobName, tagName, now, uuid);
  }

  @SqlUpdate(
      """
      DELETE FROM jobs_tag_mapping jtm
      WHERE EXISTS (
            SELECT 1
            FROM
                jobs j
            JOIN
                tags t
            ON
                j.uuid = jtm.job_uuid
            AND
                t.uuid = jtm.tag_uuid
            WHERE
                t.name = :tagName
            AND
                j.simple_name = :jobName
            AND
                j.namespace_name = :namespaceName
            );
      """)
  void deleteJobTags(String namespaceName, String jobName, String tagName);
}
