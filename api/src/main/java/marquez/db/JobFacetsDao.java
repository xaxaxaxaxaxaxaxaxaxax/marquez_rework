/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.time.Instant;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.NonNull;
import marquez.db.mappers.JobFacetsMapper;
import marquez.service.models.JobFacets;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.postgresql.util.PGobject;

@RegisterRowMapper(JobFacetsMapper.class)
/** The DAO for {@code job} facets. */
public interface JobFacetsDao {

  @SqlUpdate(
      """
            INSERT INTO job_facets (
               created_at,
               job_uuid,
               run_uuid,
               lineage_event_time,
               lineage_event_type,
               name,
               facet
            ) VALUES (
               :createdAt,
               :jobUuid,
               :runUuid,
               :lineageEventTime,
               :lineageEventType,
               :name,
               :facet
            )
            """)
  void insertJobFacet(
      Instant createdAt,
      UUID jobUuid,
      UUID runUuid,
      Instant lineageEventTime,
      String lineageEventType,
      String name,
      PGobject facet);

  @SqlUpdate(
      """
      INSERT INTO job_facets (
         created_at,
         job_uuid,
         run_uuid,
         lineage_event_time,
         lineage_event_type,
         name,
         facet
      )
      SELECT
         :createdAt,
         :jobUuid,
         :runUuid,
         :lineageEventTime,
         :lineageEventType,
         facet_entry.name,
         jsonb_build_object(facet_entry.name, facet_entry.value)
      FROM jsonb_each(CAST(:facets AS jsonb)) AS facet_entry(name, value)
      """)
  void insertJobFacetContainerForRun(
      Instant createdAt,
      UUID jobUuid,
      @Nullable UUID runUuid,
      Instant lineageEventTime,
      @Nullable String lineageEventType,
      PGobject facets);

  @SqlUpdate(
      """
            INSERT INTO job_facets (
               created_at,
               job_uuid,
               job_version_uuid,
               lineage_event_time,
               name,
               facet
            ) VALUES (
               :createdAt,
               :jobUuid,
               :jobVersionUuid,
               :lineageEventTime,
               :name,
               :facet
            )
            """)
  void insertJobFacet(
      Instant createdAt,
      UUID jobUuid,
      UUID jobVersionUuid,
      Instant lineageEventTime,
      String name,
      PGobject facet);

  @SqlUpdate(
      """
      INSERT INTO job_facets (
         created_at,
         job_uuid,
         job_version_uuid,
         lineage_event_time,
         name,
         facet
      )
      SELECT
         :createdAt,
         :jobUuid,
         :jobVersionUuid,
         :lineageEventTime,
         facet_entry.name,
         jsonb_build_object(facet_entry.name, facet_entry.value)
      FROM jsonb_each(CAST(:facets AS jsonb)) AS facet_entry(name, value)
      """)
  void insertJobFacetContainerForVersion(
      Instant createdAt,
      UUID jobUuid,
      UUID jobVersionUuid,
      Instant lineageEventTime,
      PGobject facets);

  @SqlQuery(
      """
            SELECT
                run_uuid,
                JSON_AGG(facet ORDER BY lineage_event_time) AS facets
            FROM
            job_facets_view
            WHERE
                run_uuid = :runUuid
            GROUP BY
                run_uuid
            """)
  JobFacets findJobFacetsByRunUuid(UUID runUuid);

  default void insertJobFacetsFor(
      @NonNull UUID jobUuid,
      @NonNull UUID jobVersionUuid,
      @NonNull Instant lineageEventTime,
      @NonNull LineageEvent.JobFacet jobFacet) {
    PGobject facets = FacetUtils.toPgObject(jobFacet);
    if (FacetUtils.isEmpty(facets)) {
      return;
    }
    insertJobFacetContainerForVersion(
        Instant.now(), jobUuid, jobVersionUuid, lineageEventTime, facets);
  }

  default void insertJobFacetsFor(
      @NonNull UUID jobUuid,
      @Nullable UUID runUuid,
      @NonNull Instant lineageEventTime,
      @Nullable String lineageEventType,
      @NonNull LineageEvent.JobFacet jobFacet) {
    PGobject facets = FacetUtils.toPgObject(jobFacet);
    if (FacetUtils.isEmpty(facets)) {
      return;
    }
    insertJobFacetContainerForRun(
        Instant.now(), jobUuid, runUuid, lineageEventTime, lineageEventType, facets);
  }

  record JobFacetRow(
      Instant createdAt,
      UUID jobUuid,
      UUID runUuid,
      Instant lineageEventTime,
      String lineageEventType,
      String name,
      PGobject facet) {}
}
