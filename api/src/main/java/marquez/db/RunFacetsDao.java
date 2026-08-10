/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.time.Instant;
import java.util.UUID;
import lombok.NonNull;
import marquez.db.mappers.RunFacetsMapper;
import marquez.service.models.LineageEvent;
import marquez.service.models.RunFacets;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.postgresql.util.PGobject;

@RegisterRowMapper(RunFacetsMapper.class)
/** The DAO for {@code run} facets. */
public interface RunFacetsDao {
  String SPARK_UNKNOWN = "spark_unknown";
  String SPARK_LOGICAL_PLAN = "spark.logicalPlan";

  /**
   * @param createdAt
   * @param runUuid
   * @param lineageEventTime
   * @param lineageEventType
   * @param name
   * @param facet
   */
  @SqlUpdate(
      """
      INSERT INTO run_facets (
         created_at,
         run_uuid,
         lineage_event_time,
         lineage_event_type,
         name,
         facet
      ) VALUES (
         :createdAt,
         :runUuid,
         :lineageEventTime,
         :lineageEventType,
         :name,
         :facet
      )
      """)
  void insertRunFacet(
      Instant createdAt,
      UUID runUuid,
      Instant lineageEventTime,
      String lineageEventType,
      String name,
      PGobject facet);

  /**
   * Inserts every field in a serialized run-facet container in one statement. The logical-plan
   * existence check intentionally keeps the legacy behavior: identifying the special field is
   * case-insensitive, while matching an already stored facet name is case-sensitive.
   */
  @SqlUpdate(
      """
      INSERT INTO run_facets (
         created_at,
         run_uuid,
         lineage_event_time,
         lineage_event_type,
         name,
         facet
      )
      SELECT
         :createdAt,
         :runUuid,
         :lineageEventTime,
         :lineageEventType,
         facet_entry.name,
         jsonb_build_object(facet_entry.name, facet_entry.value)
      FROM jsonb_each(CAST(:facets AS jsonb)) AS facet_entry(name, value)
      WHERE lower(facet_entry.name) <> lower(:sparkUnknown)
        AND (
          lower(facet_entry.name) <> lower(:sparkLogicalPlan)
          OR NOT EXISTS (
            SELECT 1
            FROM run_facets existing
            WHERE existing.run_uuid = :runUuid
              AND existing.name = facet_entry.name
          )
        )
      """)
  void insertRunFacetContainer(
      Instant createdAt,
      UUID runUuid,
      Instant lineageEventTime,
      String lineageEventType,
      PGobject facets,
      String sparkUnknown,
      String sparkLogicalPlan);

  @SqlQuery("SELECT EXISTS (SELECT 1 FROM run_facets WHERE name = :name AND run_uuid = :runUuid)")
  boolean runFacetExists(String name, UUID runUuid);

  /**
   * @param runUuid
   */
  @SqlQuery(
      """
      SELECT
        run_uuid,
        JSON_AGG(facet ORDER BY lineage_event_time) AS facets
      FROM
      run_facets_view
      WHERE
        run_uuid = :runUuid
      GROUP BY
        run_uuid
    """)
  RunFacets findRunFacetsByRunUuid(UUID runUuid);

  /**
   * @param runUuid
   * @param lineageEventTime
   * @param lineageEventType
   * @param runFacet
   */
  default void insertRunFacetsFor(
      @NonNull UUID runUuid,
      @NonNull Instant lineageEventTime,
      @NonNull String lineageEventType,
      @NonNull LineageEvent.RunFacet runFacet) {
    PGobject facets = FacetUtils.toPgObject(runFacet);
    if (FacetUtils.isEmpty(facets)) {
      return;
    }
    insertRunFacetContainer(
        Instant.now(),
        runUuid,
        lineageEventTime,
        lineageEventType,
        facets,
        SPARK_UNKNOWN,
        SPARK_LOGICAL_PLAN);
  }

  record RunFacetRow(
      Instant createdAt,
      UUID runUuid,
      Instant lineageEventTime,
      String lineageEventType,
      String name,
      PGobject facet) {}
}
