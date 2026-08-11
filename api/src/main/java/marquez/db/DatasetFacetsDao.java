/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.Value;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.sqlobject.customizer.BindBeanList;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;
import org.postgresql.util.PGobject;

/** The DAO for {@code dataset} facets. */
public interface DatasetFacetsDao {
  // Each container binds eight values; keep chunks comfortably below PostgreSQL's bind limit.
  int MAX_FACET_CONTAINERS_PER_INSERT = 1000;

  /* An {@code enum} used ... */
  enum Type {
    DATASET,
    INPUT,
    OUTPUT,
    UNKNOWN;
  }

  /* An {@code enum} used to determine the dataset facet. */
  enum DatasetFacet {
    DOCUMENTATION(Type.DATASET, "documentation"),
    DESCRIPTION(Type.DATASET, "description"),
    SCHEMA(Type.DATASET, "schema"),
    DATASOURCE(Type.DATASET, "dataSource"),
    LIFECYCLE_STATE_CHANGE(Type.DATASET, "lifecycleStateChange"),
    VERSION(Type.DATASET, "version"),
    COLUMN_LINEAGE(Type.DATASET, "columnLineage"),
    OWNERSHIP(Type.DATASET, "ownership"),
    DATA_QUALITY_METRICS(Type.INPUT, "dataQualityMetrics"),
    DATA_QUALITY_ASSERTIONS(Type.INPUT, "dataQualityAssertions"),
    OUTPUT_STATISTICS(Type.OUTPUT, "outputStatistics");

    final Type type;
    final String name;

    DatasetFacet(@NonNull final Type type, @NonNull final String name) {
      this.type = type;
      this.name = name;
    }

    Type getType() {
      return type;
    }

    String getName() {
      return name;
    }

    /** ... */
    public static Type typeFromName(@NonNull final String name) {
      return Arrays.stream(DatasetFacet.values())
          .filter(facet -> facet.getName().equalsIgnoreCase(name))
          .map(facet -> facet.getType())
          .findFirst()
          .orElse(Type.UNKNOWN);
    }
  }

  /**
   * @param createdAt
   * @param datasetUuid
   * @param datasetVersionUuid
   * @param runUuid
   * @param lineageEventTime
   * @param lineageEventType
   * @param type
   * @param name
   * @param facet
   */
  @SqlUpdate(
      """
          INSERT INTO dataset_facets (
             created_at,
             dataset_uuid,
             dataset_version_uuid,
             run_uuid,
             lineage_event_time,
             lineage_event_type,
             type,
             name,
             facet
          ) VALUES (
             :createdAt,
             :datasetUuid,
             :datasetVersionUuid,
             :runUuid,
             :lineageEventTime,
             :lineageEventType,
             :type,
             :name,
             :facet
          )
      """)
  void insertDatasetFacet(
      Instant createdAt,
      UUID datasetUuid,
      UUID datasetVersionUuid,
      UUID runUuid,
      Instant lineageEventTime,
      String lineageEventType,
      Type type,
      String name,
      PGobject facet);

  @SqlUpdate(
      """
      WITH facet_containers (
        created_at,
        dataset_uuid,
        dataset_version_uuid,
        run_uuid,
        lineage_event_time,
        lineage_event_type,
        type_override,
        facets
      ) AS (
        VALUES <values>
      )
      INSERT INTO dataset_facets (
        created_at,
        dataset_uuid,
        dataset_version_uuid,
        run_uuid,
        lineage_event_time,
        lineage_event_type,
        type,
        name,
        facet
      )
      SELECT
        CAST(facet_containers.created_at AS timestamptz),
        CAST(facet_containers.dataset_uuid AS uuid),
        CAST(facet_containers.dataset_version_uuid AS uuid),
        CAST(facet_containers.run_uuid AS uuid),
        CAST(facet_containers.lineage_event_time AS timestamptz),
        CAST(facet_containers.lineage_event_type AS varchar),
        COALESCE(
          CAST(facet_containers.type_override AS varchar),
          CASE
            WHEN lower(facet_entry.name) IN (
              'documentation',
              'description',
              'schema',
              'datasource',
              'lifecyclestatechange',
              'version',
              'columnlineage',
              'ownership'
            ) THEN 'DATASET'
            WHEN lower(facet_entry.name) IN (
              'dataqualitymetrics',
              'dataqualityassertions'
            ) THEN 'INPUT'
            WHEN lower(facet_entry.name) = 'outputstatistics' THEN 'OUTPUT'
            ELSE 'UNKNOWN'
          END
        ),
        facet_entry.name,
        jsonb_build_object(facet_entry.name, facet_entry.value)
      FROM facet_containers
      CROSS JOIN LATERAL
        jsonb_each(CAST(facet_containers.facets AS jsonb)) AS facet_entry(name, value)
      """)
  void doInsertDatasetFacetWrites(
      @BindBeanList(
              propertyNames = {
                "createdAt",
                "datasetUuid",
                "datasetVersionUuid",
                "runUuid",
                "lineageEventTime",
                "lineageEventType",
                "typeOverride",
                "facets"
              },
              value = "values")
          List<DatasetFacetWrite> writes);

  /**
   * Flushes serialized facet containers in bounded batches. This is the event-scope API: callers
   * may collect dataset, input, and output containers and write them after their referenced rows
   * have been created.
   */
  @Transaction
  default void insertDatasetFacetWrites(@NonNull List<DatasetFacetWrite> writes) {
    insertDatasetFacetWritesInTransaction(writes);
  }

  /**
   * Flushes serialized facet containers without opening a transaction. Callers must already own the
   * transaction when multiple chunks need to commit atomically.
   */
  default void insertDatasetFacetWritesInTransaction(@NonNull List<DatasetFacetWrite> writes) {
    int nonEmptyWrites = 0;
    for (DatasetFacetWrite write : writes) {
      if (!FacetUtils.isEmpty(write.getFacets())) {
        nonEmptyWrites++;
      }
    }

    if (nonEmptyWrites == 0) {
      return;
    }

    // When no filtering is needed, pass the list (or bounded views of it) directly to Jdbi
    // instead of copying every reference into another pre-sized list.
    if (nonEmptyWrites == writes.size()) {
      for (int start = 0; start < writes.size(); start += MAX_FACET_CONTAINERS_PER_INSERT) {
        int end = Math.min(start + MAX_FACET_CONTAINERS_PER_INSERT, writes.size());
        doInsertDatasetFacetWrites(
            start == 0 && end == writes.size() ? writes : writes.subList(start, end));
      }
      return;
    }

    // Allocate one right-sized filter buffer only when callers supply empty containers, and reuse
    // it after each synchronous Jdbi write.
    List<DatasetFacetWrite> batch =
        new ArrayList<>(Math.min(nonEmptyWrites, MAX_FACET_CONTAINERS_PER_INSERT));
    for (DatasetFacetWrite write : writes) {
      if (FacetUtils.isEmpty(write.getFacets())) {
        continue;
      }
      batch.add(write);
      if (batch.size() == MAX_FACET_CONTAINERS_PER_INSERT) {
        doInsertDatasetFacetWrites(batch);
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      doInsertDatasetFacetWrites(batch);
    }
  }

  /**
   * @param datasetUuid
   * @param runUuid
   * @param lineageEventTime
   * @param lineageEventType
   * @param datasetFacets
   */
  default void insertDatasetFacetsFor(
      @NonNull UUID datasetUuid,
      @NonNull UUID datasetVersionUuid,
      @Nullable UUID runUuid,
      @NonNull Instant lineageEventTime,
      @Nullable String lineageEventType,
      @NonNull LineageEvent.DatasetFacets datasetFacets) {
    insertDatasetFacetWrites(
        List.of(
            DatasetFacetWrite.forDatasetFacets(
                Instant.now(),
                datasetUuid,
                datasetVersionUuid,
                runUuid,
                lineageEventTime,
                lineageEventType,
                datasetFacets)));
  }

  default void insertInputDatasetFacetsFor(
      @NonNull UUID datasetUuid,
      @NonNull UUID datasetVersionUuid,
      @Nullable UUID runUuid,
      @NonNull Instant lineageEventTime,
      @Nullable String lineageEventType,
      @NonNull LineageEvent.InputDatasetFacets inputFacets) {
    insertDatasetFacetWrites(
        List.of(
            DatasetFacetWrite.forInputFacets(
                Instant.now(),
                datasetUuid,
                datasetVersionUuid,
                runUuid,
                lineageEventTime,
                lineageEventType,
                inputFacets)));
  }

  default void insertOutputDatasetFacetsFor(
      @NonNull UUID datasetUuid,
      @NonNull UUID datasetVersionUuid,
      @Nullable UUID runUuid,
      @NonNull Instant lineageEventTime,
      @Nullable String lineageEventType,
      @NonNull LineageEvent.OutputDatasetFacets outputFacets) {
    insertDatasetFacetWrites(
        List.of(
            DatasetFacetWrite.forOutputFacets(
                Instant.now(),
                datasetUuid,
                datasetVersionUuid,
                runUuid,
                lineageEventTime,
                lineageEventType,
                outputFacets)));
  }

  /** A serialized facet container plus the foreign keys shared by all fields in that container. */
  @Value
  class DatasetFacetWrite {
    @NonNull Instant createdAt;
    @NonNull UUID datasetUuid;
    @NonNull UUID datasetVersionUuid;
    @Nullable UUID runUuid;
    @NonNull Instant lineageEventTime;
    @Nullable String lineageEventType;
    @Nullable Type typeOverride;
    @NonNull PGobject facets;

    public static DatasetFacetWrite forDatasetFacets(
        @NonNull Instant createdAt,
        @NonNull UUID datasetUuid,
        @NonNull UUID datasetVersionUuid,
        @Nullable UUID runUuid,
        @NonNull Instant lineageEventTime,
        @Nullable String lineageEventType,
        @NonNull LineageEvent.DatasetFacets facets) {
      return create(
          createdAt,
          datasetUuid,
          datasetVersionUuid,
          runUuid,
          lineageEventTime,
          lineageEventType,
          null,
          facets);
    }

    public static DatasetFacetWrite forInputFacets(
        @NonNull Instant createdAt,
        @NonNull UUID datasetUuid,
        @NonNull UUID datasetVersionUuid,
        @Nullable UUID runUuid,
        @NonNull Instant lineageEventTime,
        @Nullable String lineageEventType,
        @NonNull LineageEvent.InputDatasetFacets facets) {
      return create(
          createdAt,
          datasetUuid,
          datasetVersionUuid,
          runUuid,
          lineageEventTime,
          lineageEventType,
          Type.INPUT,
          facets);
    }

    public static DatasetFacetWrite forOutputFacets(
        @NonNull Instant createdAt,
        @NonNull UUID datasetUuid,
        @NonNull UUID datasetVersionUuid,
        @Nullable UUID runUuid,
        @NonNull Instant lineageEventTime,
        @Nullable String lineageEventType,
        @NonNull LineageEvent.OutputDatasetFacets facets) {
      return create(
          createdAt,
          datasetUuid,
          datasetVersionUuid,
          runUuid,
          lineageEventTime,
          lineageEventType,
          Type.OUTPUT,
          facets);
    }

    private static DatasetFacetWrite create(
        Instant createdAt,
        UUID datasetUuid,
        UUID datasetVersionUuid,
        UUID runUuid,
        Instant lineageEventTime,
        String lineageEventType,
        Type typeOverride,
        Object facets) {
      return new DatasetFacetWrite(
          createdAt,
          datasetUuid,
          datasetVersionUuid,
          runUuid,
          lineageEventTime,
          lineageEventType,
          typeOverride,
          FacetUtils.toPgObject(facets));
    }
  }

  record DatasetFacetRow(
      Instant createdAt,
      UUID datasetUuid,
      UUID datasetVersionUuid,
      UUID runUuid,
      Instant lineageEventTime,
      String lineageEventType,
      DatasetFacetsDao.Type type,
      String name,
      PGobject facet) {}
}
