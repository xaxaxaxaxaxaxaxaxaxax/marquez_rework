/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.LineageTestUtils.PRODUCER_URL;
import static marquez.db.LineageTestUtils.SCHEMA_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import marquez.api.JdbiUtils;
import marquez.db.DatasetFacetsDao.DatasetFacetWrite;
import marquez.db.models.UpdateLineageRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.Dataset;
import marquez.service.models.LineageEvent.JobFacet;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.transaction.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.postgresql.util.PGobject;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
public class DatasetFacetsDaoTest {

  private static DatasetFacetsDao datasetFacetsDao;

  private static OpenLineageDao openLineageDao;

  private Jdbi jdbi;

  private Instant lineageEventTime = Instant.now();

  @BeforeAll
  public static void setUpOnce(Jdbi jdbi) {
    datasetFacetsDao = jdbi.onDemand(DatasetFacetsDao.class);
    openLineageDao = jdbi.onDemand(OpenLineageDao.class);
  }

  @BeforeEach
  public void setup(Jdbi jdbi) {
    this.jdbi = jdbi;
  }

  @AfterEach
  public void tearDown(Jdbi jdbi) {
    JdbiUtils.cleanDatabase(jdbi);
  }

  @Test
  public void testInsertDatasetFacetsForDocumentationFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder()
                .documentation(
                    new LineageEvent.DocumentationDatasetFacet(
                        PRODUCER_URL, SCHEMA_URL, "some-doc")));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "documentation");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue(
            "datasetVersionUuid",
            lineageRow.getInputs().get().get(0).getDatasetVersionRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.DATASET);

    assertThat(facet.facet().toString())
        .isEqualTo(
            "{\"documentation\": {\"_producer\": \"http://test.producer/\", "
                + "\"_schemaURL\": \"http://test.schema/\", \"description\": \"some-doc\"}}");
  }

  @Test
  public void testInsertDatasetFacetsForSchemaFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder()
                .schema(
                    new LineageEvent.SchemaDatasetFacet(
                        PRODUCER_URL, SCHEMA_URL, Collections.emptyList())));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "schema");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.DATASET);

    assertThat(facet.facet().toString())
        .isEqualTo(
            "{\"schema\": {\"fields\": [], \"_producer\": \"http://test.producer/\", "
                + "\"_schemaURL\": \"http://test.schema/\"}}");
  }

  @Test
  public void testInsertDatasetFacetsForDatasourceFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder()
                .dataSource(
                    new LineageEvent.DatasourceDatasetFacet(
                        PRODUCER_URL, SCHEMA_URL, "the source", "http://thesource.com")));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "dataSource");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.DATASET);

    assertThat(facet.facet().toString())
        .isEqualTo(
            "{\"dataSource\": {\"uri\": \"http://thesource.com\", \"name\": \"the source\", "
                + "\"_producer\": \"http://test.producer/\", \"_schemaURL\": \"http://test.schema/\"}}");
  }

  @Test
  public void testInsertDatasetFacetsForDescriptionFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder().description("some-description"));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "description");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.DATASET);

    assertThat(facet.facet().toString()).isEqualTo("{\"description\": \"some-description\"}");
  }

  @Test
  public void testInsertDatasetFacetsForLifecycleStateChangeFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder()
                .lifecycleStateChange(new LineageEvent.LifecycleStateChangeFacet()));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "lifecycleStateChange");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.DATASET);

    assertThat(facet.facet().toString()).isEqualTo("{\"lifecycleStateChange\": {}}");
  }

  @Test
  public void testInsertDatasetFacetsForVersionFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder().additional(Map.of("version", "some-version")));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "version");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.DATASET);

    assertThat(facet.facet().toString()).isEqualTo("{\"version\": \"some-version\"}");
  }

  @Test
  public void testInsertDatasetFacetsForColumnLineageFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder()
                .columnLineage(new LineageEvent.ColumnLineageDatasetFacet()));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "columnLineage");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.DATASET);

    assertThat(facet.facet().toString()).isEqualTo("{\"columnLineage\": {}}");
  }

  @Test
  public void testInsertDatasetFacetsForOwnershipFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder().additional(Map.of("ownership", "some-owner")));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "ownership");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.DATASET);

    assertThat(facet.facet().toString()).isEqualTo("{\"ownership\": \"some-owner\"}");
  }

  @Test
  public void testInsertDatasetFacetsForDataQualityMetricsFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder().additional(Map.of("dataQualityMetrics", "m1")));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "dataQualityMetrics");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.INPUT);

    assertThat(facet.facet().toString()).isEqualTo("{\"dataQualityMetrics\": \"m1\"}");
  }

  @Test
  public void testInsertDatasetFacetsForDataQualityAssertionsFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder().additional(Map.of("dataQualityAssertions", "m2")));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "dataQualityAssertions");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getInputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.INPUT);

    assertThat(facet.facet().toString()).isEqualTo("{\"dataQualityAssertions\": \"m2\"}");
  }

  @Test
  public void testInsertDatasetFacetsForOutputStatisticsFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithOutputDataset(
            LineageEvent.DatasetFacets.builder().additional(Map.of("outputStatistics", "m3")));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "outputStatistics");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getOutputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.OUTPUT);

    assertThat(facet.facet().toString()).isEqualTo("{\"outputStatistics\": \"m3\"}");
  }

  @Test
  public void testInsertDatasetFacetsForUnknownTypeFacet() {
    UpdateLineageRow lineageRow =
        createLineageRowWithOutputDataset(
            LineageEvent.DatasetFacets.builder().additional(Map.of("custom-output", "{whatever}")));

    DatasetFacetsDao.DatasetFacetRow facet = getDatasetFacet(lineageRow, "custom-output");

    assertThat(facet)
        .hasFieldOrPropertyWithValue(
            "datasetUuid", lineageRow.getOutputs().get().get(0).getDatasetRow().getUuid())
        .hasFieldOrPropertyWithValue("runUuid", lineageRow.getRun().getUuid())
        .hasFieldOrPropertyWithValue("lineageEventTime", lineageRow.getRun().getCreatedAt())
        .hasFieldOrPropertyWithValue("lineageEventType", "COMPLETE")
        .hasFieldOrPropertyWithValue("type", DatasetFacetsDao.Type.UNKNOWN);

    assertThat(facet.facet().toString()).isEqualTo("{\"custom-output\": \"{whatever}\"}");
  }

  @Test
  public void testInsertOutputDatasetFacetsFor() {
    LineageEvent.JobFacet jobFacet = JobFacet.builder().build();

    UpdateLineageRow lineageRow =
        LineageTestUtils.createLineageRow(
            openLineageDao,
            "job_" + UUID.randomUUID(),
            "COMPLETE",
            jobFacet,
            Collections.emptyList(),
            Arrays.asList(
                new Dataset(
                    "namespace",
                    "dataset_output",
                    null,
                    null,
                    LineageEvent.OutputDatasetFacets.builder()
                        .additional(
                            ImmutableMap.of(
                                "outputFacet1", "{some-facet1}",
                                "outputFacet2", "{some-facet2}"))
                        .build())),
            null);

    assertThat(getDatasetFacet(lineageRow, "outputFacet1").facet().toString())
        .isEqualTo("{\"outputFacet1\": \"{some-facet1}\"}");
    assertThat(getDatasetFacet(lineageRow, "outputFacet2").facet().toString())
        .isEqualTo("{\"outputFacet2\": \"{some-facet2}\"}");
  }

  @Test
  public void testInsertInputDatasetFacetsFor() {
    LineageEvent.JobFacet jobFacet = JobFacet.builder().build();

    UpdateLineageRow lineageRow =
        LineageTestUtils.createLineageRow(
            openLineageDao,
            "job_" + UUID.randomUUID(),
            "COMPLETE",
            jobFacet,
            Arrays.asList(
                new Dataset(
                    "namespace",
                    "dataset_output",
                    null,
                    LineageEvent.InputDatasetFacets.builder()
                        .additional(
                            ImmutableMap.of(
                                "inputFacet1", "{some-facet1}",
                                "inputFacet2", "{some-facet2}"))
                        .build(),
                    null)),
            Collections.emptyList(),
            null);

    assertThat(getDatasetFacet(lineageRow, "inputFacet1").facet().toString())
        .isEqualTo("{\"inputFacet1\": \"{some-facet1}\"}");
    assertThat(getDatasetFacet(lineageRow, "inputFacet2").facet().toString())
        .isEqualTo("{\"inputFacet2\": \"{some-facet2}\"}");
  }

  @Test
  public void testInsertDatasetFacetWritesCombinesContainerKinds() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder().description("existing-description"));
    UpdateLineageRow.DatasetRecord datasetRecord = lineageRow.getInputs().orElseThrow().get(0);
    Instant createdAt = Instant.now();

    datasetFacetsDao.insertDatasetFacetWrites(
        List.of(
            DatasetFacetWrite.forDatasetFacets(
                createdAt,
                datasetRecord.getDatasetRow().getUuid(),
                datasetRecord.getDatasetVersionRow().getUuid(),
                lineageRow.getRun().getUuid(),
                lineageRow.getRun().getCreatedAt(),
                "COMPLETE",
                LineageEvent.DatasetFacets.builder()
                    .additional(Map.of("bulk-dataset", Map.of("nested", true)))
                    .build()),
            DatasetFacetWrite.forInputFacets(
                createdAt,
                datasetRecord.getDatasetRow().getUuid(),
                datasetRecord.getDatasetVersionRow().getUuid(),
                lineageRow.getRun().getUuid(),
                lineageRow.getRun().getCreatedAt(),
                "COMPLETE",
                LineageEvent.InputDatasetFacets.builder()
                    .additional(Map.of("documentation", "input-override"))
                    .build())));

    assertThat(getDatasetFacet(lineageRow, "bulk-dataset").type())
        .isEqualTo(DatasetFacetsDao.Type.UNKNOWN);
    DatasetFacetsDao.DatasetFacetRow inputOverride = getDatasetFacet(lineageRow, "documentation");
    assertThat(inputOverride.type()).isEqualTo(DatasetFacetsDao.Type.INPUT);
    assertThat(inputOverride.facet().toString())
        .isEqualTo("{\"documentation\": \"input-override\"}");
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testInsertDatasetFacetWritesUsesBoundedContainerBatches() {
    DatasetFacetsDao batchingDao = mock(DatasetFacetsDao.class, CALLS_REAL_METHODS);
    DatasetFacetWrite write =
        DatasetFacetWrite.forDatasetFacets(
            Instant.now(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            null,
            lineageEventTime,
            null,
            LineageEvent.DatasetFacets.builder().description("description").build());

    batchingDao.insertDatasetFacetWrites(
        Collections.nCopies(DatasetFacetsDao.MAX_FACET_CONTAINERS_PER_INSERT + 1, write));

    ArgumentCaptor<List<DatasetFacetWrite>> batches = ArgumentCaptor.forClass(List.class);
    verify(batchingDao, times(2)).doInsertDatasetFacetWrites(batches.capture());
    assertThat(batches.getAllValues())
        .extracting(List::size)
        .containsExactly(DatasetFacetsDao.MAX_FACET_CONTAINERS_PER_INSERT, 1);
  }

  @Test
  public void testInsertDatasetFacetWritesInTransactionDoesNotCopySingleNonEmptyBatch() {
    DatasetFacetsDao batchingDao = mock(DatasetFacetsDao.class, CALLS_REAL_METHODS);
    List<DatasetFacetWrite> writes = new ArrayList<>();
    writes.add(nonEmptyFacetWrite(UUID.randomUUID(), UUID.randomUUID(), "description"));

    batchingDao.insertDatasetFacetWritesInTransaction(writes);

    verify(batchingDao).doInsertDatasetFacetWrites(same(writes));
  }

  @Test
  public void testInsertDatasetFacetWritesInTransactionFiltersBeforeChunking() {
    DatasetFacetsDao batchingDao = mock(DatasetFacetsDao.class, CALLS_REAL_METHODS);
    DatasetFacetWrite nonEmptyWrite =
        nonEmptyFacetWrite(UUID.randomUUID(), UUID.randomUUID(), "description");
    DatasetFacetWrite emptyWrite =
        DatasetFacetWrite.forDatasetFacets(
            Instant.now(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            null,
            lineageEventTime,
            null,
            LineageEvent.DatasetFacets.builder().build());
    List<DatasetFacetWrite> writes =
        new ArrayList<>(DatasetFacetsDao.MAX_FACET_CONTAINERS_PER_INSERT + 2);
    writes.add(emptyWrite);
    writes.addAll(
        Collections.nCopies(DatasetFacetsDao.MAX_FACET_CONTAINERS_PER_INSERT + 1, nonEmptyWrite));
    List<Integer> batchSizes = new ArrayList<>();
    doAnswer(
            invocation -> {
              List<DatasetFacetWrite> batch = invocation.getArgument(0);
              batchSizes.add(batch.size());
              assertThat(batch)
                  .allSatisfy(write -> assertThat(FacetUtils.isEmpty(write.getFacets())).isFalse());
              return null;
            })
        .when(batchingDao)
        .doInsertDatasetFacetWrites(anyList());

    batchingDao.insertDatasetFacetWritesInTransaction(writes);

    verify(batchingDao, times(2)).doInsertDatasetFacetWrites(anyList());
    assertThat(batchSizes).containsExactly(DatasetFacetsDao.MAX_FACET_CONTAINERS_PER_INSERT, 1);
  }

  @Test
  public void testInsertDatasetFacetWritesKeepsAllChunksAtomic() {
    UpdateLineageRow lineageRow =
        createLineageRowWithInputDataset(
            LineageEvent.DatasetFacets.builder().description("existing-description"));
    UpdateLineageRow.DatasetRecord datasetRecord = lineageRow.getInputs().orElseThrow().get(0);
    String facetName = "rollback-" + UUID.randomUUID();
    DatasetFacetWrite validWrite =
        nonEmptyFacetWrite(
            datasetRecord.getDatasetRow().getUuid(),
            datasetRecord.getDatasetVersionRow().getUuid(),
            facetName);
    DatasetFacetWrite invalidWrite =
        nonEmptyFacetWrite(
            UUID.randomUUID(), datasetRecord.getDatasetVersionRow().getUuid(), facetName);
    List<DatasetFacetWrite> writes =
        new ArrayList<>(DatasetFacetsDao.MAX_FACET_CONTAINERS_PER_INSERT + 1);
    writes.addAll(
        Collections.nCopies(DatasetFacetsDao.MAX_FACET_CONTAINERS_PER_INSERT, validWrite));
    writes.add(invalidWrite);

    assertThatThrownBy(() -> datasetFacetsDao.insertDatasetFacetWrites(writes))
        .isInstanceOf(RuntimeException.class);

    long insertedRows =
        jdbi.withHandle(
            h ->
                h.createQuery("SELECT count(*) FROM dataset_facets WHERE name = :facetName")
                    .bind("facetName", facetName)
                    .mapTo(Long.class)
                    .one());
    assertThat(insertedRows).isZero();
  }

  @Test
  public void testOnlyPublicWrapperOpensTransaction() throws NoSuchMethodException {
    assertThat(
            DatasetFacetsDao.class
                .getMethod("insertDatasetFacetWrites", List.class)
                .getAnnotation(Transaction.class))
        .isNotNull();
    assertThat(
            DatasetFacetsDao.class
                .getMethod("insertDatasetFacetWritesInTransaction", List.class)
                .getAnnotation(Transaction.class))
        .isNull();
  }

  private DatasetFacetWrite nonEmptyFacetWrite(
      UUID datasetUuid, UUID datasetVersionUuid, String facetName) {
    return DatasetFacetWrite.forDatasetFacets(
        Instant.now(),
        datasetUuid,
        datasetVersionUuid,
        null,
        lineageEventTime,
        null,
        LineageEvent.DatasetFacets.builder().additional(Map.of(facetName, "facet-value")).build());
  }

  private UpdateLineageRow createLineageRowWithInputDataset(
      LineageEvent.DatasetFacets.DatasetFacetsBuilder inputDatasetFacetsbuilder) {
    LineageEvent.JobFacet jobFacet = JobFacet.builder().build();

    return LineageTestUtils.createLineageRow(
        openLineageDao,
        "job_" + UUID.randomUUID(),
        "COMPLETE",
        jobFacet,
        Arrays.asList(
            new LineageEvent.Dataset(
                "namespace", "dataset_input", inputDatasetFacetsbuilder.build())),
        Collections.emptyList(),
        null);
  }

  private UpdateLineageRow createLineageRowWithOutputDataset(
      LineageEvent.DatasetFacets.DatasetFacetsBuilder outputDatasetFacetsbuilder) {
    LineageEvent.JobFacet jobFacet = JobFacet.builder().build();

    return LineageTestUtils.createLineageRow(
        openLineageDao,
        "job_" + UUID.randomUUID(),
        "COMPLETE",
        jobFacet,
        Collections.emptyList(),
        Arrays.asList(
            new LineageEvent.Dataset(
                "namespace", "dataset_output", outputDatasetFacetsbuilder.build())),
        null);
  }

  private DatasetFacetsDao.DatasetFacetRow getDatasetFacet(
      UpdateLineageRow lineageRow, String facetName) {
    return jdbi.withHandle(
        h ->
            h.createQuery("SELECT * FROM dataset_facets WHERE name = :facetName")
                .bind("facetName", facetName)
                .map(
                    rv ->
                        new DatasetFacetsDao.DatasetFacetRow(
                            rv.getColumn("created_at", Instant.class),
                            rv.getColumn("dataset_uuid", UUID.class),
                            rv.getColumn("dataset_version_uuid", UUID.class),
                            rv.getColumn("run_uuid", UUID.class),
                            rv.getColumn("lineage_event_time", Instant.class),
                            rv.getColumn("lineage_event_type", String.class),
                            rv.getColumn("type", DatasetFacetsDao.Type.class),
                            rv.getColumn("name", String.class),
                            rv.getColumn("facet", PGobject.class)))
                .one());
  }
}
