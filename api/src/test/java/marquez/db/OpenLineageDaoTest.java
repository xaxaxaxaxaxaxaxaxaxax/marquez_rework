/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.DbTestUtils.createJobWithSymlinkTarget;
import static marquez.db.DbTestUtils.createJobWithoutSymlinkTarget;
import static marquez.db.LineageTestUtils.NAMESPACE;
import static marquez.db.LineageTestUtils.PRODUCER_URL;
import static marquez.db.LineageTestUtils.SCHEMA_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetVersionId;
import marquez.common.models.NamespaceName;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.UpdateLineageRow;
import marquez.db.models.UpdateLineageRow.DatasetRecord;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.Dataset;
import marquez.service.models.LineageEvent.DatasetFacets;
import marquez.service.models.LineageEvent.DocumentationJobFacet;
import marquez.service.models.LineageEvent.Job;
import marquez.service.models.LineageEvent.JobFacet;
import marquez.service.models.LineageEvent.SchemaDatasetFacet;
import marquez.service.models.LineageEvent.SchemaField;
import marquez.service.models.Run;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.groups.Tuple;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.SqlStatements;
import org.jdbi.v3.core.statement.StatementContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class OpenLineageDaoTest {

  public static final String WRITE_JOB_NAME = "writeJobName";
  public static final String READ_JOB_NAME = "readJobName";
  public static final String DATASET_NAME = "theDataset";

  public static final String OUTPUT_COLUMN = "output_column";
  public static final String INPUT_NAMESPACE = "input_namespace";
  public static final String INPUT_DATASET = "input_dataset";
  public static final String INPUT_FIELD_NAME = "input_field_name";
  public static final String TRANSFORMATION_TYPE = "transformation_type";
  public static final String TRANSFORMATION_DESCRIPTION = "transformation_description";

  private static OpenLineageDao dao;
  private static DatasetSymlinkDao symlinkDao;
  private static NamespaceDao namespaceDao;
  private static DatasetFieldDao datasetFieldDao;
  private static DatasetDao datasetDao;
  private static ColumnLineageDao columnLineageDao;
  private static JobDao jobDao;
  private static RunDao runDao;
  private static Jdbi jdbi;
  private final DatasetFacets datasetFacets =
      LineageTestUtils.newDatasetFacet(
          new SchemaField("name", "STRING", "my name"), new SchemaField("age", "INT", "my age"));

  @BeforeAll
  public static void setUpOnce(Jdbi configuredJdbi) {
    jdbi = configuredJdbi;
    dao = jdbi.onDemand(OpenLineageDao.class);
    symlinkDao = jdbi.onDemand(DatasetSymlinkDao.class);
    namespaceDao = jdbi.onDemand(NamespaceDao.class);
    datasetFieldDao = jdbi.onDemand(DatasetFieldDao.class);
    datasetDao = jdbi.onDemand(DatasetDao.class);
    columnLineageDao = jdbi.onDemand(ColumnLineageDao.class);
    jobDao = jdbi.onDemand(JobDao.class);
    runDao = jdbi.onDemand(RunDao.class);
  }

  /** When reading a dataset, the version is assumed to be the version last written */
  @Test
  void testUpdateMarquezModel() {
    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(),
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)));

    UpdateLineageRow readJob =
        LineageTestUtils.createLineageRow(
            dao,
            READ_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)),
            Arrays.asList());

    assertThat(writeJob.getJob().getLocation()).isNull();
    assertThat(writeJob.getOutputs()).isPresent().get().asList().size().isEqualTo(1);
    assertThat(readJob.getInputs()).isPresent().get().asList().size().isEqualTo(1);
    assertThat(readJob.getInputs().get().get(0).getDatasetVersionRow())
        .isEqualTo(writeJob.getOutputs().get().get(0).getDatasetVersionRow());
    assertThat(writeJob.getRunIoSnapshot()).isNotNull();
    assertThat(writeJob.getRunIoSnapshot().getInputs()).isEmpty();
    assertThat(writeJob.getRunIoSnapshot().getOutputs()).hasSize(1);

    // ensure schema version has the right field associations
    UUID schemaVersionUuid =
        writeJob
            .getOutputs()
            .get()
            .get(0)
            .getDatasetVersionRow()
            .getSchemaVersionUuid()
            .orElseThrow();
    assertThat(datasetFieldDao.findByDatasetSchemaVersion(schemaVersionUuid))
        .extracting((ds) -> ds.getName().getValue())
        .containsExactlyInAnyOrder("name", "age");
  }

  @Test
  void batchesDistinctDatasetFieldsAndMappingsOncePerSide() {
    String suffix = UUID.randomUUID().toString();
    String datasetNamespace = "side_batch_" + suffix;
    Dataset existingInput = new Dataset(datasetNamespace, "input_a", datasetFacets);
    LineageTestUtils.createLineageRow(dao, existingInput);

    List<String> executedSql = new ArrayList<>();
    UpdateLineageRow[] projected = new UpdateLineageRow[1];
    jdbi.useHandle(
        handle -> {
          handle
              .getConfig(SqlStatements.class)
              .setSqlLogger(
                  new SqlLogger() {
                    @Override
                    public void logAfterExecution(StatementContext context) {
                      executedSql.add(context.getRawSql());
                    }
                  });
          OpenLineageDao attachedDao = handle.attach(OpenLineageDao.class);
          projected[0] =
              LineageTestUtils.createLineageRow(
                  attachedDao,
                  "side_batch_job_" + suffix,
                  "COMPLETE",
                  JobFacet.builder().build(),
                  List.of(existingInput, new Dataset(datasetNamespace, "input_b", datasetFacets)),
                  List.of(
                      new Dataset(datasetNamespace, "output_a", datasetFacets),
                      new Dataset(datasetNamespace, "output_b", datasetFacets)));
        });

    assertThat(projected[0].getInputs().orElseThrow())
        .extracting(record -> record.getDatasetRow().getName())
        .containsExactly("input_a", "input_b");
    assertThat(projected[0].getOutputs().orElseThrow())
        .extracting(record -> record.getDatasetRow().getName())
        .containsExactly("output_a", "output_b");
    assertThat(
            executedSql.stream().filter(sql -> sql.contains("INSERT INTO dataset_fields")).count())
        .isEqualTo(2);
    assertThat(
            executedSql.stream()
                .filter(sql -> sql.contains("INSERT INTO dataset_versions_field_mapping"))
                .count())
        .isEqualTo(2);
    assertThat(executedSql.stream().filter(sql -> sql.contains("WHERE dv.uuid = ANY")).count())
        .isEqualTo(1);
    assertThat(
            executedSql.stream()
                .filter(sql -> sql.contains("INSERT INTO runs_input_mapping"))
                .count())
        .isEqualTo(1);
  }

  @Test
  void ordersInputMappingResolutionOutputMappingAndPhysicalColumnLineageFlush() {
    UUID runUuid = UUID.randomUUID();
    String suffix = runUuid.toString();
    String datasetNamespace = "column_order_" + suffix;
    String inputName = "input_" + suffix;
    String outputName = "output_" + suffix;
    Dataset input =
        new Dataset(
            datasetNamespace,
            inputName,
            LineageTestUtils.newDatasetFacet(new SchemaField(INPUT_FIELD_NAME, "STRING", "input")));
    Dataset output =
        new Dataset(
            datasetNamespace,
            outputName,
            DatasetFacets.builder()
                .schema(
                    new SchemaDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        List.of(new SchemaField(OUTPUT_COLUMN, "STRING", "output"))))
                .columnLineage(
                    new LineageEvent.ColumnLineageDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        new LineageEvent.ColumnLineageDatasetFacetFields(
                            Collections.singletonMap(
                                OUTPUT_COLUMN,
                                new LineageEvent.ColumnLineageOutputColumn(
                                    List.of(
                                        new LineageEvent.ColumnLineageInputField(
                                            datasetNamespace, inputName, INPUT_FIELD_NAME)),
                                    TRANSFORMATION_DESCRIPTION,
                                    TRANSFORMATION_TYPE)))))
                .build());

    List<String> executedSql = new ArrayList<>();
    jdbi.useHandle(
        handle -> {
          handle
              .getConfig(SqlStatements.class)
              .setSqlLogger(
                  new SqlLogger() {
                    @Override
                    public void logAfterExecution(StatementContext context) {
                      executedSql.add(context.getRawSql());
                    }
                  });
          OpenLineageDao attachedDao = handle.attach(OpenLineageDao.class);
          LineageTestUtils.createLineageRow(
              attachedDao,
              "column_order_job_" + suffix,
              runUuid,
              "COMPLETE",
              JobFacet.builder().build(),
              List.of(input),
              List.of(output),
              null,
              ImmutableMap.of());
        });

    assertThat(
            executedSql.stream()
                .filter(
                    sql ->
                        sql.contains("FROM dataset_fields")
                            && sql.contains("JOIN runs_input_mapping"))
                .count())
        .isEqualTo(1);
    assertThat(
            executedSql.stream().filter(sql -> sql.contains("INSERT INTO column_lineage")).count())
        .isEqualTo(1);

    int inputMapping = findSqlIndexAfter(executedSql, -1, "INSERT INTO runs_input_mapping");
    int inputFieldResolution =
        findSqlIndexAfter(
            executedSql, inputMapping, "FROM dataset_fields", "JOIN runs_input_mapping");
    int outputFieldMapping =
        findSqlIndexAfter(
            executedSql, inputFieldResolution, "INSERT INTO dataset_versions_field_mapping");
    int physicalColumnLineage =
        findSqlIndexAfter(executedSql, outputFieldMapping, "INSERT INTO column_lineage");

    assertThat(inputMapping).isLessThan(inputFieldResolution);
    assertThat(inputFieldResolution).isLessThan(outputFieldMapping);
    assertThat(outputFieldMapping).isLessThan(physicalColumnLineage);
  }

  @Test
  void repeatedDatasetOnOneSidePreservesOccurrenceSemantics() {
    String suffix = UUID.randomUUID().toString();
    Dataset repeated = new Dataset("repeated_side_" + suffix, "input", datasetFacets);

    UpdateLineageRow projected =
        LineageTestUtils.createLineageRow(
            dao,
            "repeated_side_job_" + suffix,
            "COMPLETE",
            JobFacet.builder().build(),
            List.of(repeated, repeated),
            Collections.emptyList());

    assertThat(projected.getInputs().orElseThrow()).hasSize(2);
    assertThat(projected.getInputs().orElseThrow())
        .extracting(record -> record.getDatasetRow().getUuid())
        .containsExactly(
            projected.getInputs().orElseThrow().get(0).getDatasetRow().getUuid(),
            projected.getInputs().orElseThrow().get(0).getDatasetRow().getUuid());
    assertThat(projected.getInputs().orElseThrow())
        .extracting(record -> record.getDatasetVersionRow().getUuid())
        .containsExactly(
            projected.getInputs().orElseThrow().get(0).getDatasetVersionRow().getUuid(),
            projected.getInputs().orElseThrow().get(0).getDatasetVersionRow().getUuid());
  }

  @Test
  void testUpdateMarquezModelWithDatasetEvent() {
    UpdateLineageRow datasetEventRow =
        LineageTestUtils.createLineageRow(dao, new Dataset(NAMESPACE, DATASET_NAME, datasetFacets));

    assertThat(datasetEventRow.getOutputs()).isPresent();
    assertThat(datasetEventRow.getOutputs().get()).hasSize(1).first();
    assertThat(datasetEventRow.getOutputs().get().get(0).getDatasetRow())
        .hasFieldOrPropertyWithValue("name", DATASET_NAME)
        .hasFieldOrPropertyWithValue("namespaceName", NAMESPACE);

    assertThat(datasetEventRow.getOutputs().get().get(0).getDatasetVersionRow())
        .hasNoNullFieldsOrPropertiesExcept("runUuid");
  }

  @Test
  void testUpdateMarquezModelWithJobEvent() {
    JobFacet jobFacet =
        JobFacet.builder()
            .documentation(DocumentationJobFacet.builder().description("documentation").build())
            .build();

    Job job = new Job(NAMESPACE, READ_JOB_NAME, jobFacet);

    UpdateLineageRow jobEventRow =
        LineageTestUtils.createLineageRow(
            dao,
            job,
            Arrays.asList(
                new LineageEvent.Dataset(
                    "namespace",
                    "dataset_input",
                    LineageEvent.DatasetFacets.builder()
                        .schema(
                            new LineageEvent.SchemaDatasetFacet(
                                PRODUCER_URL, SCHEMA_URL, Collections.emptyList()))
                        .build())),
            Arrays.asList(
                new LineageEvent.Dataset(
                    "namespace",
                    "dataset_output",
                    LineageEvent.DatasetFacets.builder()
                        .schema(
                            new LineageEvent.SchemaDatasetFacet(
                                PRODUCER_URL, SCHEMA_URL, Collections.emptyList()))
                        .lifecycleStateChange(
                            new LineageEvent.LifecycleStateChangeFacet(
                                PRODUCER_URL, SCHEMA_URL, "create"))
                        .build())));

    assertThat(jobEventRow.getJob().getNamespaceName()).isEqualTo(NAMESPACE);
    assertThat(jobEventRow.getJob().getName()).isEqualTo(READ_JOB_NAME);
    assertThat(jobEventRow.getJob().getDescription().get()).isEqualTo("documentation");
    assertThat(jobEventRow.getJob().getLocation()).isNull();

    assertThat(jobEventRow.getInputs()).isPresent();
    assertThat(jobEventRow.getInputs().get()).hasSize(1);
    assertThat(jobEventRow.getInputs().get().get(0).getDatasetRow())
        .hasFieldOrPropertyWithValue("namespaceName", "namespace")
        .hasFieldOrPropertyWithValue("name", "dataset_input");

    assertThat(jobEventRow.getOutputs()).isPresent();
    assertThat(jobEventRow.getOutputs().get()).hasSize(1);
    assertThat(jobEventRow.getOutputs().get().get(0).getDatasetRow())
        .hasFieldOrPropertyWithValue("namespaceName", "namespace")
        .hasFieldOrPropertyWithValue("name", "dataset_output");
    assertThat(jobEventRow.getOutputs().get().get(0).getDatasetVersionRow().getLifecycleState())
        .isEqualTo("create");
  }

  @Test
  void testUpdateMarquezModelLifecycleStateChangeFacet() {
    Dataset dataset =
        new Dataset(
            NAMESPACE,
            DATASET_NAME,
            LineageEvent.DatasetFacets.builder()
                .lifecycleStateChange(
                    new LineageEvent.LifecycleStateChangeFacet(
                        PRODUCER_URL, SCHEMA_URL, "TRUNCATE"))
                .build());

    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob =
        LineageTestUtils.createLineageRow(
            dao, WRITE_JOB_NAME, "COMPLETE", jobFacet, Arrays.asList(), Arrays.asList(dataset));

    assertThat(writeJob.getOutputs()).isPresent().get().asList().size().isEqualTo(1);
    assertThat(writeJob.getOutputs().get().get(0).getDatasetVersionRow().getLifecycleState())
        .isEqualTo("TRUNCATE");
  }

  @Test
  void testUpdateMarquezModelDatasetWithColumnLineageFacet() {
    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(getInputDataset()),
            Arrays.asList(getOutputDatasetWithColumnLineage()));

    UUID inputDatasetVersion = writeJob.getInputs().get().get(0).getDatasetVersionRow().getUuid();
    UUID outputDatasetVersion = writeJob.getOutputs().get().get(0).getDatasetVersionRow().getUuid();

    UUID outputDatasetField =
        datasetFieldDao
            .findUuid(writeJob.getOutputs().get().get(0).getDatasetRow().getUuid(), OUTPUT_COLUMN)
            .orElseThrow();
    assertThat(writeJob.getOutputs().get().get(0).getColumnLineageRows()).isEmpty();
    assertThat(
            columnLineageDao.findColumnLineageByDatasetVersionAndOutputDatasetFields(
                outputDatasetVersion, List.of(outputDatasetField)))
        .extracting(
            (ds) -> ds.getInputDatasetFieldUuid(),
            (ds) -> ds.getInputDatasetVersionUuid(),
            (ds) -> ds.getOutputDatasetFieldUuid(),
            (ds) -> ds.getOutputDatasetVersionUuid(),
            (ds) -> ds.getTransformationDescription().get(),
            (ds) -> ds.getTransformationType().get())
        .containsExactly(
            Tuple.tuple(
                datasetFieldDao
                    .findUuid(
                        writeJob.getInputs().get().get(0).getDatasetRow().getUuid(),
                        INPUT_FIELD_NAME)
                    .get(),
                inputDatasetVersion,
                outputDatasetField,
                outputDatasetVersion,
                TRANSFORMATION_DESCRIPTION,
                TRANSFORMATION_TYPE));
  }

  @Test
  void testUpdateMarquezModelDatasetWithColumnLineageFacetWhenInputFieldDoesNotExist() {
    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Collections.emptyList(),
            Arrays.asList(getOutputDatasetWithColumnLineage()));

    UUID outputDatasetVersion = writeJob.getOutputs().get().get(0).getDatasetVersionRow().getUuid();
    UUID outputDatasetField =
        datasetFieldDao
            .findUuid(writeJob.getOutputs().get().get(0).getDatasetRow().getUuid(), OUTPUT_COLUMN)
            .orElseThrow();
    assertThat(
            columnLineageDao.findColumnLineageByDatasetVersionAndOutputDatasetFields(
                outputDatasetVersion, List.of(outputDatasetField)))
        .isEmpty();
  }

  @Test
  void testUpdateMarquezModelDatasetWithColumnLineageFacetWhenOutputFieldDoesNotExist() {
    Dataset outputDatasetWithoutOutputFieldSchema =
        new Dataset(
            NAMESPACE,
            DATASET_NAME,
            LineageEvent.DatasetFacets.builder() // schema is missing
                .columnLineage(
                    new LineageEvent.ColumnLineageDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        new LineageEvent.ColumnLineageDatasetFacetFields(
                            Collections.singletonMap(
                                OUTPUT_COLUMN,
                                new LineageEvent.ColumnLineageOutputColumn(
                                    Collections.singletonList(
                                        new LineageEvent.ColumnLineageInputField(
                                            INPUT_NAMESPACE, INPUT_DATASET, INPUT_FIELD_NAME)),
                                    TRANSFORMATION_DESCRIPTION,
                                    TRANSFORMATION_TYPE)))))
                .build());

    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(getInputDataset()),
            Arrays.asList(outputDatasetWithoutOutputFieldSchema));

    // make sure no column lineage was written
    assertEquals(0, writeJob.getOutputs().get().get(0).getColumnLineageRows().size());
  }

  @Test
  void lateColumnLineageFailureRollsBackWholeProjection() {
    UUID runUuid = UUID.randomUUID();
    String suffix = runUuid.toString();
    String jobName = "rollback_job_" + suffix;
    String datasetNamespace = "rollback_namespace_" + suffix;
    String inputName = "rollback_input_" + suffix;
    String outputName = "rollback_output_" + suffix;
    Dataset input =
        new Dataset(
            datasetNamespace,
            inputName,
            LineageTestUtils.newDatasetFacet(new SchemaField(INPUT_FIELD_NAME, "STRING", "input")));
    Dataset output =
        new Dataset(
            datasetNamespace,
            outputName,
            DatasetFacets.builder()
                .schema(
                    new SchemaDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        List.of(new SchemaField(OUTPUT_COLUMN, "STRING", "output"))))
                .columnLineage(
                    new LineageEvent.ColumnLineageDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        new LineageEvent.ColumnLineageDatasetFacetFields(
                            Collections.singletonMap(
                                OUTPUT_COLUMN,
                                new LineageEvent.ColumnLineageOutputColumn(
                                    List.of(
                                        new LineageEvent.ColumnLineageInputField(
                                            datasetNamespace, inputName, INPUT_FIELD_NAME)),
                                    TRANSFORMATION_DESCRIPTION,
                                    // Force a physical column_lineage insert failure after the job,
                                    // run, and both dataset sides have been projected.
                                    "x".repeat(256))))))
                .build());

    assertThatThrownBy(
            () ->
                LineageTestUtils.createLineageRow(
                    dao,
                    jobName,
                    runUuid,
                    "COMPLETE",
                    JobFacet.builder().build(),
                    List.of(input),
                    List.of(output),
                    null,
                    ImmutableMap.of()))
        .isInstanceOf(RuntimeException.class)
        .hasStackTraceContaining("value too long for type character varying(255)");

    assertThat(runDao.findRunByUuidAsRow(runUuid)).isEmpty();
    assertThat(jobDao.findJobByNameAsRow(NAMESPACE, jobName)).isEmpty();
    assertThat(datasetDao.findDatasetAsRow(datasetNamespace, inputName)).isEmpty();
    assertThat(datasetDao.findDatasetAsRow(datasetNamespace, outputName)).isEmpty();
    assertThat(namespaceDao.findNamespaceByName(datasetNamespace)).isEmpty();
  }

  @Test
  void testGetUrlOrNullReturnsEmptyString() {
    assertEquals("", dao.getUrlOrNull(null));
  }

  private static int findSqlIndexAfter(
      List<String> executedSql, int previousIndex, String... requiredFragments) {
    for (int index = previousIndex + 1; index < executedSql.size(); index++) {
      String sql = executedSql.get(index);
      if (Arrays.stream(requiredFragments).allMatch(sql::contains)) {
        return index;
      }
    }
    throw new AssertionError(
        "Did not execute SQL containing "
            + Arrays.toString(requiredFragments)
            + " after statement index "
            + previousIndex);
  }

  @Test
  /**
   * When trying to insert new column level lineage data, do not create additional row if triad
   * (dataset_version_uuid, output_column_name and input_field) is the same. Upsert instead.
   */
  void testUpsertColumnLineageData() {
    final String UPDATED_TRANSFORMATION_TYPE = "transformation_type";
    final String UPDATED_TRANSFORMATION_DESCRIPTION = "updated_transformation_description";

    Dataset inputDataset = getInputDataset();
    Dataset dataset = getOutputDatasetWithColumnLineage();

    Dataset updateDataset =
        new Dataset(
            NAMESPACE,
            DATASET_NAME,
            LineageEvent.DatasetFacets.builder()
                .schema(
                    new SchemaDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        Arrays.asList(new SchemaField(OUTPUT_COLUMN, "STRING", "my name"))))
                .columnLineage(
                    new LineageEvent.ColumnLineageDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        new LineageEvent.ColumnLineageDatasetFacetFields(
                            Collections.singletonMap(
                                OUTPUT_COLUMN,
                                new LineageEvent.ColumnLineageOutputColumn(
                                    Collections.singletonList(
                                        new LineageEvent.ColumnLineageInputField(
                                            INPUT_NAMESPACE, INPUT_DATASET, INPUT_FIELD_NAME)),
                                    UPDATED_TRANSFORMATION_DESCRIPTION,
                                    UPDATED_TRANSFORMATION_TYPE)))))
                .build());

    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob1 =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(inputDataset),
            Arrays.asList(dataset));

    UpdateLineageRow writeJob2 =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(inputDataset),
            Arrays.asList(updateDataset));

    // try to read with same inputs as writeJob1 and check if size=1
    UpdateLineageRow readJob2 =
        LineageTestUtils.createLineageRow(
            dao, WRITE_JOB_NAME, "COMPLETE", jobFacet, Arrays.asList(dataset), Arrays.asList());

    // only 1 row should be present (no multiple Optional<DatasetVersionRow> candidates)
    assertThat(readJob2.getInputs()).isPresent().get().asList().size().isEqualTo(1);

    // finally, test if upsert was successful
    assertThat(readJob2.getInputs().get().get(0).getDatasetVersionRow())
        .isNotEqualTo(writeJob1.getOutputs().get().get(0).getDatasetVersionRow());

    assertThat(readJob2.getInputs().get().get(0).getDatasetVersionRow())
        .isEqualTo(writeJob2.getOutputs().get().get(0).getDatasetVersionRow());
  }

  @Test
  void testUpdateMarquezModelDatasetWithSymlinks() {
    Dataset dataset =
        new Dataset(
            NAMESPACE,
            DATASET_NAME,
            LineageEvent.DatasetFacets.builder()
                .symlinks(
                    new LineageEvent.DatasetSymlinkFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        Collections.singletonList(
                            new LineageEvent.SymlinkIdentifier(
                                "symlinkNamespace", "symlinkName", "some-type"))))
                .build());

    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob =
        LineageTestUtils.createLineageRow(
            dao, WRITE_JOB_NAME, "COMPLETE", jobFacet, Arrays.asList(), Arrays.asList(dataset));

    UpdateLineageRow readJob =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(
                new Dataset(
                    "symlinkNamespace",
                    "symlinkName",
                    LineageEvent.DatasetFacets.builder().build())),
            Arrays.asList());

    // make sure writeJob output dataset and readJob input dataset are the same (have the same uuid)
    assertThat(writeJob.getOutputs()).isPresent().get().asList().size().isEqualTo(1);
    assertThat(writeJob.getOutputs().get().get(0).getDatasetRow().getUuid())
        .isEqualTo(readJob.getInputs().get().get(0).getDatasetRow().getUuid());
    // make sure symlink is stored with type in dataset_symlinks table
    assertThat(
            symlinkDao
                .findDatasetSymlinkByNamespaceUuidAndName(
                    namespaceDao.findNamespaceByName("symlinkNamespace").get().getUuid(),
                    "symlinkName")
                .get()
                .getType()
                .get())
        .isEqualTo("some-type");
  }

  @Test
  void concurrentPrimaryAndAliasEventsSerializeCanonicalJobMappings() throws Exception {
    String suffix = UUID.randomUUID().toString();
    NamespaceRow namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), java.time.Instant.now(), NAMESPACE, getClass().getName());
    String primaryJobName = "canonical_job_" + suffix;
    String aliasJobName = "alias_job_" + suffix;
    JobRow primaryJob =
        createJobWithoutSymlinkTarget(
            jdbi, namespace, primaryJobName, "canonical concurrency target");
    createJobWithSymlinkTarget(
        jdbi, namespace, aliasJobName, primaryJob.getUuid(), "existing alias");

    Dataset primaryInput = new Dataset("alias_lock_" + suffix, "primary_input", datasetFacets);
    Dataset primaryOutput = new Dataset("alias_lock_" + suffix, "primary_output", datasetFacets);
    Dataset aliasInput = new Dataset("alias_lock_" + suffix, "alias_input", datasetFacets);
    Dataset aliasOutput = new Dataset("alias_lock_" + suffix, "alias_output", datasetFacets);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<UpdateLineageRow> primary =
          executor.submit(
              () ->
                  projectAfter(
                      ready, start, primaryJobName, List.of(primaryInput), List.of(primaryOutput)));
      Future<UpdateLineageRow> alias =
          executor.submit(
              () ->
                  projectAfter(
                      ready, start, aliasJobName, List.of(aliasInput), List.of(aliasOutput)));

      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      start.countDown();
      assertThat(primary.get(20, TimeUnit.SECONDS).getJob().getUuid())
          .isEqualTo(primaryJob.getUuid());
      assertThat(alias.get(20, TimeUnit.SECONDS).getJob().getUuid())
          .isEqualTo(primaryJob.getUuid());

      for (JobVersionDao.IoType ioType : JobVersionDao.IoType.values()) {
        int currentMappings =
            jdbi.withHandle(
                handle ->
                    handle
                        .createQuery(
                            """
                            SELECT count(*)
                            FROM job_versions_io_mapping
                            WHERE job_uuid = :jobUuid
                              AND io_type = :ioType
                              AND is_current_job_version = TRUE
                            """)
                        .bind("jobUuid", primaryJob.getUuid())
                        .bind("ioType", ioType.name())
                        .mapTo(Integer.class)
                        .one());
        assertThat(currentMappings).as(ioType.name()).isEqualTo(1);
      }
    } finally {
      start.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private UpdateLineageRow projectAfter(
      CountDownLatch ready,
      CountDownLatch start,
      String jobName,
      List<Dataset> inputs,
      List<Dataset> outputs)
      throws InterruptedException {
    ready.countDown();
    if (!start.await(5, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Timed out waiting to start concurrent projections");
    }
    return LineageTestUtils.createLineageRow(
        jdbi.onDemand(OpenLineageDao.class),
        jobName,
        "COMPLETE",
        LineageEvent.JobFacet.builder().build(),
        inputs,
        outputs);
  }

  /**
   * When reading a new dataset, a version is created and the dataset's current version is updated
   * immediately.
   */
  @Test
  void testUpdateMarquezModelWithInputOnlyDataset() {
    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "RUNNING",
            jobFacet,
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)),
            Arrays.asList());

    assertThat(writeJob.getInputs())
        .isPresent()
        .get(InstanceOfAssertFactories.list(DatasetRecord.class))
        .hasSize(1)
        .first()
        .matches(v -> v.getDatasetRow().getCurrentVersionUuid().isPresent());
    assertThat(writeJob.getRunIoSnapshot()).isNotNull();
    assertThat(writeJob.getRunIoSnapshot().getInputs()).hasSize(1);
    assertThat(writeJob.getRunIoSnapshot().getOutputs()).isEmpty();
  }

  /**
   * When reading a dataset, even when reporting a schema that differs from the prior written
   * schema, the dataset version doesn't change.
   */
  @Test
  void testUpdateMarquezModelWithNonMatchingReadSchema() {
    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(),
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)));

    DatasetFacets overrideFacet =
        new DatasetFacets(
            this.datasetFacets.getDocumentation(),
            new SchemaDatasetFacet(
                LineageTestUtils.PRODUCER_URL,
                LineageTestUtils.SCHEMA_URL,
                Arrays.asList(
                    new SchemaField("name", "STRING", "my name"),
                    new SchemaField("age", "INT", "my age"),
                    new SchemaField("eyeColor", "STRING", "my eye color"))),
            this.datasetFacets.getLifecycleStateChange(),
            this.datasetFacets.getDataSource(),
            this.datasetFacets.getColumnLineage(),
            null,
            this.datasetFacets.getDescription(),
            this.datasetFacets.getAdditionalFacets());
    UpdateLineageRow readJob =
        LineageTestUtils.createLineageRow(
            dao,
            READ_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, overrideFacet)),
            Arrays.asList());

    assertThat(writeJob.getOutputs()).isPresent().get().asList().size().isEqualTo(1);
    assertThat(readJob.getInputs()).isPresent().get().asList().size().isEqualTo(1);
    assertThat(readJob.getInputs().get().get(0).getDatasetVersionRow())
        .isEqualTo(writeJob.getOutputs().get().get(0).getDatasetVersionRow());
  }

  /**
   * When a dataset is written, its version changes. When read the version is assumed to be the last
   * version written.
   */
  @Test
  void testUpdateMarquezModelWithPriorWrites() {
    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob1 =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(),
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)));
    UpdateLineageRow readJob1 =
        LineageTestUtils.createLineageRow(
            dao,
            READ_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)),
            Arrays.asList());

    UpdateLineageRow writeJob2 =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(),
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)));
    UpdateLineageRow writeJob3 =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(),
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)));

    UpdateLineageRow readJob2 =
        LineageTestUtils.createLineageRow(
            dao,
            READ_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)),
            Arrays.asList());

    // verify readJob1 read the version written by writeJob1
    assertThat(writeJob1.getOutputs()).isPresent().get().asList().size().isEqualTo(1);
    assertThat(readJob1.getInputs()).isPresent().get().asList().size().isEqualTo(1);

    assertThat(readJob1.getInputs().get().get(0).getDatasetVersionRow())
        .isEqualTo(writeJob1.getOutputs().get().get(0).getDatasetVersionRow());

    // verify that writeJob2 and writeJob3 wrote different versions from writeJob1
    assertThat(writeJob2.getOutputs()).isPresent().get().asList().size().isEqualTo(1);
    assertThat(writeJob3.getOutputs()).isPresent().get().asList().size().isEqualTo(1);
    assertThat(writeJob1.getOutputs())
        .get()
        .extracting((ds) -> ds.get(0).getDatasetVersionRow().getUuid())
        .isNotEqualTo(writeJob2.getOutputs().get().get(0).getDatasetVersionRow().getUuid())
        .isNotEqualTo(writeJob3.getOutputs().get().get(0).getDatasetVersionRow().getUuid());
    assertThat(writeJob2.getOutputs())
        .get()
        .extracting((ds) -> ds.get(0).getDatasetVersionRow().getUuid())
        .isNotEqualTo(writeJob3.getOutputs().get().get(0).getDatasetVersionRow().getUuid());

    // verify that readJob2 read the version produced by writeJob3
    assertThat(readJob2.getInputs()).isPresent().get().asList().size().isEqualTo(1);

    assertThat(readJob2.getInputs().get().get(0).getDatasetVersionRow())
        .isEqualTo(writeJob3.getOutputs().get().get(0).getDatasetVersionRow());

    // verify that the dataset schema version remained the same across all runs
    assertThat(
            Stream.of(
                    writeJob1.getOutputs().get().get(0),
                    readJob1.getInputs().get().get(0),
                    writeJob2.getOutputs().get().get(0),
                    writeJob3.getOutputs().get().get(0),
                    readJob2.getInputs().get().get(0))
                .map(ds -> ds.getDatasetVersionRow().getSchemaVersionUuid().orElseThrow())
                .collect(Collectors.toSet()))
        .hasSize(1);
  }

  @Test
  void testGetOpenLineageEvents() {
    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow writeJob =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(),
            Arrays.asList(new Dataset(NAMESPACE, DATASET_NAME, datasetFacets)));

    List<LineageEvent> lineageEvents = dao.findLineageEventsByRunUuid(writeJob.getRun().getUuid());
    assertThat(lineageEvents).hasSize(1);

    assertThat(lineageEvents.get(0).getEventType()).isEqualTo("COMPLETE");

    LineageEvent.Job job = lineageEvents.get(0).getJob();
    assertThat(job).extracting("namespace", "name").contains(NAMESPACE, WRITE_JOB_NAME);
  }

  @Test
  void testInputOutputDatasetFacets() {
    JobFacet jobFacet = JobFacet.builder().build();
    UpdateLineageRow lineageRow =
        LineageTestUtils.createLineageRow(
            dao,
            WRITE_JOB_NAME,
            "COMPLETE",
            jobFacet,
            Arrays.asList(
                new Dataset(
                    "namespace",
                    "dataset_input",
                    null,
                    LineageEvent.InputDatasetFacets.builder()
                        .additional(
                            ImmutableMap.of(
                                "inputFacet1", "{some-facet1}",
                                "inputFacet2", "{some-facet2}"))
                        .build(),
                    null)),
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
                        .build())));

    Run run = runDao.findRunByUuid(lineageRow.getRun().getUuid()).get();

    assertThat(run.getInputDatasetVersions()).hasSize(1);
    assertThat(run.getInputDatasetVersions().get(0).getDatasetVersionId())
        .isEqualTo(
            new DatasetVersionId(
                NamespaceName.of("namespace"),
                DatasetName.of("dataset_input"),
                lineageRow.getInputs().get().get(0).getDatasetVersionRow().getVersion()));
    assertThat(run.getInputDatasetVersions().get(0).getFacets())
        .containsAllEntriesOf(
            ImmutableMap.of(
                "inputFacet1", "{some-facet1}",
                "inputFacet2", "{some-facet2}"));

    assertThat(run.getOutputDatasetVersions()).hasSize(1);
    assertThat(run.getOutputDatasetVersions().get(0).getDatasetVersionId())
        .isEqualTo(
            new DatasetVersionId(
                NamespaceName.of("namespace"),
                DatasetName.of("dataset_output"),
                lineageRow.getOutputs().get().get(0).getDatasetVersionRow().getVersion()));
    assertThat(run.getOutputDatasetVersions().get(0).getFacets())
        .containsAllEntriesOf(
            ImmutableMap.of(
                "outputFacet1", "{some-facet1}",
                "outputFacet2", "{some-facet2}"));
  }

  private Dataset getInputDataset() {
    return new Dataset(
        INPUT_NAMESPACE,
        INPUT_DATASET,
        LineageEvent.DatasetFacets.builder()
            .schema(
                new SchemaDatasetFacet(
                    PRODUCER_URL,
                    SCHEMA_URL,
                    Arrays.asList(new SchemaField(INPUT_FIELD_NAME, "STRING", "my name"))))
            .build());
  }

  private Dataset getOutputDatasetWithColumnLineage() {
    return new Dataset(
        NAMESPACE,
        DATASET_NAME,
        LineageEvent.DatasetFacets.builder()
            .schema(
                new SchemaDatasetFacet(
                    PRODUCER_URL,
                    SCHEMA_URL,
                    Arrays.asList(new SchemaField(OUTPUT_COLUMN, "STRING", "my name"))))
            .columnLineage(
                new LineageEvent.ColumnLineageDatasetFacet(
                    PRODUCER_URL,
                    SCHEMA_URL,
                    new LineageEvent.ColumnLineageDatasetFacetFields(
                        Collections.singletonMap(
                            OUTPUT_COLUMN,
                            new LineageEvent.ColumnLineageOutputColumn(
                                Collections.singletonList(
                                    new LineageEvent.ColumnLineageInputField(
                                        INPUT_NAMESPACE, INPUT_DATASET, INPUT_FIELD_NAME)),
                                TRANSFORMATION_DESCRIPTION,
                                TRANSFORMATION_TYPE)))))
            .build());
  }
}
