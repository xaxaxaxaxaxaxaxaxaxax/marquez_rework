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

import java.time.Instant;
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
import marquez.common.Utils;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetVersionId;
import marquez.common.models.NamespaceName;
import marquez.db.models.DatasetRow;
import marquez.db.models.JobRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.ProjectionOrder;
import marquez.db.models.RunArgsRow;
import marquez.db.models.UpdateLineageRow;
import marquez.db.models.UpdateLineageRow.DatasetRecord;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.JobEvent;
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
  void parentFacetCreatesParentJobAndRunInItsOwnSanitizedNamespace() {
    String suffix = UUID.randomUUID().toString();
    String rawParentNamespace = "parent://tenant;" + suffix;
    String parentNamespace = Utils.sanitizeOpenLineageNamespace(rawParentNamespace);
    String parentJobName = "parent-" + suffix;
    UUID parentRunUuid = UUID.randomUUID();
    LineageEvent.ParentRunFacet parent =
        LineageEvent.ParentRunFacet.builder()
            ._producer(PRODUCER_URL)
            ._schemaURL(SCHEMA_URL)
            .run(LineageEvent.RunLink.builder().runId(parentRunUuid.toString()).build())
            .job(
                LineageEvent.JobLink.builder()
                    .namespace(rawParentNamespace)
                    .name(parentJobName)
                    .build())
            .build();

    UpdateLineageRow child =
        LineageTestUtils.createLineageRow(
            dao,
            "child-" + suffix,
            "COMPLETE",
            JobFacet.builder().build(),
            List.of(),
            List.of(),
            parent);

    assertThat(child.getJob().getNamespaceName()).isEqualTo(NAMESPACE);
    jdbi.useHandle(
        handle -> {
          UUID parentJobUuid =
              handle
                  .createQuery(
                      "SELECT uuid FROM jobs WHERE namespace_name = :namespace AND name = :name")
                  .bind("namespace", parentNamespace)
                  .bind("name", parentJobName)
                  .mapTo(UUID.class)
                  .one();
          handle
              .createQuery("SELECT job_uuid, namespace_name FROM runs WHERE uuid = :parentRunUuid")
              .bind("parentRunUuid", parentRunUuid)
              .map(
                  (resultSet, context) -> {
                    assertThat(resultSet.getObject("job_uuid", UUID.class))
                        .isEqualTo(parentJobUuid);
                    assertThat(resultSet.getString("namespace_name")).isEqualTo(parentNamespace);
                    return true;
                  })
              .one();
        });
  }

  @Test
  void nonUuidParentRunUsesNormalOpenLineageIdentityAndPersistsItOnChild() {
    String suffix = UUID.randomUUID().toString();
    String parentRunId = "scheduled-parent-" + suffix;
    UUID expectedParentRunUuid = Utils.openLineageRunUuid(parentRunId);
    LineageEvent.ParentRunFacet parent =
        LineageEvent.ParentRunFacet.builder()
            ._producer(PRODUCER_URL)
            ._schemaURL(SCHEMA_URL)
            .run(LineageEvent.RunLink.builder().runId(parentRunId).build())
            .job(
                LineageEvent.JobLink.builder()
                    .namespace("normal-parent-namespace")
                    .name("normal-parent-job")
                    .build())
            .build();

    UpdateLineageRow child =
        LineageTestUtils.createLineageRow(
            dao,
            "normal-child-" + suffix,
            "COMPLETE",
            JobFacet.builder().build(),
            List.of(),
            List.of(),
            parent);

    assertThat(child.getRun().getParentRunUuid()).contains(expectedParentRunUuid);
    assertThat(runDao.findRunByUuidAsRow(expectedParentRunUuid)).isPresent();
  }

  @Test
  void normalRunIdentityIsReturnedAndStoredOnTheCanonicalJob() {
    String suffix = UUID.randomUUID().toString();
    String runId = "normal-run-" + suffix;
    UUID expectedRunUuid = Utils.openLineageRunUuid(runId);
    Instant eventTime = Instant.parse("2026-08-14T00:00:00Z");
    LineageEvent event =
        runEvent(
            NAMESPACE,
            "normal-job-" + suffix,
            runId,
            eventTime,
            "START",
            JobFacet.builder().build(),
            LineageEvent.RunFacet.builder().build(),
            Collections.emptyList(),
            Collections.emptyList());

    UpdateLineageRow projected =
        dao.updateMarquezModel(
            event,
            Utils.getMapper(),
            false,
            new ProjectionOrder(eventTime, Utils.sha256Utf8(Utils.toJson(event))));

    assertThat(projected.getRun().getUuid()).isEqualTo(expectedRunUuid);
    assertThat(projected.getRunState().getRunUuid()).isEqualTo(expectedRunUuid);
    assertThat(projected.getRun().getCreatedAt()).isEqualTo(eventTime);
    assertThat(projected.getRunArgs().getArgs()).isEqualTo(OpenLineageDao.EMPTY_RUN_ARGS_JSON);
    assertThat(projected.getRunArgs().getChecksum())
        .isEqualTo(OpenLineageDao.EMPTY_RUN_ARGS_CHECKSUM);
    assertThat(jobDao.lockJobByUuid(projected.getJob().getUuid()).getCurrentRunUuid())
        .contains(expectedRunUuid);
  }

  @Test
  void legacyAirflowRepairedParentPlaceholderIsPromotedByTheObservedParentRun() {
    String suffix = UUID.randomUUID().toString();
    String rawParentNamespace = "airflow parent " + suffix;
    String parentNamespace = Utils.sanitizeOpenLineageNamespace(rawParentNamespace);
    String dagName = "dag_" + suffix;
    String taskName = dagName + ".task";
    String parentRunId = "scheduled__2026-08-14T00:00:00+00:00_" + suffix;
    UUID expectedLegacyUuid = Utils.toNameBasedUuid(parentNamespace, dagName, parentRunId);
    UUID expectedRepairedUuid =
        Utils.toNameBasedUuid(parentNamespace, dagName, expectedLegacyUuid.toString());
    NamespaceRow parentNamespaceRow =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), parentNamespace, getClass().getName());
    JobRow foreignJob =
        createJobWithoutSymlinkTarget(
            jdbi, parentNamespaceRow, "legacy-foreign-" + suffix, "foreign legacy owner");
    RunArgsRow foreignArgs = DbTestUtils.newRunArgs(jdbi);
    runDao.upsert(
        expectedLegacyUuid,
        null,
        "foreign-legacy-external-" + suffix,
        Instant.parse("2026-08-13T23:30:00Z"),
        foreignJob.getUuid(),
        null,
        foreignArgs.getUuid(),
        null,
        null,
        foreignJob.getNamespaceName(),
        foreignJob.getName(),
        null);
    ForeignRunSnapshot foreignBefore = foreignRunSnapshot(expectedLegacyUuid);
    LineageEvent.ParentRunFacet parent =
        LineageEvent.ParentRunFacet.builder()
            ._producer(PRODUCER_URL)
            ._schemaURL(SCHEMA_URL)
            .run(LineageEvent.RunLink.builder().runId(parentRunId).build())
            .job(
                LineageEvent.JobLink.builder().namespace(rawParentNamespace).name(taskName).build())
            .build();
    Instant childTime = Instant.parse("2026-08-14T01:00:00Z");
    LineageEvent childEvent =
        runEvent(
            NAMESPACE,
            taskName,
            "child-" + suffix,
            childTime,
            "START",
            JobFacet.builder().build(),
            LineageEvent.RunFacet.builder().parent(parent).build(),
            Collections.emptyList(),
            Collections.emptyList());

    UpdateLineageRow child = projectOrdered(childEvent, false);

    assertThat(child.getRun().getParentRunUuid()).contains(expectedRepairedUuid);
    assertThat(isOpenLineageParentPlaceholder(expectedRepairedUuid)).isTrue();

    Instant parentTime = childTime.plusSeconds(1);
    LineageEvent observedParent =
        runEvent(
            rawParentNamespace,
            dagName,
            parentRunId,
            parentTime,
            "START",
            JobFacet.builder().build(),
            LineageEvent.RunFacet.builder().build(),
            Collections.emptyList(),
            Collections.emptyList());
    UpdateLineageRow promoted = projectOrdered(observedParent, false);

    assertThat(promoted.getRun().getUuid()).isEqualTo(expectedRepairedUuid);
    assertThat(promoted.getRun().getCreatedAt()).isEqualTo(parentTime);
    assertThat(isOpenLineageParentPlaceholder(expectedRepairedUuid)).isFalse();
    assertThat(jobDao.lockJobByUuid(promoted.getJob().getUuid()).getCurrentRunUuid())
        .contains(expectedRepairedUuid);
    assertThat(foreignRunSnapshot(expectedLegacyUuid)).isEqualTo(foreignBefore);
  }

  @Test
  void airflowParentAliasUsesCanonicalRunIdentityAndReportedPrefix() {
    String suffix = UUID.randomUUID().toString();
    NamespaceRow namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), NAMESPACE, getClass().getName());
    String canonicalParentName = "canonical-dag-" + suffix;
    String reportedParentName = "reported-dag-" + suffix;
    String reportedTaskName = reportedParentName + ".task";
    JobRow canonicalParent =
        createJobWithoutSymlinkTarget(jdbi, namespace, canonicalParentName, "canonical parent job");
    createJobWithSymlinkTarget(
        jdbi, namespace, reportedParentName, canonicalParent.getUuid(), "reported parent alias");
    String parentRunId = "scheduled__2026-08-14T03:00:00+00:00_" + suffix;
    UUID expectedParentRunUuid = Utils.toNameBasedUuid(NAMESPACE, canonicalParentName, parentRunId);
    UUID aliasDerivedParentRunUuid =
        Utils.toNameBasedUuid(NAMESPACE, reportedParentName, parentRunId);
    LineageEvent.ParentRunFacet parent =
        LineageEvent.ParentRunFacet.builder()
            ._producer(PRODUCER_URL)
            ._schemaURL(SCHEMA_URL)
            .run(LineageEvent.RunLink.builder().runId(parentRunId).build())
            .job(LineageEvent.JobLink.builder().namespace(NAMESPACE).name(reportedTaskName).build())
            .build();
    Instant childTime = Instant.parse("2026-08-14T04:00:00Z");
    LineageEvent childEvent =
        runEvent(
            NAMESPACE,
            reportedTaskName,
            "aliased-parent-child-" + suffix,
            childTime,
            "START",
            JobFacet.builder().build(),
            LineageEvent.RunFacet.builder().parent(parent).build(),
            Collections.emptyList(),
            Collections.emptyList());

    UpdateLineageRow child = projectOrdered(childEvent, false);

    assertThat(child.getJob().getParentJobUuid()).isEqualTo(canonicalParent.getUuid());
    assertThat(child.getJob().getSimpleName()).isEqualTo("task");
    assertThat(child.getJob().getName()).isEqualTo(canonicalParentName + ".task");
    assertThat(child.getRun().getParentRunUuid()).contains(expectedParentRunUuid);
    assertThat(isOpenLineageParentPlaceholder(expectedParentRunUuid)).isTrue();
    assertThat(runDao.findRunByUuidAsRow(aliasDerivedParentRunUuid)).isEmpty();
    assertThat(jobDao.findJobByNameAsRow(NAMESPACE, canonicalParentName + "." + reportedTaskName))
        .isEmpty();
    assertThat(jobDao.lockJobByUuid(canonicalParent.getUuid()).getCurrentRunUuid())
        .contains(expectedParentRunUuid);

    LineageEvent observedParent =
        runEvent(
            NAMESPACE,
            canonicalParentName,
            parentRunId,
            childTime.plusSeconds(1),
            "START",
            JobFacet.builder().build(),
            LineageEvent.RunFacet.builder().build(),
            Collections.emptyList(),
            Collections.emptyList());
    UpdateLineageRow promoted = projectOrdered(observedParent, false);

    assertThat(promoted.getRun().getUuid()).isEqualTo(expectedParentRunUuid);
    assertThat(isOpenLineageParentPlaceholder(expectedParentRunUuid)).isFalse();
  }

  @Test
  void foreignRequestedRunCollisionRepairsEveryRunBoundIdentifierWithoutTouchingForeignRow() {
    String suffix = UUID.randomUUID().toString();
    String requestedRunId = UUID.randomUUID().toString();
    UUID requestedRunUuid = UUID.fromString(requestedRunId);
    NamespaceRow namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), NAMESPACE, getClass().getName());
    JobRow foreignJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, "foreign-owner-" + suffix, "foreign-owner");
    RunArgsRow foreignArgs = DbTestUtils.newRunArgs(jdbi);
    Instant foreignTime = Instant.parse("2026-08-13T23:00:00Z");
    runDao.upsert(
        requestedRunUuid,
        null,
        "foreign-external-" + suffix,
        foreignTime,
        foreignJob.getUuid(),
        null,
        foreignArgs.getUuid(),
        foreignTime,
        foreignTime.plusSeconds(5),
        marquez.common.models.RunState.COMPLETED,
        foreignTime.plusSeconds(5),
        foreignJob.getNamespaceName(),
        foreignJob.getName(),
        "foreign-location");
    ForeignRunSnapshot foreignBefore = foreignRunSnapshot(requestedRunUuid);

    String jobName = "collision-job-" + suffix;
    String datasetNamespace = "collision-dataset-" + suffix;
    String inputName = "input-" + suffix;
    String outputName = "output-" + suffix;
    Dataset input = getInputDataset(datasetNamespace, inputName);
    Dataset output = getOutputDatasetWithColumnLineage(datasetNamespace, outputName, inputName);
    Instant eventTime = Instant.parse("2026-08-14T02:00:00Z");
    LineageEvent.RunFacet runFacet = LineageEvent.RunFacet.builder().build();
    runFacet.setFacet("customRun", Collections.singletonMap("value", suffix));
    JobFacet jobFacet = JobFacet.builder().build();
    jobFacet.setFacet("customJob", Collections.singletonMap("value", suffix));
    LineageEvent event =
        runEvent(
            NAMESPACE,
            jobName,
            requestedRunId,
            eventTime,
            "COMPLETE",
            jobFacet,
            runFacet,
            List.of(input),
            List.of(output));

    UpdateLineageRow projected = projectOrdered(event, true);
    UUID effectiveRunUuid = projected.getRun().getUuid();

    assertThat(effectiveRunUuid).isNotEqualTo(requestedRunUuid);
    assertThat(projected.getRunState().getRunUuid()).isEqualTo(effectiveRunUuid);
    assertThat(jobDao.lockJobByUuid(projected.getJob().getUuid()).getCurrentRunUuid())
        .contains(effectiveRunUuid);
    assertThat(projected.getRunIoSnapshot()).isNotNull();
    assertThat(projected.getRunIoSnapshot().getInputs()).hasSize(1);
    assertThat(projected.getRunIoSnapshot().getOutputs()).hasSize(1);
    assertThat(projected.getJobVersionBag()).isNotNull();

    DatasetRecord projectedInput = projected.getInputs().orElseThrow().get(0);
    DatasetRecord projectedOutput = projected.getOutputs().orElseThrow().get(0);
    assertThat(projectedOutput.getDatasetVersionRow().getRunUuid()).contains(effectiveRunUuid);
    assertThat(projectedOutput.getDatasetVersionRow().getVersion())
        .isEqualTo(
            Utils.newDatasetVersionFor(
                    datasetNamespace,
                    OpenLineageDao.DEFAULT_SOURCE_NAME,
                    output.getName(),
                    output.getName(),
                    "",
                    output.getFacets().getSchema().getFields(),
                    effectiveRunUuid)
                .getValue());

    RunBoundPresence effectivePresence = runBoundPresence(effectiveRunUuid);
    assertThat(effectivePresence)
        .isEqualTo(new RunBoundPresence(true, true, true, true, true, true, true));
    assertThat(runBoundPresence(requestedRunUuid))
        .isEqualTo(new RunBoundPresence(false, false, false, false, false, false, false));

    UUID outputField =
        datasetFieldDao
            .findUuid(projectedOutput.getDatasetRow().getUuid(), OUTPUT_COLUMN)
            .orElseThrow();
    assertThat(
            columnLineageDao.findColumnLineageByDatasetVersionAndOutputDatasetFields(
                projectedOutput.getDatasetVersionRow().getUuid(), List.of(outputField)))
        .extracting(row -> row.getInputDatasetVersionUuid())
        .containsExactly(projectedInput.getDatasetVersionRow().getUuid());
    assertThat(foreignRunSnapshot(requestedRunUuid)).isEqualTo(foreignBefore);
  }

  @Test
  void canonicalJobAliasScopesRepairToTheCanonicalIdentity() {
    String suffix = UUID.randomUUID().toString();
    NamespaceRow namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), NAMESPACE, getClass().getName());
    String primaryJobName = "run-primary-" + suffix;
    String aliasJobName = "run-alias-" + suffix;
    JobRow primary =
        createJobWithoutSymlinkTarget(jdbi, namespace, primaryJobName, "canonical run identity");
    createJobWithSymlinkTarget(
        jdbi, namespace, aliasJobName, primary.getUuid(), "canonical run alias");
    String runId = "alias-run-" + suffix;
    UUID normalRunUuid = Utils.openLineageRunUuid(runId);
    JobRow foreignJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, "alias-foreign-" + suffix, "foreign");
    RunArgsRow foreignArgs = DbTestUtils.newRunArgs(jdbi);
    runDao.upsert(
        normalRunUuid,
        null,
        "foreign-alias-external-" + suffix,
        Instant.parse("2026-08-14T02:30:00Z"),
        foreignJob.getUuid(),
        null,
        foreignArgs.getUuid(),
        null,
        null,
        foreignJob.getNamespaceName(),
        foreignJob.getName(),
        null);
    Instant eventTime = Instant.parse("2026-08-14T03:00:00Z");
    LineageEvent aliasEvent =
        runEvent(
            NAMESPACE,
            aliasJobName,
            runId,
            eventTime,
            "START",
            JobFacet.builder().build(),
            LineageEvent.RunFacet.builder().build(),
            Collections.emptyList(),
            Collections.emptyList());

    UpdateLineageRow projected = projectOrdered(aliasEvent, false);
    UUID expectedRepair =
        Utils.toNameBasedUuid(
            primary.getNamespaceName(),
            primary.getName(),
            Utils.openLineageRunUuid(runId).toString());

    assertThat(projected.getJob().getUuid()).isEqualTo(primary.getUuid());
    assertThat(projected.getRun().getUuid()).isEqualTo(expectedRepair);
    assertThat(projected.getRun().getJobUuid()).isEqualTo(primary.getUuid());
    assertThat(jobDao.lockJobByUuid(primary.getUuid()).getCurrentRunUuid())
        .contains(expectedRepair);
  }

  @Test
  void crossNamespaceAliasUsesCanonicalNamespaceForTerminalProjection() {
    String suffix = UUID.randomUUID().toString();
    NamespaceRow canonicalNamespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(),
            Instant.now(),
            "canonical-namespace-" + suffix,
            getClass().getName());
    NamespaceRow reportedNamespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), "reported-namespace-" + suffix, getClass().getName());
    String canonicalJobName = "canonical-job-" + suffix;
    String aliasJobName = "reported-job-" + suffix;
    JobRow canonicalJob =
        createJobWithoutSymlinkTarget(
            jdbi, canonicalNamespace, canonicalJobName, "canonical cross-namespace job");
    createJobWithSymlinkTarget(
        jdbi, reportedNamespace, aliasJobName, canonicalJob.getUuid(), "cross-namespace alias");
    Instant eventTime = Instant.parse("2026-08-14T05:00:00Z");
    LineageEvent event =
        runEvent(
            reportedNamespace.getName(),
            aliasJobName,
            "cross-namespace-run-" + suffix,
            eventTime,
            "COMPLETE",
            JobFacet.builder().build(),
            LineageEvent.RunFacet.builder().build(),
            Collections.emptyList(),
            Collections.emptyList());

    UpdateLineageRow projected = projectOrdered(event, false);
    UUID effectiveRunUuid = projected.getRun().getUuid();
    JobVersionRow jobVersion = projected.getJobVersionBag().getJobVersionRow();

    assertThat(projected.getJob().getUuid()).isEqualTo(canonicalJob.getUuid());
    assertThat(projected.getNamespace().getUuid()).isEqualTo(canonicalNamespace.getUuid());
    assertThat(projected.getNamespace().getName()).isEqualTo(canonicalNamespace.getName());
    assertThat(projected.getRun().getJobUuid()).isEqualTo(canonicalJob.getUuid());
    assertThat(projected.getRun().getNamespaceName()).isEqualTo(canonicalNamespace.getName());
    assertThat(jobDao.lockJobByUuid(canonicalJob.getUuid()).getCurrentRunUuid())
        .contains(effectiveRunUuid);
    assertThat(jobVersion.getJobUuid()).isEqualTo(canonicalJob.getUuid());
    assertThat(jobVersion.getNamespaceUuid()).isEqualTo(canonicalNamespace.getUuid());
    assertThat(jobVersion.getNamespaceName()).isEqualTo(canonicalNamespace.getName());
    assertThat(jdbi.onDemand(JobVersionDao.class).findLatestRunFor(jobVersion.getUuid()))
        .contains(effectiveRunUuid);
  }

  @Test
  void skipsUnneededStartSnapshotAndUsesCompactStateAndMissingIoWrites() {
    UUID runUuid = UUID.randomUUID();
    LineageEvent event =
        newRunEvent(
            "compact_start_" + runUuid,
            runUuid,
            "START",
            JobFacet.builder().build(),
            Collections.emptyList(),
            Collections.emptyList());
    List<String> executedSql = new ArrayList<>();

    UpdateLineageRow projected = projectWithSqlCapture(event, false, executedSql);

    assertThat(projected.getRunIoSnapshot()).isNull();
    assertThat(projected.getInputs()).isEmpty();
    assertThat(projected.getOutputs()).isEmpty();
    assertThat(countSql(executedSql, "FROM runs_input_mapping rim")).isZero();
    assertThat(countSql(executedSql, "WITH inserted_state AS")).isEqualTo(1);
    assertThat(countSql(executedSql, "io_type IN ('INPUT', 'OUTPUT')")).isEqualTo(1);
    assertThat(runDao.findRunByUuidAsRow(runUuid).orElseThrow().getStartRunStateUuid())
        .contains(projected.getRunState().getUuid());

    UpdateLineageRow missingIo =
        dao.updateMarquezModel(
            newRunEvent(
                "compact_missing_" + runUuid,
                UUID.randomUUID(),
                "START",
                JobFacet.builder().build(),
                null,
                null),
            Utils.getMapper(),
            false);
    assertThat(missingIo.getInputs()).isEmpty();
    assertThat(missingIo.getOutputs()).isEmpty();
  }

  @Test
  void terminalEventLoadsSnapshotWithoutListenerDemand() {
    UUID runUuid = UUID.randomUUID();
    LineageEvent event =
        newRunEvent(
            "terminal_snapshot_" + runUuid,
            runUuid,
            "COMPLETE",
            JobFacet.builder().build(),
            Collections.emptyList(),
            Collections.emptyList());

    UpdateLineageRow projected = dao.updateMarquezModel(event, Utils.getMapper(), false);

    assertThat(projected.getRunIoSnapshot()).isNotNull();
  }

  @Test
  void batchesPrimaryResolutionAndDatasetBasesOncePerSideInOccurrenceOrder() {
    String suffix = UUID.randomUUID().toString();
    String datasetNamespace = "base_batch_" + suffix;
    DatasetFacets emptyFacets = DatasetFacets.builder().build();
    List<Dataset> inputs =
        List.of(
            new Dataset(datasetNamespace, "input_a", emptyFacets),
            new Dataset(datasetNamespace, "input_b", emptyFacets),
            new Dataset(datasetNamespace, "input_c", emptyFacets),
            new Dataset(datasetNamespace, "input_d", emptyFacets));
    List<Dataset> outputs =
        List.of(
            new Dataset(datasetNamespace, "output_a", emptyFacets),
            new Dataset(datasetNamespace, "output_b", emptyFacets));
    LineageEvent event =
        newRunEvent(
            "base_batch_job_" + suffix,
            UUID.randomUUID(),
            "COMPLETE",
            JobFacet.builder().build(),
            inputs,
            outputs);
    List<String> executedSql = new ArrayList<>();

    UpdateLineageRow projected = projectWithSqlCapture(event, false, executedSql);

    assertThat(projected.getInputs().orElseThrow())
        .extracting(record -> record.getDatasetRow().getName())
        .containsExactly("input_a", "input_b", "input_c", "input_d");
    assertThat(projected.getOutputs().orElseThrow())
        .extracting(record -> record.getDatasetRow().getName())
        .containsExactly("output_a", "output_b");
    assertThat(
            countSql(
                executedSql,
                "WITH requested(dataset_uuid, name, namespace_uuid, created_at)",
                "INSERT INTO dataset_symlinks"))
        .isEqualTo(2);
    assertThat(countSql(executedSql, "WITH requested(", "INSERT INTO datasets")).isEqualTo(2);
    assertThat(countSql(executedSql, "INSERT INTO dataset_facets")).isZero();
  }

  @Test
  void symlinkFacetDisablesStagedBaseWritesForTheEntireSide() {
    String suffix = UUID.randomUUID().toString();
    String datasetNamespace = "alias_fallback_" + suffix;
    Dataset datasetWithEmptySymlinkFacet =
        new Dataset(
            datasetNamespace,
            "input_b",
            DatasetFacets.builder()
                .symlinks(
                    new LineageEvent.DatasetSymlinkFacet(
                        PRODUCER_URL, SCHEMA_URL, Collections.emptyList()))
                .build());
    LineageEvent event =
        newRunEvent(
            "alias_fallback_job_" + suffix,
            UUID.randomUUID(),
            "COMPLETE",
            JobFacet.builder().build(),
            List.of(new Dataset(datasetNamespace, "input_a", null), datasetWithEmptySymlinkFacet),
            Collections.emptyList());
    List<String> executedSql = new ArrayList<>();

    UpdateLineageRow projected = projectWithSqlCapture(event, false, executedSql);

    assertThat(projected.getInputs().orElseThrow())
        .extracting(record -> record.getDatasetRow().getName())
        .containsExactly("input_a", "input_b");
    assertThat(
            countSql(
                executedSql,
                "WITH requested(dataset_uuid, name, namespace_uuid, created_at)",
                "INSERT INTO dataset_symlinks"))
        .isZero();
    assertThat(countSql(executedSql, "WITH requested(", "INSERT INTO datasets")).isZero();
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
  void distinctPrimaryNamesResolvingToOneDatasetKeepSequentialOccurrenceSemantics() {
    String suffix = UUID.randomUUID().toString();
    String datasetNamespace = "resolved_alias_" + suffix;
    String primaryName = "primary_" + suffix;
    String aliasName = "alias_" + suffix;
    Dataset primaryWithAlias =
        new Dataset(
            datasetNamespace,
            primaryName,
            DatasetFacets.builder()
                .symlinks(
                    new LineageEvent.DatasetSymlinkFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        List.of(
                            new LineageEvent.SymlinkIdentifier(
                                datasetNamespace, aliasName, "alias"))))
                .build());
    UpdateLineageRow initial =
        LineageTestUtils.createLineageRow(
            dao,
            "resolved_alias_seed_" + suffix,
            "COMPLETE",
            JobFacet.builder().build(),
            Collections.emptyList(),
            List.of(primaryWithAlias));
    LineageEvent aliasRead =
        newRunEvent(
            "resolved_alias_read_" + suffix,
            UUID.randomUUID(),
            "COMPLETE",
            JobFacet.builder().build(),
            List.of(
                new Dataset(datasetNamespace, primaryName, null),
                new Dataset(datasetNamespace, aliasName, null)),
            Collections.emptyList());
    List<String> executedSql = new ArrayList<>();

    UpdateLineageRow projected = projectWithSqlCapture(aliasRead, false, executedSql);

    UUID datasetUuid = initial.getOutputs().orElseThrow().get(0).getDatasetRow().getUuid();
    UUID datasetVersionUuid =
        initial.getOutputs().orElseThrow().get(0).getDatasetVersionRow().getUuid();
    assertThat(projected.getInputs().orElseThrow())
        .extracting(record -> record.getDatasetRow().getUuid())
        .containsExactly(datasetUuid, datasetUuid);
    assertThat(projected.getInputs().orElseThrow())
        .extracting(record -> record.getDatasetVersionRow().getUuid())
        .containsExactly(datasetVersionUuid, datasetVersionUuid);
    assertThat(countSql(executedSql, "WITH requested(", "INSERT INTO datasets")).isZero();
  }

  @Test
  void orderedCanonicalDatasetUsesLastPayloadOccurrenceAndKeepsImmutableVersions() {
    String suffix = UUID.randomUUID().toString();
    String datasetNamespace = "ordered_duplicate_" + suffix;
    String primaryName = "primary_" + suffix;
    String aliasName = "alias_" + suffix;
    Dataset seed =
        new Dataset(
            datasetNamespace,
            primaryName,
            DatasetFacets.builder()
                .symlinks(
                    new LineageEvent.DatasetSymlinkFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        List.of(
                            new LineageEvent.SymlinkIdentifier(
                                datasetNamespace, aliasName, "alias"))))
                .build());
    UpdateLineageRow initial =
        LineageTestUtils.createLineageRow(
            dao,
            "ordered_duplicate_seed_" + suffix,
            "COMPLETE",
            JobFacet.builder().build(),
            Collections.emptyList(),
            List.of(seed));
    UUID datasetUuid = initial.getOutputs().orElseThrow().get(0).getDatasetRow().getUuid();

    Dataset first =
        new Dataset(
            datasetNamespace,
            primaryName,
            DatasetFacets.builder()
                .documentation(
                    new LineageEvent.DocumentationDatasetFacet(
                        PRODUCER_URL, SCHEMA_URL, "first description"))
                .schema(
                    new SchemaDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        List.of(new SchemaField("first", "STRING", "first field"))))
                .build());
    Dataset last =
        new Dataset(
            datasetNamespace,
            aliasName,
            DatasetFacets.builder()
                .documentation(
                    new LineageEvent.DocumentationDatasetFacet(
                        PRODUCER_URL, SCHEMA_URL, "last description"))
                .schema(
                    new SchemaDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        List.of(new SchemaField("last", "INT", "last field"))))
                .build());
    Instant eventTime =
        initial.getOutputs().orElseThrow().get(0).getDatasetRow().getUpdatedAt().plusSeconds(1);
    LineageEvent event =
        LineageEvent.builder()
            .eventType("COMPLETE")
            .eventTime(eventTime.atZone(LineageTestUtils.LOCAL_ZONE))
            .run(new LineageEvent.Run(UUID.randomUUID().toString(), null))
            .job(new Job(NAMESPACE, "ordered_duplicate_job_" + suffix, JobFacet.builder().build()))
            .inputs(Collections.emptyList())
            .outputs(List.of(first, last))
            .producer(PRODUCER_URL.toString())
            .build();
    UpdateLineageRow projected =
        dao.updateMarquezModel(
            event,
            Utils.getMapper(),
            false,
            new ProjectionOrder(eventTime, Utils.sha256Utf8(Utils.toJson(event))));

    assertThat(projected.getOutputs().orElseThrow()).hasSize(2);
    assertThat(projected.getOutputs().orElseThrow())
        .extracting(record -> record.getDatasetRow().getUuid())
        .containsExactly(datasetUuid, datasetUuid);
    assertThat(projected.getOutputs().orElseThrow())
        .extracting(record -> record.getDatasetVersionRow().getUuid())
        .doesNotHaveDuplicates();
    DatasetRow canonical = datasetDao.findDatasetAsRow(datasetNamespace, primaryName).orElseThrow();
    assertThat(canonical.getPhysicalName()).isEqualTo(aliasName);
    assertThat(canonical.getDescription()).contains("last description");
    assertThat(canonical.getCurrentVersionUuid())
        .contains(projected.getOutputs().orElseThrow().get(1).getDatasetVersionRow().getUuid());

    Instant reverseTime = eventTime.plusSeconds(1);
    LineageEvent reverse =
        LineageEvent.builder()
            .eventType("COMPLETE")
            .eventTime(reverseTime.atZone(LineageTestUtils.LOCAL_ZONE))
            .run(new LineageEvent.Run(UUID.randomUUID().toString(), null))
            .job(
                new Job(
                    NAMESPACE, "ordered_duplicate_reverse_" + suffix, JobFacet.builder().build()))
            .inputs(Collections.emptyList())
            .outputs(List.of(last, first))
            .producer(PRODUCER_URL.toString())
            .build();
    UpdateLineageRow reversed =
        dao.updateMarquezModel(
            reverse,
            Utils.getMapper(),
            false,
            new ProjectionOrder(reverseTime, Utils.sha256Utf8(Utils.toJson(reverse))));
    DatasetRow reversedCanonical =
        datasetDao.findDatasetAsRow(datasetNamespace, primaryName).orElseThrow();
    assertThat(reversedCanonical.getPhysicalName()).isEqualTo(primaryName);
    assertThat(reversedCanonical.getDescription()).contains("first description");
    assertThat(reversedCanonical.getCurrentVersionUuid())
        .contains(reversed.getOutputs().orElseThrow().get(1).getDatasetVersionRow().getUuid());

    Instant crossSideTime = reverseTime.plusSeconds(1);
    LineageEvent crossSide =
        LineageEvent.builder()
            .eventType("COMPLETE")
            .eventTime(crossSideTime.atZone(LineageTestUtils.LOCAL_ZONE))
            .run(new LineageEvent.Run(UUID.randomUUID().toString(), null))
            .job(
                new Job(
                    NAMESPACE,
                    "ordered_duplicate_cross_side_" + suffix,
                    JobFacet.builder().build()))
            .inputs(List.of(first))
            .outputs(List.of(last))
            .producer(PRODUCER_URL.toString())
            .build();
    UpdateLineageRow crossSideProjection =
        dao.updateMarquezModel(
            crossSide,
            Utils.getMapper(),
            false,
            new ProjectionOrder(crossSideTime, Utils.sha256Utf8(Utils.toJson(crossSide))));

    DatasetRow crossSideCanonical =
        datasetDao.findDatasetAsRow(datasetNamespace, primaryName).orElseThrow();
    assertThat(crossSideCanonical.getPhysicalName()).isEqualTo(aliasName);
    assertThat(crossSideCanonical.getDescription()).contains("last description");
    assertThat(crossSideCanonical.getCurrentVersionUuid())
        .contains(
            crossSideProjection.getOutputs().orElseThrow().get(0).getDatasetVersionRow().getUuid());
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

    UpdateLineageRow missingIo =
        LineageTestUtils.createLineageRow(
            dao,
            new Job(NAMESPACE, "missing_job_io_" + UUID.randomUUID(), JobFacet.builder().build()),
            null,
            null);
    assertThat(missingIo.getInputs()).isPresent();
    assertThat(missingIo.getInputs().orElseThrow()).isEmpty();
    assertThat(missingIo.getOutputs()).isPresent();
    assertThat(missingIo.getOutputs().orElseThrow()).isEmpty();
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

  private UpdateLineageRow projectWithSqlCapture(
      LineageEvent event, boolean listenerSnapshotRequired, List<String> executedSql) {
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
          projected[0] =
              handle
                  .attach(OpenLineageDao.class)
                  .updateMarquezModel(event, Utils.getMapper(), listenerSnapshotRequired);
        });
    return projected[0];
  }

  private static LineageEvent newRunEvent(
      String jobName,
      UUID runUuid,
      String eventType,
      JobFacet jobFacet,
      List<Dataset> inputs,
      List<Dataset> outputs) {
    return LineageEvent.builder()
        .eventType(eventType)
        .eventTime(Instant.now().atZone(LineageTestUtils.LOCAL_ZONE))
        .run(new LineageEvent.Run(runUuid.toString(), null))
        .job(new Job(NAMESPACE, jobName, jobFacet))
        .inputs(inputs)
        .outputs(outputs)
        .producer(PRODUCER_URL.toString())
        .build();
  }

  private static LineageEvent runEvent(
      String namespace,
      String jobName,
      String runId,
      Instant eventTime,
      String eventType,
      JobFacet jobFacet,
      LineageEvent.RunFacet runFacet,
      List<Dataset> inputs,
      List<Dataset> outputs) {
    return LineageEvent.builder()
        .eventType(eventType)
        .eventTime(eventTime.atZone(LineageTestUtils.LOCAL_ZONE))
        .run(new LineageEvent.Run(runId, runFacet))
        .job(new Job(namespace, jobName, jobFacet))
        .inputs(inputs)
        .outputs(outputs)
        .producer(PRODUCER_URL.toString())
        .build();
  }

  private UpdateLineageRow projectOrdered(LineageEvent event, boolean listenerSnapshotRequired) {
    Instant eventTime = event.getEventTime().toInstant();
    return dao.updateMarquezModel(
        event,
        Utils.getMapper(),
        listenerSnapshotRequired,
        new ProjectionOrder(eventTime, Utils.sha256Utf8(Utils.toJson(event))));
  }

  private boolean isOpenLineageParentPlaceholder(UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT open_lineage_parent_placeholder IS TRUE FROM runs WHERE uuid = :uuid")
                .bind("uuid", runUuid)
                .mapTo(Boolean.class)
                .one());
  }

  private ForeignRunSnapshot foreignRunSnapshot(UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT xmin::text AS row_version, row_to_json(r)::text AS row_json
                    FROM runs AS r
                    WHERE uuid = :uuid
                    """)
                .bind("uuid", runUuid)
                .map(
                    (resultSet, context) ->
                        new ForeignRunSnapshot(
                            resultSet.getString("row_version"), resultSet.getString("row_json")))
                .one());
  }

  private RunBoundPresence runBoundPresence(UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT
                      EXISTS (SELECT 1 FROM run_states WHERE run_uuid = :uuid) AS run_state,
                      EXISTS (SELECT 1 FROM run_facets
                              WHERE run_uuid = :uuid AND name = 'customRun') AS run_facet,
                      EXISTS (SELECT 1 FROM job_facets
                              WHERE run_uuid = :uuid AND name = 'customJob') AS job_facet,
                      EXISTS (SELECT 1 FROM dataset_facets WHERE run_uuid = :uuid) AS dataset_facet,
                      EXISTS (SELECT 1 FROM runs_input_mapping WHERE run_uuid = :uuid) AS input_map,
                      EXISTS (SELECT 1 FROM dataset_versions WHERE run_uuid = :uuid) AS output_version,
                      EXISTS (SELECT 1 FROM job_versions WHERE latest_run_uuid = :uuid) AS job_version
                    """)
                .bind("uuid", runUuid)
                .map(
                    (resultSet, context) ->
                        new RunBoundPresence(
                            resultSet.getBoolean("run_state"),
                            resultSet.getBoolean("run_facet"),
                            resultSet.getBoolean("job_facet"),
                            resultSet.getBoolean("dataset_facet"),
                            resultSet.getBoolean("input_map"),
                            resultSet.getBoolean("output_version"),
                            resultSet.getBoolean("job_version")))
                .one());
  }

  private record ForeignRunSnapshot(String rowVersion, String rowJson) {}

  private record RunBoundPresence(
      boolean runState,
      boolean runFacet,
      boolean jobFacet,
      boolean datasetFacet,
      boolean inputMapping,
      boolean outputVersion,
      boolean jobVersion) {}

  private static long countSql(List<String> executedSql, String... requiredFragments) {
    return executedSql.stream()
        .filter(sql -> Arrays.stream(requiredFragments).allMatch(sql::contains))
        .count();
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

  @Test
  void orderedAliasJobEventCasProjectsAndReadsTheCanonicalWinner() {
    String suffix = UUID.randomUUID().toString();
    NamespaceRow namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), NAMESPACE, getClass().getName());
    String primaryJobName = "ordered_primary_" + suffix;
    String aliasJobName = "ordered_alias_" + suffix;
    JobRow primary =
        createJobWithoutSymlinkTarget(jdbi, namespace, primaryJobName, "initial description");
    createJobWithSymlinkTarget(jdbi, namespace, aliasJobName, primary.getUuid(), "alias row");

    Instant winnerTime = primary.getUpdatedAt().plusSeconds(1);
    Dataset winnerInput = new Dataset("ordered_alias_dataset_" + suffix, "winner", datasetFacets);
    JobEvent winner =
        jobEvent(aliasJobName, winnerTime, "winner description", List.of(winnerInput));
    ProjectionOrder winnerOrder =
        new ProjectionOrder(winnerTime, Utils.sha256Utf8(Utils.toJson(winner)));
    UpdateLineageRow projected = dao.updateMarquezModel(winner, Utils.getMapper(), winnerOrder);

    assertThat(projected.getJob().getUuid()).isEqualTo(primary.getUuid());
    JobRow canonical = jobDao.lockJobByUuid(primary.getUuid());
    assertThat(canonical.getDescription()).contains("winner description");
    assertThat(canonical.getInputs())
        .extracting(dataset -> dataset.getName().getValue())
        .containsExactly("winner");
    UUID winningVersion = canonical.getCurrentVersionUuid().orElseThrow();

    Instant staleTime = winnerTime.minusSeconds(1);
    JobEvent stale =
        jobEvent(primaryJobName, staleTime, "stale description", Collections.emptyList());
    dao.updateMarquezModel(
        stale,
        Utils.getMapper(),
        new ProjectionOrder(staleTime, Utils.sha256Utf8(Utils.toJson(stale))));

    JobRow afterStale = jobDao.lockJobByUuid(primary.getUuid());
    assertThat(afterStale.getDescription()).contains("winner description");
    assertThat(afterStale.getCurrentVersionUuid()).contains(winningVersion);
    assertThat(afterStale.getInputs())
        .extracting(dataset -> dataset.getName().getValue())
        .containsExactly("winner");
  }

  private static JobEvent jobEvent(
      String jobName, Instant eventTime, String description, List<Dataset> inputs) {
    return JobEvent.builder()
        .eventTime(eventTime.atZone(LineageTestUtils.LOCAL_ZONE))
        .job(
            new Job(
                NAMESPACE,
                jobName,
                JobFacet.builder()
                    .documentation(DocumentationJobFacet.builder().description(description).build())
                    .build()))
        .inputs(inputs)
        .outputs(Collections.emptyList())
        .producer(PRODUCER_URL.toString())
        .build();
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
    return getInputDataset(INPUT_NAMESPACE, INPUT_DATASET);
  }

  private Dataset getInputDataset(String namespace, String name) {
    return new Dataset(
        namespace,
        name,
        LineageEvent.DatasetFacets.builder()
            .schema(
                new SchemaDatasetFacet(
                    PRODUCER_URL,
                    SCHEMA_URL,
                    Arrays.asList(new SchemaField(INPUT_FIELD_NAME, "STRING", "my name"))))
            .build());
  }

  private Dataset getOutputDatasetWithColumnLineage() {
    return getOutputDatasetWithColumnLineage(
        NAMESPACE, DATASET_NAME, INPUT_NAMESPACE, INPUT_DATASET);
  }

  private Dataset getOutputDatasetWithColumnLineage(
      String namespace, String name, String inputName) {
    return getOutputDatasetWithColumnLineage(namespace, name, namespace, inputName);
  }

  private Dataset getOutputDatasetWithColumnLineage(
      String namespace, String name, String inputNamespace, String inputName) {
    return new Dataset(
        namespace,
        name,
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
                                        inputNamespace, inputName, INPUT_FIELD_NAME)),
                                TRANSFORMATION_DESCRIPTION,
                                TRANSFORMATION_TYPE)))))
            .build());
  }
}
