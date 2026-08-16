/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.DbTestUtils.createJobWithSymlinkTarget;
import static marquez.db.DbTestUtils.createJobWithoutSymlinkTarget;
import static marquez.db.LineageTestUtils.PRODUCER_URL;
import static marquez.db.LineageTestUtils.SCHEMA_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import marquez.common.Utils;
import marquez.db.OpenLineageProjector.DatasetProjection;
import marquez.db.OpenLineageProjector.DatasetProjectionResult;
import marquez.db.OpenLineageProjector.JobProjectionResult;
import marquez.db.OpenLineageProjector.ProjectionRequest;
import marquez.db.OpenLineageProjector.ProjectionResult;
import marquez.db.OpenLineageProjector.RunProjectionResult;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetSymlinkRow;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.Dataset;
import marquez.service.models.LineageEvent.DatasetFacets;
import marquez.service.models.LineageEvent.DocumentationJobFacet;
import marquez.service.models.LineageEvent.JobFacet;
import marquez.service.models.LineageEvent.JobTypeJobFacet;
import marquez.service.models.LineageEvent.RunFacet;
import marquez.service.models.LineageEvent.SchemaDatasetFacet;
import marquez.service.models.LineageEvent.SchemaField;
import org.assertj.core.groups.Tuple;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class OpenLineageProjectorTest {
  private static final String NAMESPACE = "projector_namespace";
  private static final Instant EVENT_TIME = Instant.parse("2026-08-16T00:00:00Z");
  private static final String INPUT_FIELD = "input_field";
  private static final String OUTPUT_FIELD = "output_field";

  private static Jdbi jdbi;
  private static JobDao jobDao;
  private static DatasetDao datasetDao;
  private static DatasetSymlinkDao datasetSymlinkDao;
  private static DatasetFieldDao datasetFieldDao;
  private static ColumnLineageDao columnLineageDao;
  private static NamespaceDao namespaceDao;

  @BeforeAll
  static void setUpOnce(Jdbi configuredJdbi) {
    jdbi = configuredJdbi;
    jobDao = jdbi.onDemand(JobDao.class);
    datasetDao = jdbi.onDemand(DatasetDao.class);
    datasetSymlinkDao = jdbi.onDemand(DatasetSymlinkDao.class);
    datasetFieldDao = jdbi.onDemand(DatasetFieldDao.class);
    columnLineageDao = jdbi.onDemand(ColumnLineageDao.class);
    namespaceDao = jdbi.onDemand(NamespaceDao.class);
  }

  @Test
  void alignsMixedResultsWithRequestsAndReturnsImmutableCollections() {
    String suffix = UUID.randomUUID().toString();
    DatasetEvent datasetEvent = datasetEvent(0, dataset("mixed_dataset_" + suffix, "field"));
    JobEvent jobEvent = jobEvent("mixed_job_" + suffix, 1, "mixed", List.of());
    UUID parentRunUuid = UUID.randomUUID();
    String parentJobName = "mixed_parent_" + suffix;
    LineageEvent runEvent =
        runEvent(
            "mixed_run_job_" + suffix,
            UUID.randomUUID(),
            2,
            "START",
            JobFacet.builder().build(),
            RunFacet.builder().parent(parentFacet(parentRunUuid, parentJobName)).build(),
            List.of(dataset("mixed_input_" + suffix, INPUT_FIELD)),
            List.of());
    LineageEvent parentEvent = runEvent(parentJobName, parentRunUuid, 3, "START", null, null);
    List<ProjectionRequest> requests =
        requests(false, datasetEvent, jobEvent, runEvent, parentEvent);

    List<ProjectionResult> results = project(requests);

    assertThat(results).hasSize(4);
    assertThat(results.get(0)).isInstanceOf(DatasetProjectionResult.class);
    assertThat(results.get(1)).isInstanceOf(JobProjectionResult.class);
    assertThat(results.get(2)).isInstanceOf(RunProjectionResult.class);
    assertThat(results.get(3)).isInstanceOf(RunProjectionResult.class);
    assertThat(results).extracting(ProjectionResult::request).containsExactlyElementsOf(requests);
    assertThat(((RunProjectionResult) results.get(2)).run().getParentRunUuid())
        .contains(parentRunUuid);
    assertThat(((RunProjectionResult) results.get(3)).run().getUuid()).isEqualTo(parentRunUuid);

    DatasetProjectionResult datasetResult = (DatasetProjectionResult) results.get(0);
    JobProjectionResult jobResult = (JobProjectionResult) results.get(1);
    RunProjectionResult runResult = (RunProjectionResult) results.get(2);
    assertThat(
            List.<List<?>>of(
                results,
                datasetResult.outputs(),
                jobResult.inputs().orElseThrow(),
                runResult.inputs().orElseThrow()))
        .allSatisfy(
            collection ->
                assertThatThrownBy(() -> collection.add(null))
                    .isInstanceOf(UnsupportedOperationException.class));
  }

  @Test
  void runIoMissingRetainsEmptyClearsAndNonemptyReplacesPerEventSnapshots() {
    String suffix = UUID.randomUUID().toString();
    UUID runUuid = UUID.randomUUID();
    String jobName = "run_io_" + suffix;
    LineageEvent initial =
        runEvent(
            jobName,
            runUuid,
            0,
            "START",
            List.of(dataset("input_a_" + suffix, INPUT_FIELD)),
            List.of(dataset("output_a_" + suffix, OUTPUT_FIELD)));
    LineageEvent missing = runEvent(jobName, runUuid, 1, "RUNNING", null, null);
    LineageEvent lineage =
        runEvent(
            jobName,
            runUuid,
            2,
            "RUNNING",
            null,
            List.of(outputWithColumnLineage("lineage_output_" + suffix, "input_a_" + suffix)));
    LineageEvent clear = runEvent(jobName, runUuid, 3, "RUNNING", List.of(), List.of());
    LineageEvent replace =
        runEvent(
            jobName,
            runUuid,
            4,
            "RUNNING",
            List.of(dataset("input_b_" + suffix, INPUT_FIELD)),
            List.of(dataset("output_b_" + suffix, OUTPUT_FIELD)));

    List<RunProjectionResult> results =
        projectRuns(true, initial, missing, lineage, clear, replace);

    assertThat(results)
        .extracting(
            result -> result.inputs().map(List::size).orElse(null),
            result -> result.outputs().map(List::size).orElse(null))
        .containsExactly(
            Tuple.tuple(1, 1),
            Tuple.tuple(null, null),
            Tuple.tuple(null, 1),
            Tuple.tuple(0, 0),
            Tuple.tuple(1, 1));

    assertSnapshot(results.get(0), List.of("input_a_" + suffix), List.of("output_a_" + suffix));
    assertSnapshot(results.get(1), List.of("input_a_" + suffix), List.of("output_a_" + suffix));
    assertSnapshot(
        results.get(2), List.of("input_a_" + suffix), List.of("lineage_output_" + suffix));
    assertSnapshot(results.get(3), List.of(), List.of());
    assertSnapshot(results.get(4), List.of("input_b_" + suffix), List.of("output_b_" + suffix));
    assertColumnLineage(results.get(0), results.get(2));
  }

  @Test
  void lastWriteWinsRejectsStaleEventsAndBreaksTimestampTiesByExactJsonHash() {
    String staleSuffix = UUID.randomUUID().toString();
    String staleJobName = "lww_stale_" + staleSuffix;
    JobEvent newer =
        jobEvent(
            staleJobName,
            10,
            "newer description",
            List.of(dataset("newer_input_" + staleSuffix, INPUT_FIELD)));
    JobEvent stale = jobEvent(staleJobName, 9, "stale description", List.of());
    List<ProjectionRequest> staleRequests = List.of(request(newer, false), request(stale, false));

    List<ProjectionResult> staleResults = project(staleRequests);

    assertThat(staleResults)
        .extracting(ProjectionResult::request)
        .containsExactlyElementsOf(staleRequests);
    JobRow afterStale = jobDao.findJobByNameAsRow(NAMESPACE, staleJobName).orElseThrow();
    assertThat(afterStale.getDescription()).contains("newer description");
    assertThat(afterStale.getInputs())
        .extracting(input -> input.getName().getValue())
        .containsExactly("newer_input_" + staleSuffix);

    String tieSuffix = UUID.randomUUID().toString();
    String tieJobName = "lww_tie_" + tieSuffix;
    JobEvent first = jobEvent(tieJobName, 20, "tie-a", null);
    JobEvent second = jobEvent(tieJobName, 20, "tie-b", null);
    ProjectionRequest firstRequest = request(first, false);
    ProjectionRequest secondRequest = request(second, false);
    ProjectionRequest tieWinner = laterRequest(firstRequest, secondRequest);
    ProjectionRequest tieLoser = tieWinner == firstRequest ? secondRequest : firstRequest;

    project(List.of(tieWinner, tieLoser));

    JobRow afterTie = jobDao.findJobByNameAsRow(NAMESPACE, tieJobName).orElseThrow();
    DocumentationJobFacet winningDocumentation =
        ((JobEvent) tieWinner.event()).getJob().getFacets().getDocumentation();
    assertThat(afterTie.getDescription()).contains(winningDocumentation.getDescription());
  }

  @ParameterizedTest(name = "terminal CAS: {0}")
  @CsvSource({"stale event,101,100", "exact JSON hash tie,110,110"})
  void terminalCasRejectsStaleEventsAndBreaksTies(
      String scenario, long firstOffset, long secondOffset) {
    String suffix = UUID.randomUUID().toString();
    UUID runUuid = UUID.randomUUID();
    String jobName = "terminal_" + scenario.replace(' ', '_') + "_" + suffix;
    String firstLocation = "https://example.com/first/" + suffix;
    String secondLocation = "https://example.com/second/" + suffix;
    String firstFacet = "terminalFirst_" + suffix.replace('-', '_');
    String secondFacet = "terminalSecond_" + suffix.replace('-', '_');
    LineageEvent first =
        terminalEvent(
            jobName, runUuid, firstOffset, firstLocation, firstFacet, "terminal_first_" + suffix);
    LineageEvent second =
        terminalEvent(
            jobName,
            runUuid,
            secondOffset,
            secondLocation,
            secondFacet,
            "terminal_second_" + suffix);
    ProjectionRequest firstRequest = request(first, true);
    ProjectionRequest secondRequest = request(second, true);
    ProjectionRequest winner =
        firstOffset > secondOffset ? firstRequest : laterRequest(firstRequest, secondRequest);
    ProjectionRequest loser = winner == firstRequest ? secondRequest : firstRequest;

    List<RunProjectionResult> results = projectRuns(List.of(winner, loser));

    assertThat(results.get(0).jobVersion()).isNotNull();
    assertThat(results.get(1).jobVersion()).isNull();
    boolean firstWins = winner == firstRequest;
    assertTerminalLinkage(
        runUuid,
        results.get(0).jobVersion().jobVersion().getUuid(),
        firstWins ? firstLocation : secondLocation,
        firstWins ? secondLocation : firstLocation,
        firstWins ? firstFacet : secondFacet,
        firstWins ? secondFacet : firstFacet);
  }

  @Test
  void canonicalizesEveryAliasOccurrenceAcrossTheWholeBatch() {
    String suffix = UUID.randomUUID().toString();
    NamespaceRow namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), NAMESPACE, OpenLineageProjectorTest.class.getName());
    String primaryName = "batch_primary_" + suffix;
    String aliasName = "batch_alias_" + suffix;
    JobRow primary =
        createJobWithoutSymlinkTarget(jdbi, namespace, primaryName, "canonical batch job");
    createJobWithSymlinkTarget(jdbi, namespace, aliasName, primary.getUuid(), "batch alias");
    Instant eventTime = Instant.now().plusSeconds(1);
    JobEvent aliasEvent =
        jobEvent(
            aliasName,
            eventTime,
            "alias occurrence",
            List.of(dataset("alias_input_" + suffix, INPUT_FIELD)));
    JobEvent primaryEvent =
        jobEvent(
            primaryName,
            eventTime.plusSeconds(1),
            "primary occurrence",
            List.of(dataset("primary_input_" + suffix, INPUT_FIELD)));
    List<ProjectionRequest> requests =
        List.of(request(aliasEvent, false), request(primaryEvent, false));

    List<ProjectionResult> results = project(requests);

    assertThat(results).allMatch(JobProjectionResult.class::isInstance);
    assertThat(results)
        .extracting(result -> ((JobProjectionResult) result).job().getUuid())
        .containsExactly(primary.getUuid(), primary.getUuid());
    assertThat(results).extracting(ProjectionResult::request).containsExactlyElementsOf(requests);
    JobRow canonical = jobDao.lockJobByUuid(primary.getUuid());
    assertThat(canonical.getInputs())
        .extracting(input -> input.getName().getValue())
        .containsExactly("primary_input_" + suffix);
  }

  @Test
  void sanitizedPayloadPrimaryDoesNotDisplaceAnExistingRawNamespacePrimary() {
    String suffix = UUID.randomUUID().toString();
    String rawNamespace = "dataset://warehouse;tenant=" + suffix;
    String normalizedNamespace = Utils.sanitizeOpenLineageNamespace(rawNamespace);
    String datasetName = "protected_raw_primary_" + suffix;
    assertThat(normalizedNamespace).isNotEqualTo(rawNamespace);
    DbTestUtils.newDataset(jdbi, rawNamespace, datasetName);
    DatasetRow rawPrimaryBefore =
        datasetDao.findDatasetAsRow(rawNamespace, datasetName).orElseThrow();
    DatasetEvent payload = datasetEvent(25, dataset(rawNamespace, datasetName, OUTPUT_FIELD));

    DatasetProjectionResult result =
        (DatasetProjectionResult) project(List.of(request(payload, false))).get(0);

    DatasetRow rawPrimaryAfter =
        datasetDao.findDatasetAsRow(rawNamespace, datasetName).orElseThrow();
    DatasetRow normalizedPrimary =
        datasetDao.findDatasetAsRow(normalizedNamespace, datasetName).orElseThrow();
    assertThat(rawPrimaryAfter).isEqualTo(rawPrimaryBefore);
    assertThat(normalizedPrimary.getUuid())
        .isEqualTo(result.outputs().get(0).dataset().getUuid())
        .isNotEqualTo(rawPrimaryBefore.getUuid());

    DatasetSymlinkRow rawPrimarySymlink = symlink(rawNamespace, datasetName);
    assertThat(rawPrimarySymlink.isPrimary()).isTrue();
    assertThat(rawPrimarySymlink.getUuid()).isEqualTo(rawPrimaryBefore.getUuid());
  }

  @Test
  void explicitAliasCollisionCannotMergeOrOverwriteAnotherPayloadPrimary() {
    String suffix = UUID.randomUUID().toString();
    String sourceName = "protected_alias_source_" + suffix;
    String protectedPrimaryName = "protected_alias_target_" + suffix;
    String ordinaryAliasName = "ordinary_alias_" + suffix;
    DbTestUtils.newDataset(jdbi, NAMESPACE, protectedPrimaryName);
    UUID protectedPrimaryUuid =
        datasetDao.findDatasetAsRow(NAMESPACE, protectedPrimaryName).orElseThrow().getUuid();
    Dataset source =
        new Dataset(
            NAMESPACE,
            sourceName,
            DatasetFacets.builder()
                .schema(schema(OUTPUT_FIELD))
                .symlinks(
                    new LineageEvent.DatasetSymlinkFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        List.of(
                            new LineageEvent.SymlinkIdentifier(
                                NAMESPACE, protectedPrimaryName, "TABLE"),
                            new LineageEvent.SymlinkIdentifier(
                                NAMESPACE, ordinaryAliasName, "TABLE"))))
                .build());
    Dataset protectedPrimary = dataset(protectedPrimaryName, OUTPUT_FIELD);
    DatasetEvent sourceEvent = datasetEvent(26, source);
    DatasetEvent protectedEvent = datasetEvent(27, protectedPrimary);

    List<ProjectionResult> projected =
        project(List.of(request(sourceEvent, false), request(protectedEvent, false)));

    DatasetProjectionResult sourceResult = (DatasetProjectionResult) projected.get(0);
    DatasetProjectionResult protectedResult = (DatasetProjectionResult) projected.get(1);
    UUID sourceUuid = sourceResult.outputs().get(0).dataset().getUuid();
    assertThat(sourceUuid).isNotEqualTo(protectedPrimaryUuid);
    assertThat(protectedResult.outputs().get(0).dataset().getUuid())
        .isEqualTo(protectedPrimaryUuid);
    assertThat(datasetDao.findDatasetAsRow(NAMESPACE, protectedPrimaryName).orElseThrow().getUuid())
        .isEqualTo(protectedPrimaryUuid);

    DatasetSymlinkRow protectedSymlink = symlink(NAMESPACE, protectedPrimaryName);
    DatasetSymlinkRow ordinaryAlias = symlink(NAMESPACE, ordinaryAliasName);
    assertThat(protectedSymlink.isPrimary()).isTrue();
    assertThat(protectedSymlink.getUuid()).isEqualTo(protectedPrimaryUuid);
    assertThat(ordinaryAlias.isPrimary()).isFalse();
    assertThat(ordinaryAlias.getUuid()).isEqualTo(sourceUuid);
  }

  @Test
  void duplicateEventsRemainAlignedAndPersistEachFacetOccurrence() {
    String suffix = UUID.randomUUID().toString();
    UUID runUuid = UUID.randomUUID();
    JobFacet jobFacet = JobFacet.builder().build();
    jobFacet.setFacet("customJob", Map.of("value", suffix));
    RunFacet runFacet = RunFacet.builder().build();
    runFacet.setFacet("customRun", Map.of("value", suffix));
    Dataset output =
        new Dataset(
            NAMESPACE,
            "duplicate_output_" + suffix,
            DatasetFacets.builder()
                .schema(schema(OUTPUT_FIELD))
                .additional(Map.of("customDataset", Map.of("value", suffix)))
                .build());
    LineageEvent event =
        runEvent(
            "duplicate_job_" + suffix,
            runUuid,
            30,
            "COMPLETE",
            jobFacet,
            runFacet,
            List.of(),
            List.of(output));
    ProjectionRequest request = request(event, true);

    List<RunProjectionResult> results = projectRuns(List.of(request, request));

    assertThat(results).hasSize(2);
    assertThat(results).extracting(RunProjectionResult::request).containsExactly(request, request);
    assertThat(results).extracting(result -> result.run().getUuid()).containsOnly(runUuid);
    UUID projectedVersion = results.get(0).outputs().orElseThrow().get(0).version().getUuid();
    assertThat(results)
        .extracting(result -> result.outputs().orElseThrow().get(0).version().getUuid())
        .containsExactly(projectedVersion, projectedVersion);
    assertThat(facetCount("run_facets", runUuid, "customRun")).isEqualTo(2);
    assertThat(facetCount("job_facets", runUuid, "customJob")).isEqualTo(2);
    assertThat(facetCount("dataset_facets", runUuid, "customDataset")).isEqualTo(2);
  }

  @Test
  void winningSkippedStreamingTerminalHydratesVersionAndRejectsOlderRelink() {
    String suffix = UUID.randomUUID().toString();
    UUID runUuid = UUID.randomUUID();
    String outputName = "stream_output_" + suffix;
    JobFacet streamingFacet =
        JobFacet.builder()
            .jobType(
                JobTypeJobFacet.builder()
                    .processingType("STREAMING")
                    .integration("FLINK")
                    .jobType("JOB")
                    .build())
            .build();
    LineageEvent running =
        runEvent(
            "stream_job_" + suffix,
            runUuid,
            50,
            "RUNNING",
            streamingFacet,
            RunFacet.builder().build(),
            List.of(),
            List.of(dataset(outputName, OUTPUT_FIELD)));
    LineageEvent complete =
        runEvent(
            "stream_job_" + suffix,
            runUuid,
            52,
            "COMPLETE",
            streamingFacet,
            RunFacet.builder().build(),
            null,
            null);
    LineageEvent olderComplete =
        runEvent(
            "stream_job_" + suffix,
            runUuid,
            51,
            "COMPLETE",
            streamingFacet,
            RunFacet.builder().build(),
            List.of(),
            List.of(dataset("stale_stream_output_" + suffix, OUTPUT_FIELD)));
    List<RunProjectionResult> results = projectRuns(false, running, complete, olderComplete);
    ProjectionRequest completeRequest = results.get(1).request();

    assertThat(results.get(0).jobVersion()).isNotNull();
    assertThat(results.get(1).jobVersion()).isNotNull();
    assertThat(results.get(2).jobVersion()).isNull();
    assertThat(results.get(1).inputs()).isEmpty();
    assertThat(results.get(1).outputs()).isEmpty();
    assertSnapshot(results.get(1), List.of(), List.of(outputName));
    UUID effectiveVersion = results.get(0).jobVersion().jobVersion().getUuid();
    assertThat(results.get(1).jobVersion().jobVersion().getUuid()).isEqualTo(effectiveVersion);
    assertThat(results.get(1).jobVersion().outputs())
        .extracting(output -> output.getDatasetName())
        .containsExactly(outputName);
    int persistedProjectionCount =
        countRows(
            "runs",
            "uuid = ? AND job_version_uuid = ? AND open_lineage_job_version_time = ? "
                + "AND open_lineage_job_version_key = ?",
            runUuid,
            effectiveVersion,
            completeRequest.order().getEventTime(),
            completeRequest.order().getEventKey());
    assertThat(persistedProjectionCount).isOne();
  }

  private List<ProjectionResult> project(List<ProjectionRequest> requests) {
    return jdbi.inTransaction(
        handle ->
            OpenLineageProjector.getInstance()
                .projectBatchInTransaction(
                    handle.attach(BaseDao.class), Utils.getMapper(), requests));
  }

  private List<RunProjectionResult> projectRuns(List<ProjectionRequest> requests) {
    return project(requests).stream().map(RunProjectionResult.class::cast).toList();
  }

  private List<RunProjectionResult> projectRuns(
      boolean listenerSnapshotRequired, BaseEvent... events) {
    return projectRuns(requests(listenerSnapshotRequired, events));
  }

  private List<ProjectionRequest> requests(boolean snapshotRequired, BaseEvent... events) {
    return Arrays.stream(events).map(event -> request(event, snapshotRequired)).toList();
  }

  private ProjectionRequest request(BaseEvent event, boolean listenerSnapshotRequired) {
    return new ProjectionRequest(event, Utils.toJson(event), listenerSnapshotRequired);
  }

  private LineageEvent runEvent(
      String jobName,
      UUID runUuid,
      long offset,
      String type,
      List<Dataset> inputs,
      List<Dataset> outputs) {
    return runEvent(
        jobName,
        runUuid,
        offset,
        type,
        JobFacet.builder().build(),
        RunFacet.builder().build(),
        inputs,
        outputs);
  }

  private LineageEvent runEvent(
      String jobName,
      UUID runUuid,
      long offset,
      String type,
      JobFacet jobFacet,
      RunFacet runFacet,
      List<Dataset> inputs,
      List<Dataset> outputs) {
    return LineageEvent.builder()
        .eventType(type)
        .eventTime(EVENT_TIME.plusSeconds(offset).atZone(LineageTestUtils.LOCAL_ZONE))
        .run(new LineageEvent.Run(runUuid.toString(), runFacet))
        .job(new LineageEvent.Job(NAMESPACE, jobName, jobFacet))
        .inputs(inputs)
        .outputs(outputs)
        .producer(PRODUCER_URL.toString())
        .build();
  }

  private JobEvent jobEvent(String jobName, long offset, String description, List<Dataset> inputs) {
    return jobEvent(jobName, EVENT_TIME.plusSeconds(offset), description, inputs);
  }

  private JobEvent jobEvent(
      String jobName, Instant eventTime, String description, List<Dataset> inputs) {
    return JobEvent.builder()
        .eventTime(eventTime.atZone(LineageTestUtils.LOCAL_ZONE))
        .job(
            new LineageEvent.Job(
                NAMESPACE,
                jobName,
                JobFacet.builder()
                    .documentation(DocumentationJobFacet.builder().description(description).build())
                    .build()))
        .inputs(inputs)
        .outputs(List.of())
        .producer(PRODUCER_URL.toString())
        .build();
  }

  private Dataset dataset(String name, String field) {
    return dataset(NAMESPACE, name, field);
  }

  private Dataset dataset(String namespace, String name, String field) {
    return new Dataset(namespace, name, DatasetFacets.builder().schema(schema(field)).build());
  }

  private DatasetEvent datasetEvent(long timeOffset, Dataset dataset) {
    return DatasetEvent.builder()
        .eventTime(EVENT_TIME.plusSeconds(timeOffset).atZone(LineageTestUtils.LOCAL_ZONE))
        .dataset(dataset)
        .producer(PRODUCER_URL.toString())
        .build();
  }

  private SchemaDatasetFacet schema(String field) {
    return new SchemaDatasetFacet(
        PRODUCER_URL, SCHEMA_URL, List.of(new SchemaField(field, "STRING", field)));
  }

  private Dataset outputWithColumnLineage(String outputName, String inputName) {
    var input = new LineageEvent.ColumnLineageInputField(NAMESPACE, inputName, INPUT_FIELD);
    var output =
        new LineageEvent.ColumnLineageOutputColumn(List.of(input), "column description", "DIRECT");
    var facet =
        new LineageEvent.ColumnLineageDatasetFacet(
            PRODUCER_URL,
            SCHEMA_URL,
            new LineageEvent.ColumnLineageDatasetFacetFields(Map.of(OUTPUT_FIELD, output)));
    return new Dataset(
        NAMESPACE,
        outputName,
        DatasetFacets.builder().schema(schema(OUTPUT_FIELD)).columnLineage(facet).build());
  }

  private LineageEvent terminalEvent(
      String jobName, UUID runUuid, long offset, String location, String facet, String output) {
    return runEvent(
        jobName,
        runUuid,
        offset,
        "COMPLETE",
        terminalFacet(location, facet),
        RunFacet.builder().build(),
        List.of(),
        List.of(dataset(output, OUTPUT_FIELD)));
  }

  private LineageEvent.ParentRunFacet parentFacet(UUID runUuid, String jobName) {
    return LineageEvent.ParentRunFacet.builder()
        ._producer(PRODUCER_URL)
        ._schemaURL(SCHEMA_URL)
        .run(LineageEvent.RunLink.builder().runId(runUuid.toString()).build())
        .job(LineageEvent.JobLink.builder().namespace(NAMESPACE).name(jobName).build())
        .build();
  }

  private JobFacet terminalFacet(String location, String markerName) {
    JobFacet facet =
        JobFacet.builder()
            .sourceCodeLocation(
                LineageEvent.SourceCodeLocationJobFacet.builder()
                    ._producer(PRODUCER_URL)
                    ._schemaURL(SCHEMA_URL)
                    .type("git")
                    .url(location)
                    .build())
            .build();
    facet.setFacet(markerName, Map.of("marker", markerName));
    return facet;
  }

  private static ProjectionRequest laterRequest(ProjectionRequest first, ProjectionRequest second) {
    return Arrays.compareUnsigned(first.order().getEventKey(), second.order().getEventKey()) > 0
        ? first
        : second;
  }

  private void assertTerminalLinkage(
      UUID runUuid,
      UUID winningVersion,
      String winningLocation,
      String losingLocation,
      String winningFacet,
      String losingFacet) {
    UUID linkedVersion = select("job_version_uuid", "runs", "uuid = ?", UUID.class, runUuid);
    String location = select("location", "job_versions", "uuid = ?", String.class, winningVersion);
    UUID latestRun =
        select("latest_run_uuid", "job_versions", "uuid = ?", UUID.class, winningVersion);
    int losingVersions = countRows("job_versions", "location = ?", losingLocation);
    int winningFacets =
        countRows(
            "job_facets",
            "run_uuid = ? AND name = ? AND job_version_uuid = ?",
            runUuid,
            winningFacet,
            winningVersion);
    int losingFacets =
        countRows(
            "job_facets",
            "run_uuid = ? AND name = ? AND job_version_uuid IS NOT NULL",
            runUuid,
            losingFacet);
    assertThat(
            Tuple.tuple(
                linkedVersion, location, latestRun, losingVersions, winningFacets, losingFacets))
        .isEqualTo(Tuple.tuple(winningVersion, winningLocation, runUuid, 0, 1, 0));
  }

  private void assertSnapshot(
      RunProjectionResult result, List<String> inputNames, List<String> outputNames) {
    assertThat(result.runIoSnapshot()).isNotNull();
    assertThat(result.runIoSnapshot().getInputs())
        .extracting(row -> row.getDatasetName())
        .containsExactlyElementsOf(inputNames);
    assertThat(result.runIoSnapshot().getOutputs())
        .extracting(row -> row.getDatasetName())
        .containsExactlyElementsOf(outputNames);
  }

  private void assertColumnLineage(
      RunProjectionResult inputResult, RunProjectionResult outputResult) {
    DatasetProjection input = inputResult.inputs().orElseThrow().get(0);
    DatasetProjection output = outputResult.outputs().orElseThrow().get(0);
    UUID outputField =
        datasetFieldDao.findUuid(output.dataset().getUuid(), OUTPUT_FIELD).orElseThrow();
    assertThat(
            columnLineageDao.findColumnLineageByDatasetVersionAndOutputDatasetFields(
                output.version().getUuid(), List.of(outputField)))
        .extracting(
            row -> row.getInputDatasetVersionUuid(),
            row -> row.getTransformationDescription().orElse(null),
            row -> row.getTransformationType().orElse(null))
        .containsExactly(Tuple.tuple(input.version().getUuid(), "column description", "DIRECT"));
  }

  private DatasetSymlinkRow symlink(String namespace, String name) {
    UUID namespaceUuid = namespaceDao.findNamespaceByName(namespace).orElseThrow().getUuid();
    return datasetSymlinkDao
        .findDatasetSymlinkByNamespaceUuidAndName(namespaceUuid, name)
        .orElseThrow();
  }

  private long facetCount(String table, UUID runUuid, String facetName) {
    if (!List.of("run_facets", "job_facets", "dataset_facets").contains(table)) {
      throw new IllegalArgumentException("Unsupported facet count target");
    }
    return countRows(table, "run_uuid = ? AND name = ?", runUuid, facetName);
  }

  private int countRows(String table, String predicate, Object... arguments) {
    return select("count(*)", table, predicate, Integer.class, arguments);
  }

  private <T> T select(
      String columns, String table, String predicate, Class<T> type, Object... arguments) {
    return jdbi.withHandle(
        handle -> {
          var query =
              handle.createQuery("SELECT " + columns + " FROM " + table + " WHERE " + predicate);
          for (int index = 0; index < arguments.length; index++) {
            query.bind(index, arguments[index]);
          }
          return query.mapTo(type).one();
        });
  }
}
