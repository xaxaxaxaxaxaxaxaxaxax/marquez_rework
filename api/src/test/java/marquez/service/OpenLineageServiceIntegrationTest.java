/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static marquez.db.LineageTestUtils.PRODUCER_URL;
import static marquez.db.LineageTestUtils.SCHEMA_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.util.Resources;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import marquez.common.Utils;
import marquez.common.models.FieldName;
import marquez.common.models.JobType;
import marquez.common.models.RunState;
import marquez.db.BaseDao;
import marquez.db.DatasetDao;
import marquez.db.DatasetVersionDao;
import marquez.db.JobDao;
import marquez.db.JobVersionDao;
import marquez.db.NamespaceDao;
import marquez.db.RunArgsDao;
import marquez.db.RunDao;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunArgsRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.RunTransitionListener.JobInputUpdate;
import marquez.service.RunTransitionListener.JobOutputUpdate;
import marquez.service.RunTransitionListener.RunTransition;
import marquez.service.models.Dataset;
import marquez.service.models.DatasetEvent;
import marquez.service.models.Job;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.DatasetFacets;
import marquez.service.models.LineageEvent.DatasourceDatasetFacet;
import marquez.service.models.LineageEvent.JobFacet;
import marquez.service.models.LineageEvent.JobTypeJobFacet;
import marquez.service.models.LineageEvent.LineageEventBuilder;
import marquez.service.models.LineageEvent.RunFacet;
import marquez.service.models.LineageEvent.SQLJobFacet;
import marquez.service.models.LineageEvent.SchemaDatasetFacet;
import marquez.service.models.LineageEvent.SchemaField;
import marquez.service.models.Run;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

@org.junit.jupiter.api.Tag("IntegrationTests")
@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
public class OpenLineageServiceIntegrationTest {

  public static final String NAMESPACE = "theNamespace";
  public static final String JOB_NAME = "theJob";
  public static final ZoneId TIMEZONE = ZoneId.of("America/Los_Angeles");
  public static final String DATASET_NAME = "theDataset";
  public static final String INPUT_DATASET_NAME = "theInputDataset";
  private RunService runService;

  private JobService jobService;
  private BaseDao baseDao;
  private Jdbi jdbi;
  private JobDao jobDao;
  private JobVersionDao jobVersionDao;
  private DatasetDao datasetDao;
  private DatasetVersionDao datasetVersionDao;
  private ArgumentCaptor<JobInputUpdate> runInputListener;
  private ArgumentCaptor<JobOutputUpdate> runOutputListener;
  private ArgumentCaptor<RunTransition> runTransitionListener;
  private OpenLineageService lineageService;

  public static String EVENT_REQUIRED_ONLY = "open_lineage/event_required_only.json";
  public static String EVENT_SIMPLE = "open_lineage/event_simple.json";
  public static String EVENT_FULL = "open_lineage/event_full.json";
  public static String EVENT_UNICODE = "open_lineage/event_unicode.json";
  public static String EVENT_LARGE = "open_lineage/event_large.json";

  public static List<Object[]> getData() throws IOException, URISyntaxException {
    return Stream.of(
            new Object[] {
              Arrays.asList(Resources.getResource(EVENT_REQUIRED_ONLY).toURI()),
              new ExpectedResults(0, 0, 0, 0)
            },
            new Object[] {
              Arrays.asList(Resources.getResource(EVENT_SIMPLE).toURI()),
              new ExpectedResults(2, 1, 1, 1)
            },
            new Object[] {
              Arrays.asList(Resources.getResource(EVENT_FULL).toURI()),
              new ExpectedResults(1, 1, 1, 1)
            },
            new Object[] {
              Arrays.asList(Resources.getResource(EVENT_UNICODE).toURI()),
              new ExpectedResults(2, 1, 1, 1)
            },
            new Object[] {
              Arrays.asList(
                  Resources.getResource("open_lineage/listener/1.json").toURI(),
                  Resources.getResource("open_lineage/listener/2.json").toURI()),
              // Each reported side replaces that side's prior state; listener/2 reports one of
              // each, rather than extending listener/1's two inputs and one output.
              new ExpectedResults(1, 1, 2, 1)
            },
            new Object[] {
              Arrays.asList(Resources.getResource(EVENT_LARGE).toURI()),
              new ExpectedResults(1, 1, 1, 1)
            })
        .collect(Collectors.toList());
  }

  public static class ExpectedResults {

    public int inputDatasetCount;
    public int outputDatasetCount;
    public int inputEventCount;
    public int outputEventCount;

    public ExpectedResults(
        int inputDatasetCount, int outputDatasetCount, int inputEventCount, int outputEventCount) {
      this.inputDatasetCount = inputDatasetCount;
      this.outputDatasetCount = outputDatasetCount;
      this.inputEventCount = inputEventCount;
      this.outputEventCount = outputEventCount;
    }
  }

  @BeforeEach
  public void setup(Jdbi jdbi) throws SQLException {
    this.jdbi = jdbi;
    baseDao = jdbi.onDemand(BaseDao.class);
    datasetVersionDao = jdbi.onDemand(DatasetVersionDao.class);
    jobDao = jdbi.onDemand(JobDao.class);
    jobVersionDao = jdbi.onDemand(JobVersionDao.class);
    runService = mock(RunService.class);
    when(runService.hasRunTransitionListeners()).thenReturn(true);
    jobService = new JobService(jobDao, runService);
    runInputListener = ArgumentCaptor.forClass(JobInputUpdate.class);
    when(runService.notify(runInputListener.capture())).thenReturn(0);
    runOutputListener = ArgumentCaptor.forClass(JobOutputUpdate.class);
    when(runService.notify(runOutputListener.capture())).thenReturn(0);
    runTransitionListener = ArgumentCaptor.forClass(RunTransition.class);
    when(runService.notify(runTransitionListener.capture())).thenReturn(0);
    lineageService = new OpenLineageService(baseDao, runService, Runnable::run);
    datasetDao = jdbi.onDemand(DatasetDao.class);

    NamespaceRow namespace =
        jdbi.onDemand(NamespaceDao.class)
            .upsertNamespaceRow(UUID.randomUUID(), Instant.now(), NAMESPACE, "me");
    JobRow job =
        jobDao.upsertJob(
            UUID.randomUUID(),
            JobType.BATCH,
            Instant.now(),
            namespace.getUuid(),
            NAMESPACE,
            "parentJob",
            "description",
            null,
            null,
            null);
    Map<String, String> runArgsMap = new HashMap<>();
    RunArgsRow argsRow =
        jdbi.onDemand(RunArgsDao.class)
            .upsertRunArgs(
                UUID.randomUUID(),
                Instant.now(),
                Utils.toJson(runArgsMap),
                Utils.checksumFor(runArgsMap));
    jdbi.onDemand(RunDao.class)
        .upsert(
            UUID.fromString("3f5e83fa-3480-44ff-99c5-ff943904e5e8"),
            null,
            "3f5e83fa-3480-44ff-99c5-ff943904e5e8",
            Instant.now(),
            job.getUuid(),
            null,
            argsRow.getUuid(),
            Instant.now(),
            Instant.now(),
            RunState.RUNNING,
            Instant.now(),
            NAMESPACE,
            job.getName(),
            null);
  }

  private List<LineageEvent> initEvents(List<URI> uris) {
    List<LineageEvent> events = new ArrayList<>();
    for (URI uri : uris) {
      try {
        LineageEvent event = getLineageEventFromResource(uri);
        lineageService.createAsync(event).get();
        events.add(event);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return events;
  }

  @ParameterizedTest
  @MethodSource("getData")
  public void testRunListenerInput(List<URI> uris, ExpectedResults expectedResults) {
    initEvents(uris);

    if (expectedResults.inputDatasetCount > 0) {
      Assertions.assertEquals(
          expectedResults.inputEventCount,
          runInputListener.getAllValues().size(),
          "RunInputListener events");
      Assertions.assertEquals(
          expectedResults.inputDatasetCount,
          runInputListener
              .getAllValues()
              .get(runInputListener.getAllValues().size() - 1)
              .getInputs()
              .size(),
          "Dataset input count");
    }
  }

  @ParameterizedTest
  @MethodSource("getData")
  public void testRunListenerOutput(List<URI> uris, ExpectedResults expectedResults) {
    initEvents(uris);

    if (expectedResults.outputDatasetCount > 0) {
      Assertions.assertEquals(
          expectedResults.outputEventCount,
          runOutputListener.getAllValues().size(),
          "RunOutputListener events");
      Assertions.assertEquals(
          expectedResults.outputDatasetCount,
          runOutputListener
              .getAllValues()
              .get(runOutputListener.getAllValues().size() - 1)
              .getOutputs()
              .size(),
          "Dataset output count");
    }
  }

  @ParameterizedTest
  @MethodSource("getData")
  public void testRunTransition(List<URI> uris, ExpectedResults expectedResults) {
    initEvents(uris);

    if (expectedResults.inputEventCount > 0) {
      Assertions.assertEquals(
          uris.size(),
          runTransitionListener.getAllValues().size(),
          "RunTransition happens once for each run");
    }
  }

  @ParameterizedTest
  @MethodSource({"getData"})
  public void serviceCalls(List<URI> uris, ExpectedResults expectedResults) {
    List<LineageEvent> events = initEvents(uris);

    JobService jobService = new JobService(baseDao, runService);
    LineageEvent event = events.get(events.size() - 1);
    Optional<Job> job =
        jobService.findWithDatasetsAndRun(
            Utils.sanitizeOpenLineageNamespace(event.getJob().getNamespace()),
            event.getJob().getName());
    Assertions.assertTrue(job.isPresent(), "Job does not exist: " + event.getJob().getName());

    RunService runService = new RunService(baseDao, new ArrayList());
    Optional<Run> run =
        runService.findRunByUuid(Utils.openLineageRunUuid(event.getRun().getRunId()));
    Assertions.assertTrue(run.isPresent(), "Should have run");

    if (event.getInputs() != null) {
      for (LineageEvent.Dataset ds : event.getInputs()) {
        checkExists(ds);
      }
    }
    if (event.getOutputs() != null) {
      for (LineageEvent.Dataset ds : event.getOutputs()) {
        checkExists(ds);
      }
    }
  }

  @Test
  public void testDatasetVersionUpdatedOnRunCompletion()
      throws ExecutionException, InterruptedException {
    LineageEvent.Dataset dataset =
        LineageEvent.Dataset.builder()
            .name(DATASET_NAME)
            .namespace(NAMESPACE)
            .facets(
                DatasetFacets.builder()
                    .dataSource(
                        DatasourceDatasetFacet.builder()
                            .name("theDatasource")
                            .uri("http://thedatasource")
                            .build())
                    .build())
            .build();

    // First run creates the dataset without a currentVersionUuid
    UUID firstRunId = UUID.randomUUID();
    lineageService
        .createAsync(
            LineageEvent.builder()
                .eventType("RUNNING")
                .run(new LineageEvent.Run(firstRunId.toString(), RunFacet.builder().build()))
                .job(LineageEvent.Job.builder().name(JOB_NAME).namespace(NAMESPACE).build())
                .eventTime(Instant.now().atZone(TIMEZONE))
                .inputs(new ArrayList<>())
                .outputs(Collections.singletonList(dataset))
                .build())
        .get();
    Optional<Dataset> datasetRow = datasetDao.findDatasetByName(NAMESPACE, DATASET_NAME);
    assertThat(datasetRow).isPresent().flatMap(Dataset::getCurrentVersion).isNotPresent();

    // On complete, the currentVersionUuid is updated
    lineageService
        .createAsync(
            LineageEvent.builder()
                .eventType("COMPLETE")
                .run(new LineageEvent.Run(firstRunId.toString(), RunFacet.builder().build()))
                .job(LineageEvent.Job.builder().name(JOB_NAME).namespace(NAMESPACE).build())
                .eventTime(Instant.now().atZone(TIMEZONE))
                .inputs(new ArrayList<>())
                .outputs(Collections.singletonList(dataset))
                .build())
        .get();
    datasetRow = datasetDao.findDatasetByName(NAMESPACE, DATASET_NAME);
    assertThat(datasetRow).isPresent().flatMap(Dataset::getCurrentVersion).isPresent();

    List<ExtendedDatasetVersionRow> outputs =
        datasetVersionDao.findOutputDatasetVersionsFor(firstRunId);
    assertThat(outputs).hasSize(1).map(DatasetVersionRow::getVersion).isNotNull();

    UUID dsVersion1Id = outputs.get(0).getVersion();

    // A consumer gets the currentVersionUuid as its input version
    UUID secondRunId = UUID.randomUUID();
    lineageService
        .createAsync(
            LineageEvent.builder()
                .eventType("COMPLETE")
                .run(new LineageEvent.Run(secondRunId.toString(), RunFacet.builder().build()))
                .job(LineageEvent.Job.builder().name("AnInputJob").namespace(NAMESPACE).build())
                .eventTime(Instant.now().atZone(TIMEZONE))
                .inputs(Collections.singletonList(dataset))
                .outputs(new ArrayList<>())
                .build())
        .get();
    List<ExtendedDatasetVersionRow> inputs =
        datasetVersionDao.findInputDatasetVersionsFor(secondRunId);
    assertThat(inputs).hasSize(1).map(DatasetVersionRow::getVersion).contains(dsVersion1Id);

    // fail to write the dataset - the currentVersionUuid is not updated
    UUID failedRunId = UUID.randomUUID();
    lineageService
        .createAsync(
            LineageEvent.builder()
                .eventType("FAILED")
                .run(new LineageEvent.Run(failedRunId.toString(), RunFacet.builder().build()))
                .job(LineageEvent.Job.builder().name(JOB_NAME).namespace(NAMESPACE).build())
                .eventTime(Instant.now().atZone(TIMEZONE))
                .inputs(new ArrayList<>())
                .outputs(Collections.singletonList(dataset))
                .build())
        .get();

    Optional<Dataset> afterFailureDataset = datasetDao.findDatasetByName(NAMESPACE, DATASET_NAME);
    assertThat(afterFailureDataset)
        .isPresent()
        .flatMap(Dataset::getCurrentVersion)
        .isPresent()
        .get()
        .isEqualTo(datasetRow.get().getCurrentVersion().get());

    // A new consumer job run only sees the first dataset version
    UUID fourthRunId = UUID.randomUUID();
    lineageService
        .createAsync(
            LineageEvent.builder()
                .eventType("COMPLETE")
                .run(new LineageEvent.Run(fourthRunId.toString(), RunFacet.builder().build()))
                .job(LineageEvent.Job.builder().name("AnInputJob").namespace(NAMESPACE).build())
                .eventTime(Instant.now().atZone(TIMEZONE))
                .inputs(Collections.singletonList(dataset))
                .outputs(new ArrayList<>())
                .build())
        .get();

    // still version 1 is consumed since the second producer job run failed
    assertThat(datasetVersionDao.findInputDatasetVersionsFor(secondRunId))
        .hasSize(1)
        .map(DatasetVersionRow::getVersion)
        .contains(dsVersion1Id);
  }

  @Test
  void testJobIsNotHiddenAfterSubsequentOLEvent() throws ExecutionException, InterruptedException {
    String name = "aNotHiddenJob";
    Instant firstEventTime = Instant.now();

    LineageEvent.LineageEventBuilder builder =
        LineageEvent.builder()
            .eventType("COMPLETE")
            .job(LineageEvent.Job.builder().name(name).namespace(NAMESPACE).build())
            .eventTime(firstEventTime.atZone(TIMEZONE))
            .inputs(Collections.emptyList())
            .outputs(Collections.emptyList());

    lineageService
        .createAsync(
            builder
                .run(new LineageEvent.Run(UUID.randomUUID().toString(), RunFacet.builder().build()))
                .build())
        .get();

    assertThat(jobService.findJobByName(NAMESPACE, name)).isNotEmpty();

    jobService.delete(NAMESPACE, name);

    assertThat(jobService.findJobByName(NAMESPACE, name)).isEmpty();

    lineageService
        .createAsync(
            builder
                .eventTime(firstEventTime.plusSeconds(1).atZone(TIMEZONE))
                .run(new LineageEvent.Run(UUID.randomUUID().toString(), RunFacet.builder().build()))
                .build())
        .get();

    assertThat(jobService.findJobByName(NAMESPACE, name)).isNotEmpty();
  }

  @Test
  void testDatasetEvent() throws ExecutionException, InterruptedException {
    LineageEvent.Dataset dataset =
        LineageEvent.Dataset.builder()
            .name(DATASET_NAME)
            .namespace(NAMESPACE)
            .facets(
                DatasetFacets.builder()
                    .schema(
                        new SchemaDatasetFacet(
                            PRODUCER_URL,
                            SCHEMA_URL,
                            Arrays.asList(new SchemaField("col", "STRING", "my name"))))
                    .dataSource(
                        DatasourceDatasetFacet.builder()
                            .name("theDatasource")
                            .uri("http://thedatasource")
                            .build())
                    .build())
            .build();

    lineageService
        .createAsync(
            DatasetEvent.builder()
                .eventTime(Instant.now().atZone(TIMEZONE))
                .dataset(dataset)
                .build())
        .get();

    Optional<Dataset> datasetRow = datasetDao.findDatasetByName(NAMESPACE, DATASET_NAME);
    assertThat(datasetRow).isPresent().map(Dataset::getCurrentVersion).isPresent();
    assertThat(datasetRow.get().getSourceName().getValue()).isEqualTo("theDatasource");
    assertThat(datasetRow.get().getFields())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("name", FieldName.of("col"))
        .hasFieldOrPropertyWithValue("type", "STRING");
  }

  @Test
  void testJobEvent() throws ExecutionException, InterruptedException {
    String query = "select * from table";
    LineageEvent.Job job =
        LineageEvent.Job.builder()
            .name(JOB_NAME)
            .namespace(NAMESPACE)
            .facets(JobFacet.builder().sql(SQLJobFacet.builder().query(query).build()).build())
            .build();

    DatasourceDatasetFacet theDatasource =
        DatasourceDatasetFacet.builder().name("theDatasource").uri("http://thedatasource").build();
    LineageEvent.Dataset input =
        LineageEvent.Dataset.builder()
            .name(INPUT_DATASET_NAME)
            .namespace(NAMESPACE)
            .facets(DatasetFacets.builder().dataSource(theDatasource).build())
            .build();

    LineageEvent.Dataset output =
        LineageEvent.Dataset.builder()
            .name(DATASET_NAME)
            .namespace(NAMESPACE)
            .facets(DatasetFacets.builder().dataSource(theDatasource).build())
            .build();

    lineageService
        .createAsync(
            JobEvent.builder()
                .eventTime(Instant.now().atZone(TIMEZONE))
                .job(job)
                .inputs(Collections.singletonList(input))
                .outputs(Collections.singletonList(output))
                .build())
        .get();

    Optional<Job> jobByName = jobDao.findJobByName(NAMESPACE, JOB_NAME);
    assertThat(jobByName).isPresent().map(Job::getCurrentVersion).isPresent();
    assertThat(jobByName.get().getFacets().get("sql").toString()).contains(query);

    DatasetService datasetService = new DatasetService(baseDao, runService);
    assertThat(
            datasetService
                .findDatasetByName(
                    Utils.sanitizeOpenLineageNamespace(input.getNamespace()), input.getName())
                .get()
                .getSourceName()
                .getValue())
        .isEqualTo("theDatasource");

    assertThat(
            datasetService
                .findDatasetByName(
                    Utils.sanitizeOpenLineageNamespace(output.getNamespace()), output.getName())
                .get()
                .getSourceName()
                .getValue())
        .isEqualTo("theDatasource");
  }

  @Test
  void testStreamingJobRunIoStateMachine() throws ExecutionException, InterruptedException {
    String jobName = "streaming_job_name";
    LineageEvent.Dataset initialInput =
        LineageEvent.Dataset.builder().name(INPUT_DATASET_NAME).namespace(NAMESPACE).build();
    LineageEvent.Dataset output =
        LineageEvent.Dataset.builder()
            .name(DATASET_NAME)
            .namespace(NAMESPACE)
            .facets(
                DatasetFacets.builder()
                    .schema(
                        new SchemaDatasetFacet(
                            PRODUCER_URL,
                            SCHEMA_URL,
                            List.of(new SchemaField("col", "STRING", "my name"))))
                    .build())
            .build();
    LineageEvent.Dataset replacementInput =
        LineageEvent.Dataset.builder().name("otherDataset").namespace(NAMESPACE).build();
    JobFacet streamingFacets =
        JobFacet.builder()
            .jobType(
                JobTypeJobFacet.builder()
                    .processingType("STREAMING")
                    .integration("FLINK")
                    .jobType("JOB")
                    .build())
            .build();
    UUID runId = UUID.randomUUID();
    Instant initialEventTime = Instant.now();
    LineageEventBuilder events =
        LineageEvent.builder()
            .eventType("RUNNING")
            .run(new LineageEvent.Run(runId.toString(), RunFacet.builder().build()))
            .job(
                LineageEvent.Job.builder()
                    .name(jobName)
                    .namespace(NAMESPACE)
                    .facets(streamingFacets)
                    .build());
    LineageEvent initialEvent =
        eventAt(events, initialEventTime, List.of(initialInput), List.of(output));

    // Initial state at t0: both sides are reported and materialize one job version.
    emit(initialEvent);
    UUID outputDatasetVersion = currentDatasetVersion(DATASET_NAME);
    Job job = jobDao.findJobByName(NAMESPACE, jobName).orElseThrow();
    assertThat(job.getInputs()).hasSize(1);
    assertThat(job.getType()).isEqualTo(JobType.STREAM);
    assertThat(job.getLabels()).containsExactly("JOB", "FLINK");
    UUID initialJobVersion = currentJobVersion(jobName);
    int initialJobVersionCount = jobVersionCount(jobName);
    Instant initialVersionUpdatedAt = latestJobVersionUpdate();
    assertStreamingState(jobName, runId, initialJobVersion, 1, 1, initialJobVersionCount);

    // Exact replay at t0 is idempotent.
    emit(initialEvent);
    assertStreamingState(jobName, runId, initialJobVersion, 1, 1, initialJobVersionCount);

    // Missing sides at t0+1 retain both sides and the current job-version UUID.
    emit(eventAt(events, initialEventTime.plusSeconds(1), null, null));
    assertStreamingState(jobName, runId, initialJobVersion, 1, 1, initialJobVersionCount);

    // Reporting only inputs at t0+2 replaces inputs while retaining outputs.
    emit(eventAt(events, initialEventTime.plusSeconds(2), List.of(replacementInput), null));
    UUID replacementJobVersion = currentJobVersion(jobName);
    assertThat(replacementJobVersion).isNotEqualTo(initialJobVersion);
    assertStreamingState(jobName, runId, replacementJobVersion, 1, 1, initialJobVersionCount + 1);
    assertThat(currentDatasetVersion(DATASET_NAME)).isEqualTo(outputDatasetVersion);

    // Explicit empty sides at t0+3 clear both sides and create a distinct version.
    events.eventType("COMPLETE");
    emit(eventAt(events, initialEventTime.plusSeconds(3), List.of(), List.of()));
    UUID clearedJobVersion = currentJobVersion(jobName);
    assertThat(clearedJobVersion).isNotIn(initialJobVersion, replacementJobVersion);
    assertStreamingState(jobName, runId, clearedJobVersion, 0, 0, initialJobVersionCount + 2);
    job = jobDao.findJobByName(NAMESPACE, jobName).orElseThrow();
    assertThat(job.getInputs()).isEmpty();
    assertThat(job.getOutputs()).isEmpty();
    assertThat(job.getType()).isEqualTo(JobType.STREAM);
    assertThat(job.getLabels()).containsExactly("JOB", "FLINK");
    assertThat(latestJobVersionUpdate()).isAfter(initialVersionUpdatedAt);
    assertThat(currentDatasetVersion(DATASET_NAME)).isEqualTo(outputDatasetVersion);
  }

  private void emit(LineageEvent event) throws ExecutionException, InterruptedException {
    lineageService.createAsync(event).get();
  }

  private UUID currentJobVersion(String jobName) {
    return jobDao.findJobByName(NAMESPACE, jobName).orElseThrow().getCurrentVersion().orElseThrow();
  }

  private UUID currentDatasetVersion(String datasetName) {
    return datasetDao
        .findDatasetAsRow(NAMESPACE, datasetName)
        .orElseThrow()
        .getCurrentVersionUuid()
        .orElseThrow();
  }

  private int jobVersionCount(String jobName) {
    return jobVersionDao.findAllJobVersions(NAMESPACE, jobName, 10, 0).size();
  }

  private Instant latestJobVersionUpdate() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT max(updated_at) FROM job_versions")
                .mapTo(Instant.class)
                .first());
  }

  private LineageEvent eventAt(
      LineageEventBuilder events,
      Instant eventTime,
      List<LineageEvent.Dataset> inputs,
      List<LineageEvent.Dataset> outputs) {
    return events.eventTime(eventTime.atZone(TIMEZONE)).inputs(inputs).outputs(outputs).build();
  }

  private void assertStreamingState(
      String jobName,
      UUID runId,
      UUID jobVersion,
      int inputCount,
      int outputCount,
      int versionCount) {
    assertThat(currentJobVersion(jobName)).isEqualTo(jobVersion);
    assertThat(jobVersionCount(jobName)).isEqualTo(versionCount);
    assertThat(jobVersionDao.findRunIoSnapshot(runId).getInputs()).hasSize(inputCount);
    assertThat(jobVersionDao.findRunIoSnapshot(runId).getOutputs()).hasSize(outputCount);
    assertThat(jobVersionDao.findInputDatasetsFor(jobVersion)).hasSize(inputCount);
    assertThat(jobVersionDao.findOutputDatasetsFor(jobVersion)).hasSize(outputCount);
  }

  private void checkExists(LineageEvent.Dataset ds) {
    DatasetService datasetService = new DatasetService(baseDao, runService);

    Optional<Dataset> dataset =
        datasetService.findDatasetByName(
            Utils.sanitizeOpenLineageNamespace(ds.getNamespace()), ds.getName());
    Assertions.assertTrue(dataset.isPresent(), "Dataset does not exist: " + ds);
  }

  private static LineageEvent getLineageEventFromResource(URI location) {
    try {
      return Utils.newObjectMapper().readValue(location.toURL(), LineageEvent.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
