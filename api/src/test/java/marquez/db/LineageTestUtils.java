/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.validation.Valid;
import lombok.Value;
import marquez.common.Utils;
import marquez.db.OpenLineageProjector.DatasetProjection;
import marquez.db.OpenLineageProjector.DatasetProjectionResult;
import marquez.db.OpenLineageProjector.JobProjectionResult;
import marquez.db.OpenLineageProjector.JobVersionProjection;
import marquez.db.OpenLineageProjector.ProjectionRequest;
import marquez.db.OpenLineageProjector.ProjectionResult;
import marquez.db.OpenLineageProjector.RunProjectionResult;
import marquez.db.models.UpdateLineageRow;
import marquez.db.models.UpdateLineageRow.DatasetRecord;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.Dataset;
import marquez.service.models.LineageEvent.DatasetFacets;
import marquez.service.models.LineageEvent.DatasourceDatasetFacet;
import marquez.service.models.LineageEvent.DocumentationDatasetFacet;
import marquez.service.models.LineageEvent.Job;
import marquez.service.models.LineageEvent.JobFacet;
import marquez.service.models.LineageEvent.NominalTimeRunFacet;
import marquez.service.models.LineageEvent.Run;
import marquez.service.models.LineageEvent.RunFacet;
import marquez.service.models.LineageEvent.SchemaDatasetFacet;
import marquez.service.models.LineageEvent.SchemaField;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class LineageTestUtils {

  public static final ZoneId LOCAL_ZONE = ZoneId.of("America/Los_Angeles");
  public static final ImmutableMap<String, Object> EMPTY_MAP = ImmutableMap.of();
  public static final URI PRODUCER_URL = URI.create("http://test.producer/");
  public static final URI SCHEMA_URL = URI.create("http://test.schema/");
  public static final String NAMESPACE = "namespace";

  /**
   * Create an {@link UpdateLineageRow} from the input job details and datasets.
   *
   * @param dao
   * @param jobName
   * @param status
   * @param jobFacet
   * @param inputs
   * @param outputs
   * @return
   */
  public static UpdateLineageRow createLineageRow(
      OpenLineageDao dao,
      String jobName,
      String status,
      JobFacet jobFacet,
      List<Dataset> inputs,
      List<Dataset> outputs) {
    return createLineageRow(dao, jobName, status, jobFacet, inputs, outputs, null);
  }

  /**
   * Create an {@link UpdateLineageRow} from the input job details and datasets.
   *
   * @param dao
   * @param jobName
   * @param status
   * @param jobFacet
   * @param inputs
   * @param outputs
   * @param parentRunFacet
   * @return
   */
  public static UpdateLineageRow createLineageRow(
      OpenLineageDao dao,
      String jobName,
      String status,
      JobFacet jobFacet,
      List<Dataset> inputs,
      List<Dataset> outputs,
      @Valid LineageEvent.ParentRunFacet parentRunFacet) {
    return createLineageRow(
        dao, jobName, status, jobFacet, inputs, outputs, parentRunFacet, ImmutableMap.of());
  }

  /**
   * Create an {@link UpdateLineageRow} from the input job details and datasets.
   *
   * @param dao
   * @param jobName
   * @param status
   * @param jobFacet
   * @param inputs
   * @param outputs
   * @param parentRunFacet
   * @param runFacets
   * @return
   */
  public static UpdateLineageRow createLineageRow(
      OpenLineageDao dao,
      String jobName,
      String status,
      JobFacet jobFacet,
      List<Dataset> inputs,
      List<Dataset> outputs,
      @Valid LineageEvent.ParentRunFacet parentRunFacet,
      ImmutableMap<String, Object> runFacets) {
    return createLineageRow(
        dao,
        jobName,
        UUID.randomUUID(),
        status,
        jobFacet,
        inputs,
        outputs,
        parentRunFacet,
        runFacets);
  }

  /**
   * Create an {@link UpdateLineageRow} from the input job details and datasets.
   *
   * @param dao
   * @param jobName
   * @param runId
   * @param status
   * @param jobFacet
   * @param inputs
   * @param outputs
   * @param parentRunFacet
   * @param runFacets
   * @return
   */
  public static UpdateLineageRow createLineageRow(
      OpenLineageDao dao,
      String jobName,
      UUID runId,
      String status,
      JobFacet jobFacet,
      List<Dataset> inputs,
      List<Dataset> outputs,
      @Valid LineageEvent.ParentRunFacet parentRunFacet,
      ImmutableMap<String, Object> runFacets) {
    NominalTimeRunFacet nominalTimeRunFacet = new NominalTimeRunFacet();
    nominalTimeRunFacet.setNominalStartTime(
        Instant.now().atZone(LOCAL_ZONE).truncatedTo(ChronoUnit.HOURS));
    nominalTimeRunFacet.setNominalEndTime(
        nominalTimeRunFacet.getNominalStartTime().plus(1, ChronoUnit.HOURS));

    LineageEvent event =
        LineageEvent.builder()
            .eventType(status)
            .eventTime(Instant.now().atZone(LOCAL_ZONE))
            .run(
                new Run(
                    runId.toString(), new RunFacet(nominalTimeRunFacet, parentRunFacet, runFacets)))
            .job(new Job(NAMESPACE, jobName, jobFacet))
            .inputs(inputs)
            .outputs(outputs)
            .producer(PRODUCER_URL.toString())
            .build();
    // emulate an OpenLineage RunEvent
    event
        .getProperties()
        .put(
            "_schemaURL",
            "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/RunEvent");
    String exactEventJson = Utils.toJson(event);
    UpdateLineageRow updateLineageRow = projectAsLegacyRow(dao, event, exactEventJson);
    dao.createOpenLineageEventDao()
        .createLineageEvent(
            event.getEventType() == null ? "" : event.getEventType(),
            event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
            runId,
            event.getJob().getName(),
            event.getJob().getNamespace(),
            exactEventJson,
            event.getProducer());

    return updateLineageRow;
  }

  /**
   * Create an {@link UpdateLineageRow} from dataset.
   *
   * @param dao
   * @param dataset
   * @return
   */
  public static UpdateLineageRow createLineageRow(OpenLineageDao dao, Dataset dataset) {
    DatasetEvent event =
        DatasetEvent.builder()
            .eventTime(Instant.now().atZone(LOCAL_ZONE))
            .dataset(dataset)
            .producer(PRODUCER_URL.toString())
            .build();

    // emulate an OpenLineage DatasetEvent
    event
        .getProperties()
        .put(
            "_schemaURL",
            "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/RunEvent");
    String exactEventJson = Utils.toJson(event);
    UpdateLineageRow updateLineageRow = projectAsLegacyRow(dao, event, exactEventJson);
    dao.createOpenLineageEventDao()
        .createDatasetEvent(
            event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
            exactEventJson,
            event.getProducer());

    return updateLineageRow;
  }

  /**
   * Create an {@link UpdateLineageRow} from dataset.
   *
   * @param dao
   * @param job
   * @return
   */
  public static UpdateLineageRow createLineageRow(
      OpenLineageDao dao, Job job, List<Dataset> inputs, List<Dataset> outputs) {
    JobEvent event =
        JobEvent.builder()
            .eventTime(Instant.now().atZone(LOCAL_ZONE))
            .job(job)
            .producer(PRODUCER_URL.toString())
            .inputs(inputs)
            .outputs(outputs)
            .build();

    // emulate an OpenLineage JobEvent
    event
        .getProperties()
        .put(
            "_schemaURL",
            "https://openlineage.io/spec/1-0-1/OpenLineage.json#/definitions/RunEvent");
    String exactEventJson = Utils.toJson(event);
    UpdateLineageRow updateLineageRow = projectAsLegacyRow(dao, event, exactEventJson);
    dao.createOpenLineageEventDao()
        .createJobEvent(
            event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant(),
            event.getJob().getName(),
            event.getJob().getNamespace(),
            exactEventJson,
            event.getProducer());

    return updateLineageRow;
  }

  /**
   * Keeps broad legacy test fixtures source-compatible while exercising the immutable projector.
   */
  static UpdateLineageRow projectAsLegacyRow(
      OpenLineageDao dao, BaseEvent event, String exactEventJson) {
    return toLegacyRow(project(dao, event, exactEventJson, true));
  }

  static ProjectionResult project(
      OpenLineageDao dao,
      BaseEvent event,
      String exactEventJson,
      boolean listenerSnapshotRequired) {
    ProjectionRequest request =
        new ProjectionRequest(event, exactEventJson, listenerSnapshotRequired);
    return dao.inTransaction(
        transactional ->
            OpenLineageProjector.getInstance()
                .projectInTransaction(transactional, Utils.getMapper(), request));
  }

  static UpdateLineageRow toLegacyRow(ProjectionResult result) {
    UpdateLineageRow row = new UpdateLineageRow();
    if (result instanceof RunProjectionResult run) {
      row.setNamespace(run.namespace());
      row.setJob(run.job());
      row.setRunArgs(run.runArgs());
      row.setRun(run.run());
      row.setRunState(run.runState());
      row.setInputs(toLegacyIo(run.inputs()));
      row.setOutputs(toLegacyIo(run.outputs()));
      row.setRunIoSnapshot(run.runIoSnapshot());
      row.setJobVersionBag(toLegacyJobVersion(run.jobVersion()));
    } else if (result instanceof JobProjectionResult job) {
      row.setNamespace(job.namespace());
      row.setJob(job.job());
      row.setInputs(toLegacyIo(job.inputs()));
      row.setOutputs(toLegacyIo(job.outputs()));
      row.setJobVersionBag(toLegacyJobVersion(job.jobVersion()));
    } else if (result instanceof DatasetProjectionResult dataset) {
      row.setNamespace(dataset.namespace());
      row.setOutputs(Optional.of(toLegacyIo(dataset.outputs())));
    } else {
      throw new IllegalArgumentException("Unsupported projection result " + result.getClass());
    }
    return row;
  }

  private static JobVersionDao.BagOfJobVersionInfo toLegacyJobVersion(
      JobVersionProjection projection) {
    return projection == null
        ? null
        : new JobVersionDao.BagOfJobVersionInfo(
            projection.job(), projection.jobVersion(), projection.inputs(), projection.outputs());
  }

  private static Optional<List<DatasetRecord>> toLegacyIo(
      Optional<List<DatasetProjection>> projections) {
    return projections.map(LineageTestUtils::toLegacyIo);
  }

  private static List<DatasetRecord> toLegacyIo(List<DatasetProjection> projections) {
    return projections.stream()
        .map(
            projection ->
                new DatasetRecord(
                    projection.dataset(),
                    projection.version(),
                    projection.namespace(),
                    projection.columnLineage()))
        .toList();
  }

  public static DatasetFacets newDatasetFacet(SchemaField... fields) {
    return newDatasetFacet(EMPTY_MAP, fields);
  }

  public static DatasetFacets newDatasetFacet(Map<String, Object> facets, SchemaField... fields) {
    return DatasetFacets.builder()
        .documentation(
            new DocumentationDatasetFacet(PRODUCER_URL, SCHEMA_URL, "the dataset documentation"))
        .schema(new SchemaDatasetFacet(PRODUCER_URL, SCHEMA_URL, Arrays.asList(fields)))
        .dataSource(
            new DatasourceDatasetFacet(
                PRODUCER_URL, SCHEMA_URL, "the source", "http://thesource.com"))
        .additional(facets)
        .build();
  }

  /**
   * Recursive function which supports writing a lineage graph by supplying an input dataset and a
   * list of {@link DatasetConsumerJob}s. Each consumer may output up to one dataset, which will be
   * consumed by the number of consumers specified by the {@link DatasetConsumerJob#numConsumers}
   * property.
   *
   * @param openLineageDao
   * @param downstream
   * @param jobFacet
   * @param dataset
   * @return
   */
  public static List<JobLineage> writeDownstreamLineage(
      OpenLineageDao openLineageDao,
      List<DatasetConsumerJob> downstream,
      JobFacet jobFacet,
      Dataset dataset) {
    DatasetConsumerJob consumer = downstream.get(0);
    return IntStream.range(0, consumer.getNumConsumers())
        .mapToObj(
            i -> {
              String jobName = consumer.getName() + i + "<-" + dataset.getName();
              Optional<Dataset> outputs =
                  consumer
                      .getOutputDatasetName()
                      .map(
                          dsName ->
                              new Dataset(
                                  NAMESPACE,
                                  dsName + "<-" + jobName,
                                  newDatasetFacet(
                                      new SchemaField("afield", "string", "a string field"),
                                      new SchemaField("anotherField", "string", "a string field"),
                                      new SchemaField("anInteger", "int", "an integer field"))));
              UpdateLineageRow row =
                  createLineageRow(
                      openLineageDao,
                      jobName,
                      "COMPLETE",
                      jobFacet,
                      Collections.singletonList(dataset),
                      outputs.stream().collect(Collectors.toList()));
              List<JobLineage> downstreamLineage =
                  outputs.stream()
                      .flatMap(
                          out -> {
                            if (consumer.numConsumers > 0) {
                              return writeDownstreamLineage(
                                  openLineageDao,
                                  downstream.subList(1, downstream.size()),
                                  jobFacet,
                                  out)
                                  .stream();
                            } else {
                              return Stream.empty();
                            }
                          })
                      .collect(Collectors.toList());
              return new JobLineage(
                  row.getJob().getUuid(),
                  row.getRun().getUuid(),
                  row.getJob().getName(),
                  row.getInputs().filter(l -> !l.isEmpty()).map(l -> l.get(0)),
                  row.getOutputs().filter(l -> !l.isEmpty()).map(l -> l.get(0)),
                  downstreamLineage);
            })
        .collect(Collectors.toList());
  }

  /**
   * Entity that encapsulates an existing job lineage- a job id, name, its input and output dataset
   * (if any) and a list of downstream jobs.
   */
  @Value
  public static class JobLineage {

    UUID id;
    UUID runId;
    String name;
    Optional<DatasetRecord> input;
    Optional<DatasetRecord> output;
    List<JobLineage> downstreamJobs;
  }

  /** Entity that encapsulates a dataset's consumer jobs and their output dataset names (if any). */
  @Value
  public static class DatasetConsumerJob {

    String name;
    int numConsumers;
    Optional<String> outputDatasetName;
  }
}
