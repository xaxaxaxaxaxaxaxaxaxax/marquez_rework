/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetType;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunState;
import marquez.common.models.SourceType;
import marquez.db.ColumnLineageDao.ColumnLineageDatasetWrite;
import marquez.db.ColumnLineageDao.ColumnLineageWrite;
import marquez.db.DatasetDao.DatasetCurrentVersionUpdate;
import marquez.db.DatasetDao.DatasetUpsert;
import marquez.db.DatasetFacetsDao.DatasetFacetWrite;
import marquez.db.DatasetFieldDao.DatasetFieldMapping;
import marquez.db.DatasetFieldDao.DatasetFieldUpsert;
import marquez.db.DatasetSymlinkDao.PrimaryDatasetSymlinkUpsert;
import marquez.db.JobVersionDao.BagOfJobVersionInfo;
import marquez.db.JobVersionDao.IoType;
import marquez.db.JobVersionDao.JobRowRunDetails;
import marquez.db.RunDao.RunUpsert;
import marquez.db.mappers.LineageEventMapper;
import marquez.db.models.ColumnLineageRow;
import marquez.db.models.DatasetFieldRow;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetSymlinkRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.InputFieldData;
import marquez.db.models.JobRow;
import marquez.db.models.ModelDaos;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunIoSnapshot;
import marquez.db.models.RunRow;
import marquez.db.models.RunStateRow;
import marquez.db.models.SourceRow;
import marquez.db.models.UpdateLineageRow;
import marquez.db.models.UpdateLineageRow.DatasetRecord;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.Dataset;
import marquez.service.models.LineageEvent.DatasetFacets;
import marquez.service.models.LineageEvent.DocumentationJobFacet;
import marquez.service.models.LineageEvent.Job;
import marquez.service.models.LineageEvent.JobFacet;
import marquez.service.models.LineageEvent.LifecycleStateChangeFacet;
import marquez.service.models.LineageEvent.NominalTimeRunFacet;
import marquez.service.models.LineageEvent.ParentRunFacet;
import marquez.service.models.LineageEvent.Run;
import marquez.service.models.LineageEvent.RunFacet;
import marquez.service.models.LineageEvent.SchemaDatasetFacet;
import marquez.service.models.LineageEvent.SchemaField;
import org.apache.commons.lang3.tuple.Pair;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transactional;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RegisterRowMapper(LineageEventMapper.class)
public interface OpenLineageDao extends BaseDao, Transactional<OpenLineageDao> {
  String DEFAULT_SOURCE_NAME = "default";
  String DEFAULT_NAMESPACE_OWNER = "anonymous";

  enum SpecEventType {
    RUN_EVENT,
    DATASET_EVENT,
    JOB_EVENT;
  }

  @SqlUpdate(
      "INSERT INTO lineage_events ("
          + "event_type, "
          + "event_time, "
          + "run_uuid, "
          + "job_name, "
          + "job_namespace, "
          + "event, "
          + "producer, "
          + "_event_type) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, 'RUN_EVENT')")
  void createLineageEvent(
      String eventType,
      Instant eventTime,
      UUID runUuid,
      String jobName,
      String jobNamespace,
      PGobject event,
      String producer);

  @SqlUpdate(
      "INSERT INTO lineage_events ("
          + "event_time, "
          + "event, "
          + "producer, "
          + "_event_type) "
          + "VALUES (?, ?, ?, 'DATASET_EVENT')")
  void createDatasetEvent(Instant eventTime, PGobject event, String producer);

  @SqlUpdate(
      "INSERT INTO lineage_events ("
          + "event_time, "
          + "job_name, "
          + "job_namespace, "
          + "event, "
          + "producer, "
          + "_event_type) "
          + "VALUES (?, ?, ?, ?, ?, 'JOB_EVENT')")
  void createJobEvent(
      Instant eventTime, String jobName, String jobNamespace, PGobject event, String producer);

  @SqlQuery(
      "SELECT event FROM lineage_events WHERE run_uuid = :runUuid AND _event_type='RUN_EVENT'")
  List<LineageEvent> findLineageEventsByRunUuid(UUID runUuid);

  @SqlQuery(
      """
  SELECT event
  FROM lineage_events le
  WHERE (le.event_time < :before
  AND le.event_time >= :after)
  AND le._event_type='RUN_EVENT'
  ORDER BY le.event_time DESC
  LIMIT :limit OFFSET :offset""")
  List<LineageEvent> getAllLineageEventsDesc(
      ZonedDateTime before, ZonedDateTime after, int limit, int offset);

  @SqlQuery(
      """
  SELECT event
  FROM lineage_events le
  WHERE (le.event_time < :before
  AND le.event_time >= :after)
  AND le._event_type='RUN_EVENT'
  ORDER BY le.event_time ASC
  LIMIT :limit OFFSET :offset""")
  List<LineageEvent> getAllLineageEventsAsc(
      ZonedDateTime before, ZonedDateTime after, int limit, int offset);

  @SqlQuery(
      """
      SELECT count(*)
      FROM lineage_events le
      WHERE (le.event_time < :before
      AND le.event_time >= :after)""")
  int getAllLineageTotalCount(ZonedDateTime before, ZonedDateTime after);

  default UpdateLineageRow updateMarquezModel(LineageEvent event, ObjectMapper mapper) {
    return updateMarquezModel(event, mapper, true);
  }

  default UpdateLineageRow updateMarquezModel(
      LineageEvent event, ObjectMapper mapper, boolean listenerSnapshotRequired) {
    return inTransaction(
        transactional ->
            transactional.updateMarquezModelInTransaction(event, mapper, listenerSnapshotRequired));
  }

  private UpdateLineageRow updateMarquezModelInTransaction(
      LineageEvent event, ObjectMapper mapper, boolean listenerSnapshotRequired) {
    Instant now = event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant();
    UUID runUuid = runToUuid(event.getRun().getRunId());
    ModelDaos daos = new ModelDaos(this);
    LineageWriteContext context = LineageWriteContext.forIntake(daos, runUuid);
    UpdateLineageRow updateLineageRow = updateBaseMarquezModel(event, mapper, now, context);
    RunState runState = getRunState(event.getEventType());
    boolean streaming = event.getJob() != null && event.getJob().isStreamingJob();

    RunIoSnapshot runIoSnapshot = null;
    if (streaming
        || (event.getEventType() != null && (runState.isDone() || listenerSnapshotRequired))) {
      runIoSnapshot = daos.getJobVersionDao().findRunIoSnapshot(runUuid);
      updateLineageRow.setRunIoSnapshot(runIoSnapshot);
    }

    if (streaming) {
      updateMarquezOnStreamingJob(event, updateLineageRow, runState, daos, runIoSnapshot);
    } else if (event.getEventType() != null && runState.isDone()) {
      updateMarquezOnComplete(event, updateLineageRow, runState, daos, runIoSnapshot);
    }

    if (runIoSnapshot != null && (streaming || "complete".equalsIgnoreCase(event.getEventType()))) {
      updateOutputDatasetVersions(daos, runIoSnapshot, Instant.now());
    }
    return updateLineageRow;
  }

  default UpdateLineageRow updateMarquezModel(DatasetEvent event, ObjectMapper mapper) {
    return inTransaction(
        transactional -> transactional.updateMarquezModelInTransaction(event, mapper));
  }

  private UpdateLineageRow updateMarquezModelInTransaction(
      DatasetEvent event, ObjectMapper mapper) {
    ModelDaos daos = new ModelDaos(this);
    LineageWriteContext context = LineageWriteContext.forIntake(daos, null);
    Instant now = event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant();

    UpdateLineageRow bag = new UpdateLineageRow();
    NamespaceRow namespace =
        context.upsertNamespace(formatNamespaceName(event.getDataset().getNamespace()), now);
    bag.setNamespace(namespace);

    Dataset dataset = event.getDataset();
    DatasetRecord record =
        upsertLineageDatasets(context, List.of(dataset), now, null, false).get(0);
    context.queueDatasetFacets(dataset, record, null, null, now, false);
    context.flushDatasetFacets();
    context.flushColumnLineage(now);

    daos.getDatasetDao()
        .updateVersionsInTransaction(
            List.of(
                new DatasetCurrentVersionUpdate(
                    record.getDatasetRow().getUuid(),
                    Instant.now(),
                    record.getDatasetVersionRow().getUuid())));

    bag.setOutputs(Optional.of(List.of(record)));
    return bag;
  }

  default UpdateLineageRow updateMarquezModel(JobEvent event, ObjectMapper mapper) {
    return inTransaction(
        transactional -> transactional.updateMarquezModelInTransaction(event, mapper));
  }

  private UpdateLineageRow updateMarquezModelInTransaction(JobEvent event, ObjectMapper mapper) {
    ModelDaos daos = new ModelDaos(this);
    LineageWriteContext context = LineageWriteContext.forIntake(daos, null);
    Instant now = event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant();

    UpdateLineageRow bag = new UpdateLineageRow();
    NamespaceRow namespace =
        context.upsertNamespace(formatNamespaceName(event.getJob().getNamespace()), now);
    bag.setNamespace(namespace);

    JobRow job =
        buildJobFromEvent(
            event.getJob(),
            event.getEventTime(),
            null,
            Collections.emptyList(),
            mapper,
            daos,
            now,
            namespace,
            null,
            null,
            Optional.empty(),
            null);
    bag.setJob(job);

    List<DatasetRecord> datasetInputs =
        event.getInputs() == null
            ? new ArrayList<>()
            : upsertLineageDatasets(context, event.getInputs(), now, null, true);
    if (event.getInputs() != null) {
      for (int index = 0; index < event.getInputs().size(); index++) {
        context.queueDatasetFacets(
            event.getInputs().get(index), datasetInputs.get(index), null, null, now, true);
      }
    }
    bag.setInputs(Optional.of(datasetInputs));
    context.flushInputMappings();

    List<DatasetRecord> datasetOutputs =
        event.getOutputs() == null
            ? new ArrayList<>()
            : upsertLineageDatasets(context, event.getOutputs(), now, null, false);
    if (event.getOutputs() != null) {
      for (int index = 0; index < event.getOutputs().size(); index++) {
        context.queueDatasetFacets(
            event.getOutputs().get(index), datasetOutputs.get(index), null, null, now, false);
      }
    }
    bag.setOutputs(Optional.of(datasetOutputs));
    context.flushDatasetFacets();
    context.flushColumnLineage(now);

    BagOfJobVersionInfo bagOfJobVersionInfo =
        daos.getJobVersionDao()
            .upsertRunlessJobVersionInTransaction(job, namespace, datasetInputs, datasetOutputs);

    // Runless job facets reference the immutable version and therefore follow its upsert.
    Optional.ofNullable(event.getJob().getFacets())
        .ifPresent(
            jobFacet ->
                daos.getJobFacetsDao()
                    .insertJobFacetsFor(
                        job.getUuid(),
                        bagOfJobVersionInfo.getJobVersionRow().getUuid(),
                        now,
                        jobFacet));

    bag.setJobVersionBag(bagOfJobVersionInfo);
    return bag;
  }

  /** Compatibility entry point for callers that need only the base run projection. */
  default UpdateLineageRow updateBaseMarquezModel(LineageEvent event, ObjectMapper mapper) {
    return inTransaction(
        transactional -> transactional.updateBaseMarquezModelInTransaction(event, mapper));
  }

  private UpdateLineageRow updateBaseMarquezModelInTransaction(
      LineageEvent event, ObjectMapper mapper) {
    Instant now = event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant();
    UUID runUuid = runToUuid(event.getRun().getRunId());
    ModelDaos daos = new ModelDaos(this);
    LineageWriteContext context = LineageWriteContext.forIntake(daos, runUuid);
    UpdateLineageRow bag = updateBaseMarquezModel(event, mapper, now, context);
    if (event.getEventType() != null) {
      bag.setRunIoSnapshot(daos.getJobVersionDao().findRunIoSnapshot(runUuid));
    }
    return bag;
  }

  private UpdateLineageRow updateBaseMarquezModel(
      LineageEvent event, ObjectMapper mapper, Instant now, LineageWriteContext context) {
    ModelDaos daos = context.daos();
    UpdateLineageRow bag = new UpdateLineageRow();
    NamespaceRow namespace =
        context.upsertNamespace(formatNamespaceName(event.getJob().getNamespace()), now);
    bag.setNamespace(namespace);

    Instant nominalStartTime = getNominalStartTime(event);
    Instant nominalEndTime = getNominalEndTime(event);
    Optional<ParentRunFacet> parentRun =
        Optional.ofNullable(event.getRun()).map(Run::getFacets).map(RunFacet::getParent);
    Optional<UUID> parentUuid = parentRun.map(Utils::findParentRunUuid);
    UUID runUuid = context.runUuid();

    JobRow job =
        buildJobFromEvent(
            event.getJob(),
            event.getEventTime(),
            event.getEventType(),
            event.getInputs(),
            mapper,
            daos,
            now,
            namespace,
            nominalStartTime,
            nominalEndTime,
            parentRun,
            runUuid);
    bag.setJob(job);

    Map<String, String> runArgsMap = createRunArgs(event);
    RunArgsRow runArgs =
        daos.getRunArgsDao()
            .upsertRunArgs(
                UUID.randomUUID(), now, Utils.toJson(runArgsMap), Utils.checksumFor(runArgsMap));
    bag.setRunArgs(runArgs);

    RunUpsert.RunUpsertBuilder runUpsertBuilder =
        RunUpsert.builder()
            .runUuid(runUuid)
            .parentRunUuid(parentUuid.orElse(null))
            .externalId(event.getRun().getRunId())
            .now(now)
            .jobUuid(job.getUuid())
            .jobVersionUuid(null)
            .runArgsUuid(runArgs.getUuid())
            .namespaceName(namespace.getName())
            .jobName(job.getName())
            .location(job.getLocation());
    if (event.getEventType() != null) {
      runUpsertBuilder.runStateType(getRunState(event.getEventType())).runStateTime(now);
    }
    RunRow run = daos.getRunDao().upsert(runUpsertBuilder.build());
    insertRunFacets(daos, event, runUuid, now);
    bag.setRun(run);

    if (event.getEventType() != null) {
      RunState runStateType = getRunState(event.getEventType());
      RunStateRow runState =
          daos.getRunStateDao()
              .insertAndLinkRunState(UUID.randomUUID(), now, run.getUuid(), runStateType);
      bag.setRunState(runState);
    }

    // These rows must exist before a terminal transition links them to a job version.
    insertJobFacets(daos, event.getJob(), event.getEventType(), job.getUuid(), runUuid, now);

    // A null list remains the sentinel for a run event that did not report this side of its I/O.
    boolean inputsMissing = event.getInputs() == null || event.getInputs().isEmpty();
    boolean outputsMissing = event.getOutputs() == null || event.getOutputs().isEmpty();
    boolean suppressMissingIoInvalidation = event.isTerminalEventForStreamingJobWithNoDatasets();
    List<DatasetRecord> datasetInputs = null;
    if (!inputsMissing) {
      datasetInputs = upsertLineageDatasets(context, event.getInputs(), now, runUuid, true);
      for (int index = 0; index < event.getInputs().size(); index++) {
        context.queueDatasetFacets(
            event.getInputs().get(index),
            datasetInputs.get(index),
            runUuid,
            event.getEventType(),
            now,
            true);
      }
    } else if (!suppressMissingIoInvalidation) {
      if (outputsMissing) {
        daos.getJobVersionDao().markInputAndOutputDatasetsAsPreviousFor(job.getUuid());
      } else {
        daos.getJobVersionDao().markInputOrOutputDatasetAsPreviousFor(job.getUuid(), IoType.INPUT);
      }
    }
    bag.setInputs(Optional.ofNullable(datasetInputs));

    // Column lineage resolves through runs_input_mapping, so publish every input before outputs.
    context.flushInputMappings();

    List<DatasetRecord> datasetOutputs = null;
    if (!outputsMissing) {
      datasetOutputs = upsertLineageDatasets(context, event.getOutputs(), now, runUuid, false);
      for (int index = 0; index < event.getOutputs().size(); index++) {
        context.queueDatasetFacets(
            event.getOutputs().get(index),
            datasetOutputs.get(index),
            runUuid,
            event.getEventType(),
            now,
            false);
      }
    } else if (!suppressMissingIoInvalidation && !inputsMissing) {
      daos.getJobVersionDao().markInputOrOutputDatasetAsPreviousFor(job.getUuid(), IoType.OUTPUT);
    }
    bag.setOutputs(Optional.ofNullable(datasetOutputs));

    context.flushDatasetFacets();
    context.flushColumnLineage(now);
    return bag;
  }

  private static Instant getNominalStartTime(LineageEvent event) {
    return Optional.ofNullable(event.getRun().getFacets())
        .flatMap(f -> Optional.ofNullable(f.getNominalTime()))
        .map(NominalTimeRunFacet::getNominalStartTime)
        .map(t -> t.withZoneSameInstant(ZoneId.of("UTC")).toInstant())
        .orElse(null);
  }

  private static Instant getNominalEndTime(LineageEvent event) {
    return Optional.ofNullable(event.getRun().getFacets())
        .flatMap(f -> Optional.ofNullable(f.getNominalTime()))
        .map(NominalTimeRunFacet::getNominalEndTime)
        .map(t -> t.withZoneSameInstant(ZoneId.of("UTC")).toInstant())
        .orElse(null);
  }

  private void insertRunFacets(ModelDaos daos, LineageEvent event, UUID runUuid, Instant now) {
    // Add ...
    Optional.ofNullable(event.getRun().getFacets())
        .ifPresent(
            runFacet ->
                daos.getRunFacetsDao()
                    .insertRunFacetsFor(
                        runUuid, now, event.getEventType(), event.getRun().getFacets()));
  }

  private void insertJobFacets(
      ModelDaos daos, Job job, String eventType, UUID jobUuid, UUID runUuid, Instant now) {
    // Add ...
    Optional.ofNullable(job.getFacets())
        .ifPresent(
            jobFacet ->
                daos.getJobFacetsDao()
                    .insertJobFacetsFor(jobUuid, runUuid, now, eventType, job.getFacets()));
  }

  private JobRow buildJobFromEvent(
      Job job,
      ZonedDateTime eventTime,
      String eventType,
      List<Dataset> inputs,
      ObjectMapper mapper,
      ModelDaos daos,
      Instant now,
      NamespaceRow namespace,
      Instant nominalStartTime,
      Instant nominalEndTime,
      Optional<ParentRunFacet> parentRun,
      @Nullable UUID runUuid) {
    Logger log = LoggerFactory.getLogger(OpenLineageDao.class);
    JobDao jobDao = daos.getJobDao();
    String description =
        Optional.ofNullable(job.getFacets())
            .map(JobFacet::getDocumentation)
            .map(DocumentationJobFacet::getDescription)
            .orElse(null);

    String location =
        Optional.ofNullable(job.getFacets())
            .flatMap(f -> Optional.ofNullable(f.getSourceCodeLocation()))
            .flatMap(s -> Optional.ofNullable(s.getUrl()))
            .orElse(null);

    Optional<UUID> parentRunUuid = parentRun.map(Utils::findParentRunUuid);
    Optional<JobRow> parentJob =
        parentRunUuid.map(
            parentRunUuidFound ->
                findParentJobRow(
                    daos,
                    job,
                    eventTime,
                    eventType,
                    namespace,
                    location,
                    nominalStartTime,
                    nominalEndTime,
                    log,
                    parentRun.get(),
                    parentRunUuidFound));

    // construct the simple name of the job by removing the parent prefix plus the dot '.' separator
    String jobName =
        parentJob
            .map(
                p -> {
                  if (job.getName().startsWith(p.getName() + '.')) {
                    return job.getName().substring(p.getName().length() + 1);
                  } else {
                    return job.getName();
                  }
                })
            .orElse(job.getName());
    log.debug(
        "Calculated job name {} from job {} with parent {}",
        jobName,
        job.getName(),
        parentJob.map(JobRow::getName));
    JobRow upsertedJob =
        parentJob
            .map(
                parent ->
                    jobDao.upsertJob(
                        UUID.randomUUID(),
                        parent.getUuid(),
                        job.type(),
                        now,
                        namespace.getUuid(),
                        namespace.getName(),
                        jobName,
                        description,
                        location,
                        null,
                        jobDao.toJson(toDatasetId(inputs), mapper),
                        parent.getCurrentRunUuid().orElse(null)))
            .orElseGet(
                () ->
                    jobDao.upsertJob(
                        UUID.randomUUID(),
                        job.type(),
                        now,
                        namespace.getUuid(),
                        namespace.getName(),
                        jobName,
                        description,
                        location,
                        null,
                        jobDao.toJson(toDatasetId(inputs), mapper),
                        runUuid));
    String requestedFullName =
        parentJob.map(parent -> parent.getName() + "." + jobName).orElse(jobName);
    return lockCanonicalJobIfAliased(jobDao, upsertedJob, namespace.getName(), requestedFullName);
  }

  private JobRow lockCanonicalJobIfAliased(
      JobDao jobDao, JobRow upsertedJob, String requestedNamespace, String requestedFullName) {
    if (requestedNamespace.equals(upsertedJob.getNamespaceName())
        && requestedFullName.equals(upsertedJob.getName())) {
      // INSERT ... ON CONFLICT already holds this primary row's lock.
      return upsertedJob;
    }
    return jobDao.lockJobByUuid(upsertedJob.getUuid());
  }

  private JobRow findParentJobRow(
      ModelDaos daos,
      Job job,
      ZonedDateTime eventTime,
      String eventType,
      NamespaceRow namespace,
      String location,
      Instant nominalStartTime,
      Instant nominalEndTime,
      Logger log,
      ParentRunFacet facet,
      UUID parentRunUuid) {
    try {
      log.debug("Found parent run event {}", facet);
      PGobject inputs = new PGobject();
      inputs.setType("json");
      inputs.setValue("[]");
      JobRow parentJobRow =
          daos.getRunDao()
              .findJobRowByRunUuid(parentRunUuid)
              .map(
                  j -> {
                    String parentJobName =
                        facet.getJob().getName().equals(job.getName())
                            ? Utils.parseParentJobName(facet.getJob().getName())
                            : facet.getJob().getName();
                    if (j.getNamespaceName().equals(facet.getJob().getNamespace())
                        && j.getName().equals(parentJobName)) {
                      return j;
                    } else {
                      // Addresses an Airflow integration bug that generated conflicting run UUIDs
                      // for DAGs that had the same name, but ran in different namespaces.
                      UUID parentRunUuidNoConflict =
                          Utils.toNameBasedUuid(
                              facet.getJob().getNamespace(),
                              parentJobName,
                              parentRunUuid.toString());
                      log.warn(
                          "Parent Run id {} has a different job name '{}.{}' from facet '{}.{}'. "
                              + "Assuming Run UUID conflict and generating a new UUID {}",
                          parentRunUuid,
                          j.getNamespaceName(),
                          j.getName(),
                          facet.getJob().getNamespace(),
                          facet.getJob().getName(),
                          parentRunUuidNoConflict);
                      return createParentJobRunRecord(
                          daos,
                          job,
                          eventTime,
                          eventType,
                          namespace,
                          location,
                          nominalStartTime,
                          nominalEndTime,
                          parentRunUuidNoConflict,
                          facet,
                          inputs);
                    }
                  })
              .orElseGet(
                  () ->
                      createParentJobRunRecord(
                          daos,
                          job,
                          eventTime,
                          eventType,
                          namespace,
                          location,
                          nominalStartTime,
                          nominalEndTime,
                          parentRunUuid,
                          facet,
                          inputs));
      log.debug("Found parent job record {}", parentJobRow);
      return parentJobRow;
    } catch (Exception e) {
      throw new RuntimeException("Unable to insert parent run", e);
    }
  }

  private JobRow createParentJobRunRecord(
      ModelDaos daos,
      Job job,
      ZonedDateTime eventTime,
      String eventType,
      NamespaceRow namespace,
      String location,
      Instant nominalStartTime,
      Instant nominalEndTime,
      UUID parentRunUuid,
      ParentRunFacet facet,
      PGobject inputs) {
    Instant now = eventTime.withZoneSameInstant(ZoneId.of("UTC")).toInstant();
    Logger log = LoggerFactory.getLogger(OpenLineageDao.class);
    String parentJobName =
        facet.getJob().getName().equals(job.getName())
            ? Utils.parseParentJobName(facet.getJob().getName())
            : facet.getJob().getName();
    JobRow upsertedParentJob =
        daos.getJobDao()
            .upsertJob(
                UUID.randomUUID(),
                job.type(),
                now,
                namespace.getUuid(),
                namespace.getName(),
                parentJobName,
                null,
                location,
                null,
                inputs,
                parentRunUuid);
    JobRow newParentJobRow =
        lockCanonicalJobIfAliased(
            daos.getJobDao(), upsertedParentJob, namespace.getName(), parentJobName);
    log.info("Created new parent job record {}", newParentJobRow);

    RunArgsRow argsRow =
        daos.getRunArgsDao()
            .upsertRunArgs(UUID.randomUUID(), now, "{}", Utils.checksumFor(ImmutableMap.of()));

    Optional<RunState> runState = Optional.ofNullable(eventType).map(this::getRunState);
    RunDao runDao = daos.getRunDao();
    RunRow newRow =
        runDao.upsert(
            parentRunUuid,
            null,
            facet.getRun().getRunId(),
            now,
            newParentJobRow.getUuid(),
            null,
            argsRow.getUuid(),
            nominalStartTime,
            nominalEndTime,
            runState.orElse(null),
            now,
            namespace.getName(),
            newParentJobRow.getName(),
            newParentJobRow.getLocation());
    log.info("Created new parent run record {}", newRow);

    runState
        .map(rs -> daos.getRunStateDao().upsert(UUID.randomUUID(), now, parentRunUuid, rs))
        .ifPresent(
            runStateRow -> {
              UUID runStateUuid = runStateRow.getUuid();
              if (RunState.valueOf(runStateRow.getState()).isDone()) {
                runDao.updateEndState(parentRunUuid, now, runStateUuid);
              } else {
                runDao.updateStartState(parentRunUuid, now, runStateUuid);
              }
            });

    return newParentJobRow;
  }

  default Set<DatasetId> toDatasetId(List<Dataset> datasets) {
    Set<DatasetId> set = new HashSet<>();
    if (datasets == null) {
      return set;
    }
    for (Dataset dataset : datasets) {
      set.add(
          new DatasetId(
              NamespaceName.of(dataset.getNamespace()), DatasetName.of(dataset.getName())));
    }
    return set;
  }

  /** Compatibility entry point for callers that project a completed run transition directly. */
  default void updateMarquezOnComplete(
      LineageEvent event, UpdateLineageRow updateLineageRow, RunState runState) {
    JobVersionDao jobVersionDao = createJobVersionDao();
    BagOfJobVersionInfo bagOfJobVersionInfo =
        jobVersionDao.upsertJobVersionOnRunTransition(
            jobVersionDao.loadJobRowRunDetails(
                updateLineageRow.getJob(), updateLineageRow.getRun().getUuid()),
            runState,
            event.getEventTime().toInstant(),
            runState.isDone());
    updateLineageRow.setJobVersionBag(bagOfJobVersionInfo);
  }

  private void updateMarquezOnComplete(
      LineageEvent event,
      UpdateLineageRow updateLineageRow,
      RunState runState,
      ModelDaos daos,
      RunIoSnapshot runIoSnapshot) {
    if (runIoSnapshot == null) {
      throw new IllegalStateException("A terminal run event requires a cumulative I/O snapshot");
    }
    JobVersionDao jobVersionDao = daos.getJobVersionDao();
    // Link the job version to the job only if the run is marked done and has transitioned into one
    // of the following states: COMPLETED, ABORTED, or FAILED.
    final boolean linkJobToJobVersion = runState.isDone();

    BagOfJobVersionInfo bagOfJobVersionInfo =
        jobVersionDao.upsertJobVersionOnRunTransitionInTransaction(
            jobVersionDao.loadJobRowRunDetails(
                updateLineageRow.getJob(),
                updateLineageRow.getNamespace(),
                updateLineageRow.getRun().getUuid(),
                runIoSnapshot),
            runState,
            event.getEventTime().toInstant(),
            linkJobToJobVersion);
    updateLineageRow.setJobVersionBag(bagOfJobVersionInfo);
  }

  /**
   * A separate method is used as the logic to update Marquez model differs for streaming and batch.
   * The assumption for batch is that the job version is created when task is done and cumulative
   * list of input and output datasets from all the events is used to compute the job version UUID.
   * However, this wouldn't make sense for streaming jobs, which are mostly long living and produce
   * output before completing.
   *
   * <p>In this case, a job version is created based on the list of input and output datasets
   * referenced by this job. If a job starts with inputs:{A,B} and outputs:{C}, new job version is
   * created immediately at job start. If a following event produces inputs:{A}, outputs:{C}, then
   * the union of all datasets registered within this job does not change, and thus job version does
   * not get modified. In case of receiving another event with no inputs nor outputs, job version
   * still will not get modified as its hash is evaluated based on the datasets attached to the run.
   *
   * <p>However, in case of event with inputs:{A,B,D} and outputs:{C}, new hash gets computed and
   * new job version row is inserted into the table.
   *
   * @param event
   * @param updateLineageRow
   * @param runState
   */
  default void updateMarquezOnStreamingJob(
      LineageEvent event, UpdateLineageRow updateLineageRow, RunState runState) {
    JobVersionDao jobVersionDao = createJobVersionDao();
    JobRowRunDetails jobRowRunDetails =
        jobVersionDao.loadJobRowRunDetails(
            updateLineageRow.getJob(), updateLineageRow.getRun().getUuid());

    if (event.isTerminalEventForStreamingJobWithNoDatasets()) {
      return;
    }

    if (!jobVersionDao.versionExists(jobRowRunDetails.jobVersion().getValue())) {
      BagOfJobVersionInfo bagOfJobVersionInfo =
          jobVersionDao.upsertJobVersionOnRunTransition(
              jobRowRunDetails, runState, event.getEventTime().toInstant(), true);
      updateLineageRow.setJobVersionBag(bagOfJobVersionInfo);
    }
  }

  private void updateMarquezOnStreamingJob(
      LineageEvent event,
      UpdateLineageRow updateLineageRow,
      RunState runState,
      ModelDaos daos,
      RunIoSnapshot runIoSnapshot) {
    if (runIoSnapshot == null) {
      throw new IllegalStateException("A streaming run event requires a cumulative I/O snapshot");
    }
    JobVersionDao jobVersionDao = daos.getJobVersionDao();
    JobRowRunDetails jobRowRunDetails =
        jobVersionDao.loadJobRowRunDetails(
            updateLineageRow.getJob(),
            updateLineageRow.getNamespace(),
            updateLineageRow.getRun().getUuid(),
            runIoSnapshot);

    if (event.isTerminalEventForStreamingJobWithNoDatasets()) {
      return;
    }

    if (!jobVersionDao.versionExists(jobRowRunDetails.jobVersion().getValue())) {
      // need to insert new job version
      BagOfJobVersionInfo bagOfJobVersionInfo =
          jobVersionDao.upsertJobVersionOnRunTransitionInTransaction(
              jobRowRunDetails, runState, event.getEventTime().toInstant(), true);
      updateLineageRow.setJobVersionBag(bagOfJobVersionInfo);
    }
  }

  private void updateOutputDatasetVersions(
      ModelDaos daos, RunIoSnapshot runIoSnapshot, Instant now) {
    List<DatasetCurrentVersionUpdate> updates = new ArrayList<>(runIoSnapshot.getOutputs().size());
    runIoSnapshot
        .getOutputs()
        .forEach(
            output ->
                updates.add(
                    new DatasetCurrentVersionUpdate(
                        output.getDatasetUuid(), now, output.getUuid())));
    if (!updates.isEmpty()) {
      daos.getDatasetDao().updateVersionsInTransaction(updates);
    }
  }

  default String getUrlOrNull(String uri) {
    if (uri == null) {
      return "";
    }
    try {
      return new URI(uri).toASCIIString();
    } catch (URISyntaxException e) {
      try {
        // assume host as string
        return new URI("http://" + uri).toASCIIString();
      } catch (Exception ex) {
        return null;
      }
    }
  }

  default String formatNamespaceName(String namespace) {
    return namespace.replaceAll("[^a-z:/A-Z0-9\\-_.@+]", "_");
  }

  default DatasetRecord upsertLineageDataset(
      ModelDaos daos, Dataset ds, Instant now, UUID runUuid, boolean isInput) {
    LineageWriteContext context = LineageWriteContext.forCompatibility(daos, runUuid);
    DatasetRecord record = upsertLineageDatasetSequentially(context, ds, now, runUuid, isInput);
    context.flushInputMappings();
    return record;
  }

  private List<DatasetRecord> upsertLineageDatasets(
      LineageWriteContext context,
      List<Dataset> datasets,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    if (datasets.isEmpty()) {
      return new ArrayList<>();
    }

    // Alias writes and repeated primary identities are occurrence-sensitive. Do not perform any
    // speculative base writes before taking their legacy sequential path.
    if (!canStageDatasetBaseWrites(datasets)) {
      return upsertLineageDatasetsSequentially(context, datasets, now, runUuid, isInput);
    }

    // Namespace and source resolution intentionally stays in encounter order. Only the primary
    // symlink resolution and dataset upserts are staged across this one I/O side.
    List<LineageWriteContext.PreparedLineageDatasetBase> preparedBases =
        new ArrayList<>(datasets.size());
    List<PrimaryDatasetSymlinkUpsert> primarySymlinkWrites = new ArrayList<>(datasets.size());
    for (Dataset dataset : datasets) {
      LineageWriteContext.PreparedLineageDatasetBase preparedBase =
          prepareLineageDatasetBase(context, dataset, now);
      preparedBases.add(preparedBase);
      primarySymlinkWrites.add(preparedBase.primarySymlinkWrite());
    }

    List<DatasetSymlinkRow> symlinks =
        context
            .daos()
            .getDatasetSymlinkDao()
            .resolvePrimarySymlinksInTransaction(primarySymlinkWrites);
    List<String> lifecycleStates = new ArrayList<>(datasets.size());
    List<DatasetUpsert> datasetUpserts = new ArrayList<>(datasets.size());
    Set<UUID> datasetUuids = new LinkedHashSet<>();
    boolean repeatedDatasetUuid = false;
    for (int index = 0; index < preparedBases.size(); index++) {
      LineageWriteContext.PreparedLineageDatasetBase preparedBase = preparedBases.get(index);
      DatasetSymlinkRow symlink = symlinks.get(index);
      String lifecycleState = getDatasetLifecycleState(preparedBase.dataset());
      lifecycleStates.add(lifecycleState);
      datasetUpserts.add(toDatasetUpsert(preparedBase, symlink, lifecycleState, now));
      repeatedDatasetUuid |= !datasetUuids.add(symlink.getUuid());
    }

    // DatasetDao deliberately executes duplicate UUIDs one at a time and returns each
    // intermediate row. Its result remains aligned with the original occurrence order.
    List<DatasetRow> datasetRows =
        context.daos().getDatasetDao().upsertAllInTransaction(datasetUpserts);
    List<LineageWriteContext.PreparedLineageDataset> prepared = new ArrayList<>(datasets.size());
    for (int index = 0; index < preparedBases.size(); index++) {
      LineageWriteContext.PreparedLineageDatasetBase preparedBase = preparedBases.get(index);
      List<SchemaField> fields =
          Optional.ofNullable(preparedBase.dataset().getFacets())
              .map(DatasetFacets::getSchema)
              .map(SchemaDatasetFacet::getFields)
              .orElse(null);
      prepared.add(
          new LineageWriteContext.PreparedLineageDataset(
              preparedBase.dataset(),
              preparedBase.rawNamespace(),
              preparedBase.datasetNamespace(),
              preparedBase.source(),
              symlinks.get(index),
              datasetRows.get(index),
              lifecycleStates.get(index),
              fields));
    }

    if (repeatedDatasetUuid) {
      // Distinct primary names can still be aliases for one physical dataset. Finish the already
      // staged occurrences sequentially, carrying current-version state forward. DatasetDao has
      // already preserved the occurrence-by-occurrence base rows for this path.
      return upsertPreparedLineageDatasetsSequentially(context, prepared, now, runUuid, isInput);
    }

    List<DatasetFieldUpsert> fieldUpserts = new ArrayList<>();
    for (LineageWriteContext.PreparedLineageDataset occurrence : prepared) {
      for (SchemaField field : occurrence.fieldsOrEmpty()) {
        fieldUpserts.add(
            new DatasetFieldUpsert(
                UUID.randomUUID(),
                now,
                now,
                occurrence.datasetRow().getUuid(),
                field.getName(),
                field.getType(),
                field.getDescription()));
      }
    }
    List<DatasetFieldRow> allDatasetFields =
        context.daos().getDatasetFieldDao().upsertAllInTransaction(fieldUpserts);

    Map<UUID, DatasetVersionRow> currentInputVersions = new LinkedHashMap<>();
    if (isInput) {
      Set<UUID> currentVersionUuids = new LinkedHashSet<>();
      for (LineageWriteContext.PreparedLineageDataset occurrence : prepared) {
        occurrence.datasetRow().getCurrentVersionUuid().ifPresent(currentVersionUuids::add);
      }
      for (DatasetVersionRow versionRow :
          context.daos().getDatasetVersionDao().findRowsByUuids(currentVersionUuids)) {
        currentInputVersions.put(versionRow.getUuid(), versionRow);
      }
    }

    List<DatasetRecord> records = new ArrayList<>(prepared.size());
    List<DatasetFieldMapping> fieldMappings = new ArrayList<>(allDatasetFields.size());
    List<DatasetCurrentVersionUpdate> currentVersionUpdates = new ArrayList<>();
    int fieldOffset = 0;
    for (int index = 0; index < prepared.size(); index++) {
      LineageWriteContext.PreparedLineageDataset occurrence = prepared.get(index);
      int nextFieldOffset = fieldOffset + occurrence.fieldsOrEmpty().size();
      List<DatasetFieldRow> datasetFields = allDatasetFields.subList(fieldOffset, nextFieldOffset);
      fieldOffset = nextFieldOffset;
      DatasetRow datasetRow = occurrence.datasetRow();
      DatasetVersionRow datasetVersionRow =
          isInput
              ? datasetRow.getCurrentVersionUuid().map(currentInputVersions::get).orElse(null)
              : null;
      if (datasetVersionRow == null) {
        datasetVersionRow =
            createDatasetVersion(context, occurrence, datasetFields, now, runUuid, isInput);
      }

      for (DatasetFieldRow datasetField : datasetFields) {
        fieldMappings.add(
            new DatasetFieldMapping(datasetVersionRow.getUuid(), datasetField.getUuid()));
      }

      if (isInput && runUuid != null) {
        context.queueInputMapping(datasetVersionRow.getUuid());
        if (datasetRow.getCurrentVersionUuid().isEmpty()) {
          currentVersionUpdates.add(
              new DatasetCurrentVersionUpdate(
                  datasetRow.getUuid(), now, datasetVersionRow.getUuid()));
          datasetRow = datasetRow.withCurrentVersionUuid(datasetVersionRow.getUuid());
        }
      }

      List<ColumnLineageRow> columnLineageRows = Collections.emptyList();
      if (!isInput) {
        // For run intake, input mappings are already visible. Physical preparation only queues
        // UUIDs; the output mappings below still precede the event-level column-lineage flush.
        columnLineageRows =
            context.collectColumnLineage(
                occurrence.dataset(), now, datasetFields, datasetVersionRow);
      }
      records.add(
          new DatasetRecord(
              datasetRow, datasetVersionRow, occurrence.datasetNamespace(), columnLineageRows));
    }

    // The caller publishes run input mappings only after the input side returns. Output physical
    // preparation above may resolve those inputs, but the lineage write is flushed still later,
    // after this output mapping and the facets.
    context.daos().getDatasetFieldDao().updateFieldMappingInTransaction(fieldMappings);
    context.daos().getDatasetDao().updateVersionsInTransaction(currentVersionUpdates);
    return records;
  }

  private boolean canStageDatasetBaseWrites(List<Dataset> datasets) {
    Map<String, Set<String>> primaryNamesByNamespace = new LinkedHashMap<>();
    for (Dataset dataset : datasets) {
      if (dataset.getFacets() != null && dataset.getFacets().getSymlinks() != null) {
        return false;
      }
      if (!primaryNamesByNamespace
          .computeIfAbsent(dataset.getNamespace(), ignored -> new LinkedHashSet<>())
          .add(formatDatasetName(dataset.getName()))) {
        return false;
      }
    }
    return true;
  }

  private List<DatasetRecord> upsertLineageDatasetsSequentially(
      LineageWriteContext context,
      List<Dataset> datasets,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    List<DatasetRecord> records = new ArrayList<>(datasets.size());
    for (Dataset dataset : datasets) {
      records.add(upsertLineageDatasetSequentially(context, dataset, now, runUuid, isInput));
    }
    return records;
  }

  private DatasetRecord upsertLineageDatasetSequentially(
      LineageWriteContext context,
      Dataset dataset,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    LineageWriteContext.PreparedLineageDataset occurrence =
        prepareLineageDataset(context, dataset, now);
    return upsertPreparedLineageDatasetSequentially(context, occurrence, now, runUuid, isInput);
  }

  private List<DatasetRecord> upsertPreparedLineageDatasetsSequentially(
      LineageWriteContext context,
      List<LineageWriteContext.PreparedLineageDataset> prepared,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    List<DatasetRecord> records = new ArrayList<>(prepared.size());
    Map<UUID, UUID> currentVersionByDataset = new LinkedHashMap<>();
    for (LineageWriteContext.PreparedLineageDataset occurrence : prepared) {
      DatasetRow datasetRow = occurrence.datasetRow();
      UUID carriedCurrentVersion = currentVersionByDataset.get(datasetRow.getUuid());
      if (carriedCurrentVersion != null) {
        occurrence =
            occurrence.withDatasetRow(datasetRow.withCurrentVersionUuid(carriedCurrentVersion));
      }
      DatasetRecord record =
          upsertPreparedLineageDatasetSequentially(context, occurrence, now, runUuid, isInput);
      records.add(record);
      record
          .getDatasetRow()
          .getCurrentVersionUuid()
          .ifPresent(
              currentVersion ->
                  currentVersionByDataset.put(record.getDatasetRow().getUuid(), currentVersion));
    }
    return records;
  }

  private DatasetRecord upsertPreparedLineageDatasetSequentially(
      LineageWriteContext context,
      LineageWriteContext.PreparedLineageDataset occurrence,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    ModelDaos daos = context.daos();
    List<DatasetFieldUpsert> fieldUpserts = new ArrayList<>(occurrence.fieldsOrEmpty().size());
    for (SchemaField field : occurrence.fieldsOrEmpty()) {
      fieldUpserts.add(
          new DatasetFieldUpsert(
              UUID.randomUUID(),
              now,
              now,
              occurrence.datasetRow().getUuid(),
              field.getName(),
              field.getType(),
              field.getDescription()));
    }
    List<DatasetFieldRow> datasetFields = new ArrayList<>();
    if (!fieldUpserts.isEmpty()) {
      datasetFields.addAll(
          context.intake()
              ? daos.getDatasetFieldDao().upsertAllInTransaction(fieldUpserts)
              : daos.getDatasetFieldDao().upsertAll(fieldUpserts));
    }

    DatasetRow datasetRow = occurrence.datasetRow();
    DatasetVersionRow datasetVersionRow =
        datasetRow
            .getCurrentVersionUuid()
            .filter(ignored -> isInput)
            .flatMap(daos.getDatasetVersionDao()::findRowByUuid)
            .orElseGet(
                () ->
                    createDatasetVersion(
                        context, occurrence, datasetFields, now, runUuid, isInput));

    List<DatasetFieldMapping> fieldMappings = new ArrayList<>(datasetFields.size());
    for (DatasetFieldRow datasetField : datasetFields) {
      fieldMappings.add(
          new DatasetFieldMapping(datasetVersionRow.getUuid(), datasetField.getUuid()));
    }
    if (!fieldMappings.isEmpty()) {
      if (context.intake()) {
        daos.getDatasetFieldDao().updateFieldMappingInTransaction(fieldMappings);
      } else {
        daos.getDatasetFieldDao().updateFieldMapping(fieldMappings);
      }
    }

    if (isInput && runUuid != null) {
      context.queueInputMapping(datasetVersionRow.getUuid());

      // TODO - this is a short term fix until
      // https://github.com/MarquezProject/marquez/issues/1361 is fully thought out
      if (datasetRow.getCurrentVersionUuid().isEmpty()) {
        if (context.intake()) {
          daos.getDatasetDao()
              .updateVersionsInTransaction(
                  List.of(
                      new DatasetCurrentVersionUpdate(
                          datasetRow.getUuid(), now, datasetVersionRow.getUuid())));
        } else {
          daos.getDatasetDao()
              .updateVersion(datasetRow.getUuid(), now, datasetVersionRow.getUuid());
        }
        datasetRow = datasetRow.withCurrentVersionUuid(datasetVersionRow.getUuid());
      }
    }

    List<ColumnLineageRow> columnLineageRows = Collections.emptyList();
    if (!isInput) {
      columnLineageRows =
          context.collectColumnLineage(occurrence.dataset(), now, datasetFields, datasetVersionRow);
    }
    return new DatasetRecord(
        datasetRow, datasetVersionRow, occurrence.datasetNamespace(), columnLineageRows);
  }

  private LineageWriteContext.PreparedLineageDataset prepareLineageDataset(
      LineageWriteContext context, Dataset ds, Instant now) {
    ModelDaos daos = context.daos();
    LineageWriteContext.PreparedLineageDatasetBase preparedBase =
        prepareLineageDatasetBase(context, ds, now);
    PrimaryDatasetSymlinkUpsert primarySymlinkWrite = preparedBase.primarySymlinkWrite();
    DatasetSymlinkRow symlink =
        daos.getDatasetSymlinkDao()
            .upsertDatasetSymlinkRow(
                primarySymlinkWrite.getUuid(),
                primarySymlinkWrite.getName(),
                primarySymlinkWrite.getNamespaceUuid(),
                true,
                null,
                primarySymlinkWrite.getNow());

    Optional.ofNullable(ds.getFacets())
        .map(facets -> facets.getSymlinks())
        .ifPresent(
            el ->
                el.getIdentifiers().stream()
                    .forEach(
                        id ->
                            daos.getDatasetSymlinkDao()
                                .doUpsertDatasetSymlinkRow(
                                    symlink.getUuid(),
                                    id.getName(),
                                    context.upsertNamespace(id.getNamespace(), now).getUuid(),
                                    false,
                                    id.getType(),
                                    now)));
    String lifecycleState = getDatasetLifecycleState(ds);
    DatasetUpsert datasetUpsert = toDatasetUpsert(preparedBase, symlink, lifecycleState, now);
    DatasetRow datasetRow =
        daos.getDatasetDao()
            .upsert(
                datasetUpsert.getUuid(),
                datasetUpsert.getType(),
                datasetUpsert.getNow(),
                datasetUpsert.getNamespaceUuid(),
                datasetUpsert.getNamespaceName(),
                datasetUpsert.getSourceUuid(),
                datasetUpsert.getSourceName(),
                datasetUpsert.getName(),
                datasetUpsert.getPhysicalName(),
                datasetUpsert.getDescription(),
                datasetUpsert.isDeleted());

    List<SchemaField> fields =
        Optional.ofNullable(ds.getFacets())
            .map(DatasetFacets::getSchema)
            .map(SchemaDatasetFacet::getFields)
            .orElse(null);

    return new LineageWriteContext.PreparedLineageDataset(
        ds,
        preparedBase.rawNamespace(),
        preparedBase.datasetNamespace(),
        preparedBase.source(),
        symlink,
        datasetRow,
        lifecycleState,
        fields);
  }

  private LineageWriteContext.PreparedLineageDatasetBase prepareLineageDatasetBase(
      LineageWriteContext context, Dataset ds, Instant now) {
    String rawNamespaceName = ds.getNamespace();
    String formattedNamespaceName = formatNamespaceName(rawNamespaceName);
    NamespaceRow dsNamespace = context.upsertNamespace(rawNamespaceName, now);

    boolean hasExplicitSource = ds.getFacets() != null && ds.getFacets().getDataSource() != null;
    String sourceName =
        hasExplicitSource ? ds.getFacets().getDataSource().getName() : DEFAULT_SOURCE_NAME;
    String sourceUrl =
        hasExplicitSource ? getUrlOrNull(ds.getFacets().getDataSource().getUri()) : "";
    SourceRow source =
        context.upsertSource(sourceName, getSourceType(ds), sourceUrl, hasExplicitSource, now);

    String dsDescription = null;
    if (ds.getFacets() != null && ds.getFacets().getDocumentation() != null) {
      dsDescription = ds.getFacets().getDocumentation().getDescription();
    }

    NamespaceRow datasetNamespace =
        rawNamespaceName.equals(formattedNamespaceName)
            ? dsNamespace
            : context.upsertNamespace(formattedNamespaceName, now);

    return new LineageWriteContext.PreparedLineageDatasetBase(
        ds,
        dsNamespace,
        datasetNamespace,
        source,
        dsDescription,
        new PrimaryDatasetSymlinkUpsert(
            UUID.randomUUID(), formatDatasetName(ds.getName()), dsNamespace.getUuid(), now));
  }

  private String getDatasetLifecycleState(Dataset dataset) {
    return Optional.ofNullable(dataset.getFacets())
        .map(DatasetFacets::getLifecycleStateChange)
        .map(LifecycleStateChangeFacet::getLifecycleStateChange)
        .orElse("");
  }

  private DatasetUpsert toDatasetUpsert(
      LineageWriteContext.PreparedLineageDatasetBase preparedBase,
      DatasetSymlinkRow symlink,
      String lifecycleState,
      Instant now) {
    Dataset ds = preparedBase.dataset();
    return new DatasetUpsert(
        symlink.getUuid(),
        getDatasetType(ds),
        now,
        preparedBase.datasetNamespace().getUuid(),
        preparedBase.datasetNamespace().getName(),
        preparedBase.source().getUuid(),
        preparedBase.source().getName(),
        preparedBase.primarySymlinkWrite().getName(),
        ds.getName(),
        preparedBase.description(),
        lifecycleState.equalsIgnoreCase("DROP"));
  }

  private DatasetVersionRow createDatasetVersion(
      LineageWriteContext context,
      LineageWriteContext.PreparedLineageDataset occurrence,
      List<DatasetFieldRow> datasetFields,
      Instant now,
      @Nullable UUID runUuid,
      boolean isInput) {
    ModelDaos daos = context.daos();
    DatasetRow datasetRow = occurrence.datasetRow();
    UUID versionUuid =
        Utils.newDatasetVersionFor(
                occurrence.rawNamespace().getName(),
                occurrence.source().getName(),
                datasetRow.getPhysicalName(),
                occurrence.symlink().getName(),
                occurrence.lifecycleState(),
                occurrence.fields(),
                runUuid)
            .getValue();
    UUID datasetSchemaVersionUuid =
        (context.intake()
                ? daos.getDatasetSchemaVersionDao()
                    .upsertSchemaVersionInTransaction(datasetRow, datasetFields, now)
                : daos.getDatasetSchemaVersionDao()
                    .upsertSchemaVersion(datasetRow, datasetFields, now))
            .getValue();
    return daos.getDatasetVersionDao()
        .upsert(
            UUID.randomUUID(),
            now,
            datasetRow.getUuid(),
            versionUuid,
            datasetSchemaVersionUuid,
            isInput ? null : runUuid,
            daos.getDatasetVersionDao().toPgObjectSchemaFields(occurrence.fields()),
            occurrence.rawNamespace().getName(),
            occurrence.dataset().getName(),
            occurrence.lifecycleState());
  }

  /** Mutable state whose lifetime is exactly one relational event projection. */
  final class LineageWriteContext {
    private record PreparedLineageDatasetBase(
        Dataset dataset,
        NamespaceRow rawNamespace,
        NamespaceRow datasetNamespace,
        SourceRow source,
        @Nullable String description,
        PrimaryDatasetSymlinkUpsert primarySymlinkWrite) {}

    private record PreparedLineageDataset(
        Dataset dataset,
        NamespaceRow rawNamespace,
        NamespaceRow datasetNamespace,
        SourceRow source,
        DatasetSymlinkRow symlink,
        DatasetRow datasetRow,
        String lifecycleState,
        @Nullable List<SchemaField> fields) {
      List<SchemaField> fieldsOrEmpty() {
        return fields == null ? Collections.emptyList() : fields;
      }

      PreparedLineageDataset withDatasetRow(DatasetRow replacement) {
        return new PreparedLineageDataset(
            dataset,
            rawNamespace,
            datasetNamespace,
            source,
            symlink,
            replacement,
            lifecycleState,
            fields);
      }
    }

    private final ModelDaos daos;
    @Nullable private final UUID runUuid;
    private final boolean intake;
    private final ColumnLineageContext columnLineageContext;
    private final Map<String, NamespaceRow> namespacesByExactName = new LinkedHashMap<>();
    private final Map<String, SourceState> lastSourceStateByName = new LinkedHashMap<>();
    private final Set<UUID> pendingInputMappings = new LinkedHashSet<>();
    @Nullable private List<DatasetFacetWrite> pendingDatasetFacets;
    private final List<ColumnLineageDatasetWrite> pendingColumnLineage = new ArrayList<>();

    private LineageWriteContext(ModelDaos daos, @Nullable UUID runUuid, boolean intake) {
      this.daos = daos;
      this.runUuid = runUuid;
      this.intake = intake;
      this.columnLineageContext = new ColumnLineageContext(daos, runUuid);
    }

    static LineageWriteContext forIntake(ModelDaos daos, @Nullable UUID runUuid) {
      return new LineageWriteContext(daos, runUuid, true);
    }

    static LineageWriteContext forCompatibility(ModelDaos daos, @Nullable UUID runUuid) {
      return new LineageWriteContext(daos, runUuid, false);
    }

    ModelDaos daos() {
      return daos;
    }

    boolean intake() {
      return intake;
    }

    @Nullable
    UUID runUuid() {
      return runUuid;
    }

    NamespaceRow upsertNamespace(String exactName, Instant now) {
      NamespaceRow namespace = namespacesByExactName.get(exactName);
      if (namespace == null) {
        namespace =
            daos.getNamespaceDao()
                .upsertNamespaceRow(UUID.randomUUID(), now, exactName, DEFAULT_NAMESPACE_OWNER);
        namespacesByExactName.put(exactName, namespace);
      }
      return namespace;
    }

    SourceRow upsertSource(
        String name, String type, @Nullable String connectionUrl, boolean explicit, Instant now) {
      SourceSpec requested =
          new SourceSpec(
              explicit ? SourceMode.EXPLICIT : SourceMode.DEFAULT, name, type, connectionUrl);
      SourceState previous = lastSourceStateByName.get(name);
      if (previous != null && previous.spec().equals(requested)) {
        return previous.row();
      }

      SourceRow row =
          explicit
              ? daos.getSourceDao().upsert(UUID.randomUUID(), type, now, name, connectionUrl)
              : intake
                  ? daos.getSourceDao()
                      .upsertOrDefaultInTransaction(
                          UUID.randomUUID(), type, now, name, connectionUrl)
                  : daos.getSourceDao()
                      .upsertOrDefault(UUID.randomUUID(), type, now, name, connectionUrl);
      lastSourceStateByName.put(name, new SourceState(requested, row));
      return row;
    }

    void queueInputMapping(UUID datasetVersionUuid) {
      if (runUuid != null) {
        pendingInputMappings.add(datasetVersionUuid);
      }
    }

    void flushInputMappings() {
      if (runUuid != null && !pendingInputMappings.isEmpty()) {
        if (intake) {
          daos.getRunDao().updateInputMappingsInTransaction(runUuid, pendingInputMappings);
        } else {
          daos.getRunDao().updateInputMappings(runUuid, pendingInputMappings);
        }
        pendingInputMappings.clear();
      }
    }

    void queueDatasetFacets(
        Dataset dataset,
        DatasetRecord record,
        @Nullable UUID facetRunUuid,
        @Nullable String eventType,
        Instant lineageEventTime,
        boolean isInput) {
      UUID datasetUuid = record.getDatasetRow().getUuid();
      UUID datasetVersionUuid = record.getDatasetVersionRow().getUuid();
      Instant createdAt = Instant.now();
      Optional.ofNullable(dataset.getFacets())
          .ifPresent(
              facets ->
                  addDatasetFacet(
                      DatasetFacetWrite.forDatasetFacets(
                          createdAt,
                          datasetUuid,
                          datasetVersionUuid,
                          facetRunUuid,
                          lineageEventTime,
                          eventType,
                          facets)));
      if (isInput) {
        Optional.ofNullable(dataset.getInputFacets())
            .ifPresent(
                facets ->
                    addDatasetFacet(
                        DatasetFacetWrite.forInputFacets(
                            createdAt,
                            datasetUuid,
                            datasetVersionUuid,
                            facetRunUuid,
                            lineageEventTime,
                            eventType,
                            facets)));
      } else {
        Optional.ofNullable(dataset.getOutputFacets())
            .ifPresent(
                facets ->
                    addDatasetFacet(
                        DatasetFacetWrite.forOutputFacets(
                            createdAt,
                            datasetUuid,
                            datasetVersionUuid,
                            facetRunUuid,
                            lineageEventTime,
                            eventType,
                            facets)));
      }
    }

    private void addDatasetFacet(DatasetFacetWrite write) {
      if (FacetUtils.isEmpty(write.getFacets())) {
        return;
      }
      if (pendingDatasetFacets == null) {
        pendingDatasetFacets = new ArrayList<>();
      }
      pendingDatasetFacets.add(write);
      if (pendingDatasetFacets.size() == DatasetFacetsDao.MAX_FACET_CONTAINERS_PER_INSERT) {
        flushDatasetFacets();
      }
    }

    void flushDatasetFacets() {
      if (pendingDatasetFacets != null) {
        List<DatasetFacetWrite> writes = pendingDatasetFacets;
        pendingDatasetFacets = null;
        if (intake) {
          daos.getDatasetFacetsDao().insertDatasetFacetWritesInTransaction(writes);
        } else {
          daos.getDatasetFacetsDao().insertDatasetFacetWrites(writes);
        }
      }
    }

    List<ColumnLineageRow> collectColumnLineage(
        Dataset dataset,
        Instant now,
        List<DatasetFieldRow> datasetFields,
        DatasetVersionRow datasetVersionRow) {
      if (!intake) {
        return columnLineageContext.upsertColumnLineage(
            dataset, now, datasetFields, datasetVersionRow);
      }

      if (runUuid == null) {
        return Collections.emptyList();
      }

      ColumnLineageDatasetWrite write =
          columnLineageContext.prepareColumnLineage(dataset, datasetFields, datasetVersionRow);
      if (write != null) {
        pendingColumnLineage.add(write);
      }
      return Collections.emptyList();
    }

    void flushColumnLineage(Instant now) {
      if (runUuid != null && !pendingColumnLineage.isEmpty()) {
        daos.getColumnLineageDao()
            .upsertColumnLineageRowsForIntakeInTransaction(List.copyOf(pendingColumnLineage), now);
        pendingColumnLineage.clear();
      }
    }

    private enum SourceMode {
      DEFAULT,
      EXPLICIT
    }

    private record SourceSpec(
        SourceMode mode, String name, String type, @Nullable String connectionUrl) {}

    private record SourceState(SourceSpec spec, SourceRow row) {}
  }

  /** Lazily loads and indexes a run's input fields for all output datasets in one event. */
  final class ColumnLineageContext {
    private final ModelDaos daos;
    private final UUID runUuid;
    private Map<InputFieldKey, List<Pair<UUID, UUID>>> inputFieldsByKey;

    ColumnLineageContext(ModelDaos daos, UUID runUuid) {
      this.daos = daos;
      this.runUuid = runUuid;
    }

    List<ColumnLineageRow> upsertColumnLineage(
        Dataset ds,
        Instant now,
        List<DatasetFieldRow> datasetFields,
        DatasetVersionRow datasetVersionRow) {
      ColumnLineageDatasetWrite write = prepareColumnLineage(ds, datasetFields, datasetVersionRow);
      if (write == null) {
        return Collections.emptyList();
      }
      return daos.getColumnLineageDao()
          .upsertColumnLineageRows(write.outputDatasetVersionUuid(), write.writes(), now);
    }

    @Nullable
    ColumnLineageDatasetWrite prepareColumnLineage(
        Dataset ds, List<DatasetFieldRow> datasetFields, DatasetVersionRow datasetVersionRow) {
      Map<String, LineageEvent.ColumnLineageOutputColumn> columnLineageByOutput =
          Optional.ofNullable(ds.getFacets())
              .map(DatasetFacets::getColumnLineage)
              .map(LineageEvent.ColumnLineageDatasetFacet::getFields)
              .map(LineageEvent.ColumnLineageDatasetFacetFields::getAdditional)
              .orElse(Collections.emptyMap());
      if (columnLineageByOutput.isEmpty()) {
        return null;
      }

      Map<String, DatasetFieldRow> outputFieldsByName = new LinkedHashMap<>();
      for (DatasetFieldRow datasetField : datasetFields) {
        outputFieldsByName.putIfAbsent(datasetField.getName(), datasetField);
      }

      Logger log = LoggerFactory.getLogger(OpenLineageDao.class);
      List<ColumnLineageWrite> writes = new ArrayList<>(columnLineageByOutput.size());
      for (Map.Entry<String, LineageEvent.ColumnLineageOutputColumn> entry :
          columnLineageByOutput.entrySet()) {
        String columnName = entry.getKey();
        LineageEvent.ColumnLineageOutputColumn columnLineage = entry.getValue();
        if (columnLineage == null) {
          continue;
        }

        DatasetFieldRow outputField = outputFieldsByName.get(columnName);
        if (outputField == null) {
          log.error(
              "Cannot produce column lineage for missing output field in output dataset: {}",
              columnName);
          continue;
        }

        List<Pair<UUID, UUID>> inputFields = resolveInputFields(columnLineage.getInputFields());
        log.debug(
            "Adding column lineage on output field '{}' for dataset version '{}' with input fields: {}",
            outputField.getName(),
            datasetVersionRow.getUuid(),
            inputFields);
        if (!inputFields.isEmpty()) {
          writes.add(
              new ColumnLineageWrite(
                  outputField.getUuid(),
                  inputFields,
                  columnLineage.getTransformationDescription(),
                  columnLineage.getTransformationType()));
        }
      }
      if (writes.isEmpty()) {
        return null;
      }
      return new ColumnLineageDatasetWrite(datasetVersionRow.getUuid(), writes);
    }

    private List<Pair<UUID, UUID>> resolveInputFields(
        List<LineageEvent.ColumnLineageInputField> inputFields) {
      if (runUuid == null || inputFields == null || inputFields.isEmpty()) {
        return Collections.emptyList();
      }

      Set<InputFieldKey> requestedFields = new LinkedHashSet<>();
      for (LineageEvent.ColumnLineageInputField inputField : inputFields) {
        requestedFields.add(
            new InputFieldKey(
                inputField.getNamespace(), inputField.getName(), inputField.getField()));
      }

      List<Pair<UUID, UUID>> resolvedFields = new ArrayList<>();
      Map<InputFieldKey, List<Pair<UUID, UUID>>> indexedInputFields = inputFieldsByKey();
      for (InputFieldKey requestedField : requestedFields) {
        resolvedFields.addAll(
            indexedInputFields.getOrDefault(requestedField, Collections.emptyList()));
      }
      return resolvedFields;
    }

    private Map<InputFieldKey, List<Pair<UUID, UUID>>> inputFieldsByKey() {
      if (inputFieldsByKey == null) {
        List<InputFieldData> runFields =
            daos.getDatasetFieldDao().findInputFieldsDataAssociatedWithRun(runUuid);
        LoggerFactory.getLogger(OpenLineageDao.class)
            .debug("Found input datasets fields for run '{}': {}", runUuid, runFields);

        inputFieldsByKey = new LinkedHashMap<>();
        for (InputFieldData fieldData : runFields) {
          InputFieldKey key =
              new InputFieldKey(
                  fieldData.getNamespace(), fieldData.getDatasetName(), fieldData.getField());
          inputFieldsByKey
              .computeIfAbsent(key, ignored -> new ArrayList<>())
              .add(Pair.of(fieldData.getDatasetVersionUuid(), fieldData.getDatasetFieldUuid()));
        }
      }
      return inputFieldsByKey;
    }

    private record InputFieldKey(String namespace, String datasetName, String field) {}
  }

  default String formatDatasetName(String name) {
    return name;
  }

  default String getSourceType(Dataset ds) {
    return SourceType.of("POSTGRESQL").getValue();
  }

  default DatasetType getDatasetType(Dataset ds) {
    return DatasetType.DB_TABLE;
  }

  default RunState getRunState(String eventType) {
    if (eventType == null) {
      return RunState.RUNNING;
    }
    switch (eventType.toLowerCase()) {
      case "complete":
        return RunState.COMPLETED;
      case "abort":
        return RunState.ABORTED;
      case "fail":
        return RunState.FAILED;
      case "start":
        return RunState.RUNNING;
      default:
        return RunState.RUNNING;
    }
  }

  default Map<String, String> createRunArgs(LineageEvent event) {
    Map<String, String> args = new LinkedHashMap<>();
    if (event.getRun().getFacets() != null) {
      if (event.getRun().getFacets().getNominalTime() != null) {
        args.put(
            "nominal_start_time",
            event.getRun().getFacets().getNominalTime().getNominalStartTime().toString());
        if (event.getRun().getFacets().getNominalTime().getNominalEndTime() != null) {
          args.put(
              "nominal_end_time",
              event.getRun().getFacets().getNominalTime().getNominalEndTime().toString());
        }
      }
      if (event.getRun().getFacets().getParent() != null) {
        args.put("run_id", event.getRun().getFacets().getParent().getRun().getRunId());
        args.put("name", event.getRun().getFacets().getParent().getJob().getName());
        args.put("namespace", event.getRun().getFacets().getParent().getJob().getNamespace());
      }
    }
    return args;
  }

  default UUID runToUuid(String runId) {
    try {
      return UUID.fromString(runId);
    } catch (Exception e) {
      // Allow non-UUID runId
      return UUID.nameUUIDFromBytes(runId.getBytes());
    }
  }

  default PGobject createJsonArray(BaseEvent event, ObjectMapper mapper) {
    try {
      PGobject jsonObject = new PGobject();
      jsonObject.setType("json");
      jsonObject.setValue(mapper.writeValueAsString(event));
      return jsonObject;
    } catch (Exception e) {
      throw new RuntimeException("Could write lineage event to db", e);
    }
  }
}
