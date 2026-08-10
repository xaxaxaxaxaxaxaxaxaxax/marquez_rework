/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.validation.constraints.NotNull;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.DatasetId;
import marquez.common.models.JobId;
import marquez.common.models.RunId;
import marquez.db.JobDao;
import marquez.db.LineageDao;
import marquez.db.LineageDao.DatasetSummary;
import marquez.db.LineageDao.JobSummary;
import marquez.db.LineageDao.RunSummary;
import marquez.db.RunDao;
import marquez.db.models.JobRow;
import marquez.service.DelegatingDaos.DelegatingLineageDao;
import marquez.service.LineageService.UpstreamRunLineage;
import marquez.service.models.DatasetData;
import marquez.service.models.Edge;
import marquez.service.models.JobData;
import marquez.service.models.Lineage;
import marquez.service.models.Node;
import marquez.service.models.NodeId;
import marquez.service.models.NodeType;
import marquez.service.models.Run;

@Slf4j
public class LineageService extends DelegatingLineageDao {

  public record UpstreamRunLineage(List<UpstreamRun> runs) {}

  public record UpstreamRun(JobSummary job, RunSummary run, List<DatasetSummary> inputs) {}

  private record DatasetNode(DatasetData data, NodeId id) {}

  private final JobDao jobDao;

  private final RunDao runDao;

  public LineageService(LineageDao delegate, JobDao jobDao, RunDao runDao) {
    super(delegate);
    this.jobDao = jobDao;
    this.runDao = runDao;
  }

  // TODO make input parameters easily extendable if adding more options like 'withJobFacets'
  public Lineage lineage(NodeId nodeId, int depth) {
    log.debug("Attempting to get lineage for node '{}' with depth '{}'", nodeId.getValue(), depth);
    Optional<UUID> optionalUUID = getJobUuid(nodeId);
    if (optionalUUID.isEmpty()) {
      log.warn(
          "Failed to get job associated with node '{}', returning orphan graph...",
          nodeId.getValue());
      return toLineageWithOrphanDataset(nodeId.asDatasetId());
    }
    UUID job = optionalUUID.get();
    log.debug("Attempting to get lineage for job '{}'", job);
    Set<JobData> jobData = getLineage(Set.of(job), depth);

    // Ensure job data is not empty before attempting to load related run and dataset data.
    if (jobData.isEmpty()) {
      // Log warning, then return an orphan lineage graph; a graph should contain at most one
      // job->dataset relationship.
      log.warn(
          "Failed to get lineage for job '{}' associated with node '{}', returning orphan graph...",
          job,
          nodeId.getValue());
      return toLineageWithOrphanDataset(nodeId.asDatasetId());
    }

    Set<UUID> currentRunUuids = new HashSet<>();
    Set<UUID> datasetIds = new HashSet<>();
    for (JobData currentJob : jobData) {
      UUID currentRunUuid = currentJob.getCurrentRunUuid();
      if (currentRunUuid != null) {
        currentRunUuids.add(currentRunUuid);
      }
      datasetIds.addAll(currentJob.getInputUuids());
      datasetIds.addAll(currentJob.getOutputUuids());
    }

    if (!currentRunUuids.isEmpty()) {
      Map<UUID, Run> currentRunsById =
          Maps.uniqueIndex(runDao.findRunsByUuids(currentRunUuids), run -> run.getId().getValue());
      for (JobData currentJob : jobData) {
        Run currentRun = currentRunsById.get(currentJob.getCurrentRunUuid());
        if (currentRun != null) {
          currentJob.setLatestRun(currentRun);
        }
      }
    }

    Set<DatasetData> datasets = datasetIds.isEmpty() ? Set.of() : this.getDatasetData(datasetIds);

    if (nodeId.isDatasetType()) {
      DatasetId datasetId = nodeId.asDatasetId();
      DatasetData datasetData =
          datasets.stream()
              .filter(dataset -> dataset.getId().equals(datasetId))
              .findFirst()
              .orElseGet(
                  () ->
                      this.getDatasetData(
                          datasetId.getNamespace().getValue(), datasetId.getName().getValue()));

      if (!datasetIds.contains(datasetData.getUuid())) {
        log.warn(
            "Found jobs {} which no longer share lineage with dataset '{}' - discarding",
            jobData.stream().map(JobData::getId).toList(),
            nodeId.getValue());
        return toLineageWithOrphanDataset(datasetData);
      }
    }
    return toLineage(jobData, datasets);
  }

  private Lineage toLineageWithOrphanDataset(@NonNull DatasetId datasetId) {
    return toLineageWithOrphanDataset(
        getDatasetData(datasetId.getNamespace().getValue(), datasetId.getName().getValue()));
  }

  private Lineage toLineageWithOrphanDataset(@NonNull DatasetData datasetData) {
    return new Lineage(
        ImmutableSortedSet.of(
            Node.dataset().data(datasetData).id(NodeId.of(datasetData.getId())).build()));
  }

  private Lineage toLineage(Set<JobData> jobData, Set<DatasetData> datasets) {
    ImmutableSortedSet.Builder<Node> nodes = ImmutableSortedSet.naturalOrder();
    Map<UUID, DatasetNode> datasetById =
        datasets.stream()
            .collect(
                toMap(
                    DatasetData::getUuid,
                    dataset -> new DatasetNode(dataset, NodeId.of(dataset.getId()))));
    Map<UUID, ImmutableSortedSet.Builder<Edge>> datasetInEdges = new HashMap<>();
    Map<UUID, ImmutableSortedSet.Builder<Edge>> datasetOutEdges = new HashMap<>();

    for (JobData data : jobData) {
      NodeId jobNodeId = NodeId.of(data.getId());
      ImmutableSet.Builder<DatasetId> inputs =
          ImmutableSet.builderWithExpectedSize(data.getInputUuids().size());
      ImmutableSet.Builder<DatasetId> outputs =
          ImmutableSet.builderWithExpectedSize(data.getOutputUuids().size());
      ImmutableSortedSet.Builder<Edge> jobInEdges = ImmutableSortedSet.naturalOrder();
      ImmutableSortedSet.Builder<Edge> jobOutEdges = ImmutableSortedSet.naturalOrder();

      for (UUID datasetUuid : data.getInputUuids()) {
        DatasetNode dataset = datasetById.get(datasetUuid);
        if (dataset == null) {
          continue;
        }

        inputs.add(dataset.data().getId());
        Edge edge = Edge.of(dataset.id(), jobNodeId);
        jobInEdges.add(edge);
        datasetOutEdges
            .computeIfAbsent(datasetUuid, ignored -> ImmutableSortedSet.<Edge>naturalOrder())
            .add(edge);
      }

      for (UUID datasetUuid : data.getOutputUuids()) {
        DatasetNode dataset = datasetById.get(datasetUuid);
        if (dataset == null) {
          continue;
        }

        outputs.add(dataset.data().getId());
        Edge edge = Edge.of(jobNodeId, dataset.id());
        jobOutEdges.add(edge);
        datasetInEdges
            .computeIfAbsent(datasetUuid, ignored -> ImmutableSortedSet.<Edge>naturalOrder())
            .add(edge);
      }

      data.setInputs(inputs.build());
      data.setOutputs(outputs.build());
      nodes.add(new Node(jobNodeId, NodeType.JOB, data, jobInEdges.build(), jobOutEdges.build()));
    }

    for (Map.Entry<UUID, DatasetNode> entry : datasetById.entrySet()) {
      DatasetNode dataset = entry.getValue();
      nodes.add(
          new Node(
              dataset.id(),
              NodeType.DATASET,
              dataset.data(),
              buildEdges(datasetInEdges.remove(entry.getKey())),
              buildEdges(datasetOutEdges.remove(entry.getKey()))));
    }

    return new Lineage(nodes.build());
  }

  private static ImmutableSortedSet<Edge> buildEdges(ImmutableSortedSet.Builder<Edge> edges) {
    return edges == null ? ImmutableSortedSet.of() : edges.build();
  }

  public Optional<UUID> getJobUuid(NodeId nodeId) {
    if (nodeId.isJobType()) {
      JobId jobId = nodeId.asJobId();
      return jobDao
          .findJobByNameAsRow(jobId.getNamespace().getValue(), jobId.getName().getValue())
          .map(JobRow::getUuid);
    } else if (nodeId.isDatasetType()) {
      DatasetId datasetId = nodeId.asDatasetId();
      return getJobFromInputOrOutput(
          datasetId.getName().getValue(), datasetId.getNamespace().getValue());
    } else {
      throw new NodeIdNotFoundException(
          String.format("Node '%s' must be of type dataset or job!", nodeId.getValue()));
    }
  }

  /**
   * Returns the upstream lineage for a given run. Recursively: run -> dataset version it read from
   * -> the run that produced it
   *
   * @param runId the run to get upstream lineage from
   * @param depth the maximum depth of the upstream lineage
   * @return the upstream lineage for that run up to `detph` levels
   */
  public UpstreamRunLineage upstream(@NotNull RunId runId, int depth) {
    List<UpstreamRunRow> upstreamRuns = getUpstreamRuns(runId.getValue(), depth);
    Map<RunId, List<UpstreamRunRow>> collect =
        upstreamRuns.stream().collect(groupingBy(r -> r.run().id(), LinkedHashMap::new, toList()));
    List<UpstreamRun> runs =
        collect.entrySet().stream()
            .map(
                row -> {
                  UpstreamRunRow upstreamRunRow = row.getValue().get(0);
                  List<DatasetSummary> inputs =
                      row.getValue().stream()
                          .map(UpstreamRunRow::input)
                          .filter(i -> i != null)
                          .collect(toList());
                  return new UpstreamRun(upstreamRunRow.job(), upstreamRunRow.run(), inputs);
                })
            .collect(toList());
    return new UpstreamRunLineage(runs);
  }
}
