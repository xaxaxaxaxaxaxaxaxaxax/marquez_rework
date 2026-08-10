/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.JobId;
import marquez.common.models.JobName;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.db.JobDao;
import marquez.db.LineageDao;
import marquez.db.RunDao;
import marquez.db.models.JobRow;
import marquez.service.models.DatasetData;
import marquez.service.models.Edge;
import marquez.service.models.JobData;
import marquez.service.models.Lineage;
import marquez.service.models.Node;
import marquez.service.models.NodeId;
import marquez.service.models.Run;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class LineageServiceUnitTest {

  private static final NamespaceName NAMESPACE = NamespaceName.of("namespace");
  private static final JobName SEED_JOB_NAME = JobName.of("seed-job");

  @Test
  void batchesCurrentRunProjectionForManyJobs() {
    LineageDao lineageDao = mock(LineageDao.class);
    JobDao jobDao = mock(JobDao.class);
    RunDao runDao = mock(RunDao.class);
    LineageService service = new LineageService(lineageDao, jobDao, runDao);
    UUID seedJobUuid = UUID.randomUUID();
    stubSeedJob(jobDao, seedJobUuid);

    Set<JobData> jobs = new LinkedHashSet<>();
    Set<UUID> currentRunUuids = new LinkedHashSet<>();
    List<Run> runs = new ArrayList<>();
    Map<JobData, Run> expectedRunsByJob = new LinkedHashMap<>();
    JobData nullCurrentRunJob = null;
    JobData missingCurrentRunJob = null;
    for (int index = 0; index < 100; index++) {
      JobData job = newJob(index);
      jobs.add(job);
      if (index == 0) {
        nullCurrentRunJob = job;
        continue;
      }

      UUID currentRunUuid = UUID.randomUUID();
      currentRunUuids.add(currentRunUuid);
      when(job.getCurrentRunUuid()).thenReturn(currentRunUuid);
      if (index == 1) {
        missingCurrentRunJob = job;
        continue;
      }

      Run run = mock(Run.class);
      when(run.getId()).thenReturn(RunId.of(currentRunUuid));
      runs.add(run);
      expectedRunsByJob.put(job, run);
    }
    when(lineageDao.getLineage(Set.of(seedJobUuid), 2)).thenReturn(jobs);
    when(runDao.findRunsByUuids(anyCollection())).thenReturn(runs);

    Lineage lineage = service.lineage(NodeId.of(NAMESPACE, SEED_JOB_NAME), 2);

    assertThat(lineage.getGraph())
        .hasSize(jobs.size())
        .extracting(Node::getData)
        .containsExactlyInAnyOrderElementsOf(jobs);
    ArgumentCaptor<Collection<UUID>> runUuidsCaptor = ArgumentCaptor.forClass(Collection.class);
    verify(runDao).findRunsByUuids(runUuidsCaptor.capture());
    assertThat(runUuidsCaptor.getValue()).containsExactlyInAnyOrderElementsOf(currentRunUuids);
    expectedRunsByJob.forEach((job, run) -> verify(job).setLatestRun(run));
    verify(nullCurrentRunJob, never()).setLatestRun(any());
    verify(missingCurrentRunJob, never()).setLatestRun(any());
    verify(runDao, never()).findRunByUuid(any());
    verify(lineageDao, never()).getParentJobData(any());
  }

  @Test
  void skipsRunProjectionWhenJobsHaveNoCurrentRun() {
    LineageDao lineageDao = mock(LineageDao.class);
    JobDao jobDao = mock(JobDao.class);
    RunDao runDao = mock(RunDao.class);
    LineageService service = new LineageService(lineageDao, jobDao, runDao);
    UUID seedJobUuid = UUID.randomUUID();
    stubSeedJob(jobDao, seedJobUuid);
    JobData job = newJob(0);
    when(lineageDao.getLineage(Set.of(seedJobUuid), 2)).thenReturn(Set.of(job));

    Lineage lineage = service.lineage(NodeId.of(NAMESPACE, SEED_JOB_NAME), 2);

    assertThat(lineage.getGraph()).singleElement().extracting(Node::getData).isSameAs(job);
    verify(runDao, never()).findRunsByUuids(anyCollection());
    verify(runDao, never()).findRunByUuid(any());
    verify(lineageDao, never()).getParentJobData(any());
  }

  @Test
  void assemblesSortedGraphWithOneSharedEdgePerRelationAndNoDatasetReread() {
    LineageDao lineageDao = mock(LineageDao.class);
    LineageService service = new LineageService(lineageDao, mock(JobDao.class), mock(RunDao.class));
    UUID seedJobUuid = UUID.randomUUID();
    UUID alphaUuid = UUID.randomUUID();
    UUID betaUuid = UUID.randomUUID();
    UUID gammaUuid = UUID.randomUUID();
    UUID missingUuid = UUID.randomUUID();
    DatasetData alpha = newDataset(alphaUuid, "alpha");
    DatasetData beta = newDataset(betaUuid, "beta");
    DatasetData gamma = newDataset(gammaUuid, "gamma");
    JobData firstJob = newJob(0);
    JobData secondJob = newJob(1);
    when(firstJob.getInputUuids()).thenReturn(new LinkedHashSet<>(List.of(betaUuid, alphaUuid)));
    when(firstJob.getOutputUuids())
        .thenReturn(new LinkedHashSet<>(List.of(gammaUuid, missingUuid)));
    when(secondJob.getInputUuids()).thenReturn(Set.of(gammaUuid));
    when(lineageDao.getJobFromInputOrOutput("alpha", NAMESPACE.getValue()))
        .thenReturn(Optional.of(seedJobUuid));
    when(lineageDao.getLineage(Set.of(seedJobUuid), 2))
        .thenReturn(new LinkedHashSet<>(List.of(secondJob, firstJob)));
    when(lineageDao.getDatasetData(Set.of(alphaUuid, betaUuid, gammaUuid, missingUuid)))
        .thenReturn(Set.of(alpha, beta, gamma));

    Lineage lineage = service.lineage(NodeId.of(alpha.getId()), 2);

    assertThat(lineage.getGraph()).hasSize(5).extracting(Node::getId).isSorted();
    Node firstJobNode = node(lineage, NodeId.of(firstJob.getId()));
    Node secondJobNode = node(lineage, NodeId.of(secondJob.getId()));
    Node alphaNode = node(lineage, NodeId.of(alpha.getId()));
    Node betaNode = node(lineage, NodeId.of(beta.getId()));
    Node gammaNode = node(lineage, NodeId.of(gamma.getId()));
    assertThat(firstJobNode.getInEdges())
        .extracting(Edge::getOrigin)
        .containsExactly(NodeId.of(alpha.getId()), NodeId.of(beta.getId()));
    assertThat(firstJobNode.getOutEdges())
        .extracting(Edge::getDestination)
        .containsExactly(NodeId.of(gamma.getId()));
    assertThat(firstJobNode.getInEdges().iterator().next())
        .isSameAs(alphaNode.getOutEdges().iterator().next());
    assertThat(firstJobNode.getInEdges().stream().skip(1).findFirst().orElseThrow())
        .isSameAs(betaNode.getOutEdges().iterator().next());
    assertThat(firstJobNode.getOutEdges().iterator().next())
        .isSameAs(gammaNode.getInEdges().iterator().next());
    assertThat(secondJobNode.getInEdges().iterator().next())
        .isSameAs(gammaNode.getOutEdges().iterator().next());
    verify(firstJob).setInputs(ImmutableSet.of(alpha.getId(), beta.getId()));
    verify(firstJob).setOutputs(ImmutableSet.of(gamma.getId()));
    verify(lineageDao).getDatasetData(Set.of(alphaUuid, betaUuid, gammaUuid, missingUuid));
    verify(lineageDao, never()).getDatasetData(NAMESPACE.getValue(), "alpha");
  }

  @Test
  void rereadsStaleDatasetOnceAndReusesItForOrphanGraph() {
    LineageDao lineageDao = mock(LineageDao.class);
    LineageService service = new LineageService(lineageDao, mock(JobDao.class), mock(RunDao.class));
    UUID seedJobUuid = UUID.randomUUID();
    DatasetData staleDataset = newDataset(UUID.randomUUID(), "stale");
    UUID currentDatasetUuid = UUID.randomUUID();
    DatasetData currentDataset = newDataset(currentDatasetUuid, "current");
    JobData job = newJob(0);
    when(job.getOutputUuids()).thenReturn(Set.of(currentDatasetUuid));
    when(lineageDao.getJobFromInputOrOutput("stale", NAMESPACE.getValue()))
        .thenReturn(Optional.of(seedJobUuid));
    when(lineageDao.getLineage(Set.of(seedJobUuid), 2)).thenReturn(Set.of(job));
    when(lineageDao.getDatasetData(Set.of(currentDatasetUuid))).thenReturn(Set.of(currentDataset));
    when(lineageDao.getDatasetData(NAMESPACE.getValue(), "stale")).thenReturn(staleDataset);

    Lineage lineage = service.lineage(NodeId.of(staleDataset.getId()), 2);

    assertThat(lineage.getGraph())
        .singleElement()
        .satisfies(
            node -> {
              assertThat(node.getId()).isEqualTo(NodeId.of(staleDataset.getId()));
              assertThat(node.getData()).isSameAs(staleDataset);
              assertThat(node.getInEdges()).isEmpty();
              assertThat(node.getOutEdges()).isEmpty();
            });
    verify(lineageDao).getDatasetData(NAMESPACE.getValue(), "stale");
  }

  private static void stubSeedJob(JobDao jobDao, UUID seedJobUuid) {
    JobRow seedJob = mock(JobRow.class);
    when(seedJob.getUuid()).thenReturn(seedJobUuid);
    when(jobDao.findJobByNameAsRow(NAMESPACE.getValue(), SEED_JOB_NAME.getValue()))
        .thenReturn(Optional.of(seedJob));
  }

  private static JobData newJob(int index) {
    JobData job = mock(JobData.class);
    JobName jobName = JobName.of("job-" + index);
    when(job.getUuid()).thenReturn(UUID.randomUUID());
    when(job.getId()).thenReturn(JobId.of(NAMESPACE, jobName));
    when(job.getNamespace()).thenReturn(NAMESPACE);
    when(job.getName()).thenReturn(jobName);
    when(job.getInputUuids()).thenReturn(Set.of());
    when(job.getOutputUuids()).thenReturn(Set.of());
    return job;
  }

  private static DatasetData newDataset(UUID uuid, String name) {
    DatasetData dataset = mock(DatasetData.class);
    DatasetName datasetName = DatasetName.of(name);
    when(dataset.getUuid()).thenReturn(uuid);
    when(dataset.getId()).thenReturn(new DatasetId(NAMESPACE, datasetName));
    when(dataset.getNamespace()).thenReturn(NAMESPACE);
    when(dataset.getName()).thenReturn(datasetName);
    return dataset;
  }

  private static Node node(Lineage lineage, NodeId nodeId) {
    return lineage.getGraph().stream()
        .filter(node -> node.getId().equals(nodeId))
        .findFirst()
        .orElseThrow();
  }
}
