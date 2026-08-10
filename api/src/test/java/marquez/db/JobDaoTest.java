/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.common.models.CommonModelGenerator.newJobName;
import static marquez.db.DbTestUtils.createJobWithSymlinkTarget;
import static marquez.db.DbTestUtils.createJobWithoutSymlinkTarget;
import static marquez.db.DbTestUtils.newJob;
import static marquez.service.models.ServiceModelGenerator.newJobMetaWith;
import static marquez.service.models.ServiceModelGenerator.newRunMeta;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import marquez.common.Utils;
import marquez.common.models.JobName;
import marquez.common.models.JobType;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.models.DbModelGenerator;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.Job;
import marquez.service.models.JobMeta;
import marquez.service.models.Run;
import marquez.service.models.RunMeta;
import org.assertj.core.api.AbstractObjectAssert;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.util.PGobject;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
public class JobDaoTest {

  private static JobDao jobDao;
  private static NamespaceDao namespaceDao;
  private static NamespaceRow namespace;
  private static Jdbi jdbi;

  @BeforeAll
  public static void setUpOnce(Jdbi jdbi) {
    JobDaoTest.jdbi = jdbi;
    jobDao = jdbi.onDemand(JobDao.class);
    namespaceDao = jdbi.onDemand(NamespaceDao.class);
    namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(),
            Instant.now(),
            JobDaoTest.class.getSimpleName(),
            JobDaoTest.class.getName());
  }

  @AfterEach
  public void cleanUp(Jdbi jdbi) {
    jdbi.inTransaction(h -> h.execute("DELETE FROM runs_input_mapping"));
    jdbi.inTransaction(h -> h.execute("DELETE FROM dataset_versions WHERE run_uuid IS NOT NULL"));
    jdbi.inTransaction(
        h -> h.execute("UPDATE runs SET start_run_state_uuid=NULL, end_run_state_uuid=NULL"));
    jdbi.inTransaction(h -> h.execute("DELETE FROM run_states"));
    jdbi.inTransaction(h -> h.execute("DELETE FROM runs"));
    jdbi.inTransaction(h -> h.execute("DELETE FROM run_args"));
    jdbi.inTransaction(h -> h.execute("DELETE FROM jobs_fqn"));
    jdbi.inTransaction(h -> h.execute("DELETE FROM jobs"));
  }

  @Test
  public void emptyUrl() {
    assertNull(jobDao.toUrlString(null));
  }

  @Test
  public void testFindSymlinkedJobByName() {
    JobRow targetJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, "targetJob", "the target of the symlink");
    JobRow symlinkJob =
        createJobWithSymlinkTarget(
            jdbi, namespace, "symlinkJob", targetJob.getUuid(), "the symlink job");
    Optional<Job> jobByName =
        jobDao.findJobByName(symlinkJob.getNamespaceName(), symlinkJob.getName());

    assertJobIdEquals(jobByName, targetJob.getNamespaceName(), targetJob.getName());
  }

  @Test
  public void testFindSymlinkedJobRowByName() {
    JobRow targetJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, "targetJob", "the target of the symlink");
    JobRow symlinkJob =
        createJobWithSymlinkTarget(
            jdbi, namespace, "symlinkJob", targetJob.getUuid(), "the symlink job");

    Optional<JobRow> jobByName =
        jobDao.findJobByNameAsRow(symlinkJob.getNamespaceName(), symlinkJob.getName());
    assertThat(jobByName)
        .isPresent()
        .get()
        .hasFieldOrPropertyWithValue("name", targetJob.getName())
        .hasFieldOrPropertyWithValue("namespaceName", targetJob.getNamespaceName());
  }

  @Test
  public void testFindAll() {
    JobRow targetJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, "targetJob", "the target of the symlink");
    JobRow symlinkJob =
        createJobWithSymlinkTarget(
            jdbi, namespace, "symlinkJob", targetJob.getUuid(), "the symlink job");
    JobRow anotherJobSameNamespace =
        createJobWithoutSymlinkTarget(jdbi, namespace, "anotherJob", "a random other job");

    List<RunState> runStates = new ArrayList<>();
    Collections.addAll(runStates, RunState.values());

    List<Job> jobs = jobDao.findAll(namespace.getName(), runStates, 10, 0);

    // the symlinked job isn't present in the response - only the symlink target and the job with
    // no symlink
    assertThat(jobs)
        .hasSize(2)
        .map(Job::getId)
        .containsExactlyInAnyOrder(
            DbModelGenerator.jobIdFor(namespace.getName(), targetJob.getName()),
            DbModelGenerator.jobIdFor(namespace.getName(), anotherJobSameNamespace.getName()));
  }

  @Test
  public void testFindAllWithNoNamespace() {
    String nullNamespace = null;

    List<RunState> runStates = new ArrayList<>();
    Collections.addAll(runStates, RunState.values());

    final List<Job> jobsWillBeEmpty = jobDao.findAll(nullNamespace, runStates, 10, 0);
    assertThat(jobsWillBeEmpty).isEmpty();

    JobName jobName0 = newJobName();
    JobName jobName1 = newJobName();
    JobName jobName2 = newJobName();

    JobRow job0 = newJob(jdbi, jobName0.getValue());
    JobRow job1 = newJob(jdbi, jobName1.getValue());
    JobRow job2 = newJob(jdbi, jobName2.getValue());

    final List<Job> jobsWillNotBeEmpty = jobDao.findAll(nullNamespace, runStates, 10, 0);
    assertThat(jobsWillNotBeEmpty)
        .isNotEmpty()
        .hasSize(3)
        .extracting(Job::getName)
        .containsExactlyInAnyOrder(jobName0, jobName1, jobName2);
  }

  @Test
  public void testFindAllAndCountForRouteByNamespacePresence() {
    JobDao routingJobDao = mock(JobDao.class, CALLS_REAL_METHODS);
    List<RunState> runStates = allRunStates();
    List<Job> globalJobs = List.of(mock(Job.class));
    List<Job> namespaceJobs = List.of(mock(Job.class));
    when(routingJobDao.findAllGlobal(runStates, 10, 0)).thenReturn(globalJobs);
    when(routingJobDao.findAllForNamespace(namespace.getName(), runStates, 10, 0))
        .thenReturn(namespaceJobs);
    when(routingJobDao.count()).thenReturn(7);
    when(routingJobDao.countForNamespace(namespace.getName())).thenReturn(3);

    assertThat(routingJobDao.findAll(null, runStates, 10, 0)).isSameAs(globalJobs);
    assertThat(routingJobDao.findAll(namespace.getName(), runStates, 10, 0))
        .isSameAs(namespaceJobs);
    assertThat(routingJobDao.countFor(null)).isEqualTo(7);
    assertThat(routingJobDao.countFor(namespace.getName())).isEqualTo(3);

    verify(routingJobDao).findAllGlobal(runStates, 10, 0);
    verify(routingJobDao).findAllForNamespace(namespace.getName(), runStates, 10, 0);
    verify(routingJobDao).count();
    verify(routingJobDao).countForNamespace(namespace.getName());
  }

  @Test
  public void testFindAllWithRunProjectsCurrentRunInputsAndOutputsAcrossNamespaces() {
    NamespaceRow secondNamespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), "current-run-second-namespace", getClass().getName());
    CurrentRunFixture first = createCurrentRunFixture(namespace, "same-job-name");
    CurrentRunFixture second = createCurrentRunFixture(secondNamespace, "same-job-name");

    List<Job> jobs = jobDao.findAllWithRun(null, allRunStates(), 10, 0);

    assertThat(jobs).hasSize(2);
    assertCurrentRunProjection(jobInNamespace(jobs, namespace), first);
    assertCurrentRunProjection(jobInNamespace(jobs, secondNamespace), second);
  }

  @Test
  public void testFindAllUsesDeterministicPaginationAcrossNamespaces() {
    NamespaceRow secondNamespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), "second-namespace", getClass().getName());
    Instant sharedUpdatedAt = Instant.parse("2024-01-01T00:00:00Z");
    createJobWithUuid(
        namespace,
        "paged-job-1",
        UUID.fromString("00000000-0000-0000-0000-000000000001"),
        sharedUpdatedAt);
    createJobWithUuid(
        secondNamespace,
        "paged-job-2",
        UUID.fromString("00000000-0000-0000-0000-000000000002"),
        sharedUpdatedAt);
    createJobWithUuid(
        namespace,
        "paged-job-3",
        UUID.fromString("00000000-0000-0000-0000-000000000003"),
        sharedUpdatedAt);
    createJobWithUuid(
        secondNamespace,
        "paged-job-4",
        UUID.fromString("00000000-0000-0000-0000-000000000004"),
        sharedUpdatedAt);

    List<Job> firstPage = jobDao.findAll(null, allRunStates(), 3, 0);
    List<Job> secondPage = jobDao.findAll(null, allRunStates(), 3, 3);

    assertThat(firstPage)
        .extracting(Job::getName)
        .containsExactly(
            JobName.of("paged-job-4"), JobName.of("paged-job-3"), JobName.of("paged-job-2"));
    assertThat(secondPage).hasSize(1);
    assertThat(secondPage).extracting(Job::getName).containsExactly(JobName.of("paged-job-1"));
    assertThat(firstPage)
        .extracting(Job::getNamespace)
        .contains(
            NamespaceName.of(namespace.getName()), NamespaceName.of(secondNamespace.getName()));
  }

  @Test
  public void testFindAllFiltersCurrentRunStateBeforeApplyingLimit() {
    createCurrentRunFixture(namespace, "excluded-newest", RunState.FAILED);
    createCurrentRunFixture(namespace, "eligible-newer", RunState.RUNNING);
    createCurrentRunFixture(namespace, "eligible-older", RunState.RUNNING);
    createJobWithoutSymlinkTarget(jdbi, namespace, "eligible-without-run", "no current run");
    setJobUpdatedAt(namespace, "excluded-newest", Instant.parse("2024-01-04T00:00:00Z"));
    setJobUpdatedAt(namespace, "eligible-newer", Instant.parse("2024-01-03T00:00:00Z"));
    setJobUpdatedAt(namespace, "eligible-older", Instant.parse("2024-01-02T00:00:00Z"));
    setJobUpdatedAt(namespace, "eligible-without-run", Instant.parse("2024-01-01T00:00:00Z"));

    List<Job> jobs = jobDao.findAll(namespace.getName(), List.of(RunState.RUNNING), 3, 0);

    assertThat(jobs)
        .extracting(Job::getName)
        .containsExactly(
            JobName.of("eligible-newer"),
            JobName.of("eligible-older"),
            JobName.of("eligible-without-run"));
  }

  @Test
  public void testFindAllWithRunHandlesEmptyAndNoCurrentRunPages() {
    assertThat(jobDao.findAllWithRun(null, allRunStates(), 10, 0)).isEmpty();
    JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespace.getName()), 2, 2);
    DbTestUtils.newJobWith(jdbi, namespace.getName(), "job-without-run", jobMeta);

    List<Job> jobs = jobDao.findAllWithRun(namespace.getName(), allRunStates(), 10, 0);

    assertThat(jobs).hasSize(1);
    assertThat(jobs.get(0).getLatestRun()).isEmpty();
    assertThat(jobs.get(0).getLatestRuns()).isEmpty();
    assertThat(jobs.get(0).getCurrentRunUuid()).isEmpty();
    assertThat(jobs.get(0).getInputs()).containsExactlyInAnyOrderElementsOf(jobMeta.getInputs());
  }

  @Test
  public void testFindAllWithRunUsesSingleCurrentRunLookup() {
    JobDao batchingJobDao = mock(JobDao.class, CALLS_REAL_METHODS);
    RunDao runDao = mock(RunDao.class);
    List<Job> page = new ArrayList<>();
    List<Run> runs = new ArrayList<>();
    Set<UUID> expectedRunUuids = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      UUID currentRunUuid = UUID.randomUUID();
      expectedRunUuids.add(currentRunUuid);
      Job job = mock(Job.class);
      when(job.getCurrentRunUuid()).thenReturn(Optional.of(currentRunUuid));
      page.add(job);

      Run run = mock(Run.class);
      when(run.getId()).thenReturn(RunId.of(currentRunUuid));
      when(run.getInputDatasetVersions()).thenReturn(List.of());
      when(run.getOutputDatasetVersions()).thenReturn(List.of());
      runs.add(run);
    }
    Job duplicateRunJob = mock(Job.class);
    UUID duplicateRunUuid = runs.get(0).getId().getValue();
    when(duplicateRunJob.getCurrentRunUuid()).thenReturn(Optional.of(duplicateRunUuid));
    page.add(duplicateRunJob);
    List<RunState> runStates = allRunStates();
    when(batchingJobDao.findAllGlobal(runStates, 101, 0)).thenReturn(page);
    when(batchingJobDao.createRunDao()).thenReturn(runDao);
    when(runDao.findRunsByUuids(anyCollection())).thenReturn(runs);

    assertThat(batchingJobDao.findAllWithRun(null, runStates, 101, 0)).hasSize(101);

    verify(runDao, times(1)).findRunsByUuids(eq(expectedRunUuids));
    verify(runDao, never()).findCurrentRunByJob(any(), any(), anyInt(), anyInt());
    verify(batchingJobDao, never()).createDatasetVersionDao();
  }

  @Test
  public void testFindAllWithRunSkipsRunLookupWithoutCurrentRuns() {
    JobDao batchingJobDao = mock(JobDao.class, CALLS_REAL_METHODS);
    Job job = mock(Job.class);
    when(job.getCurrentRunUuid()).thenReturn(Optional.empty());
    List<RunState> runStates = allRunStates();
    when(batchingJobDao.findAllGlobal(runStates, 10, 0)).thenReturn(List.of(job));

    assertThat(batchingJobDao.findAllWithRun(null, runStates, 10, 0)).containsExactly(job);

    verify(batchingJobDao, never()).createRunDao();
  }

  @Test
  public void testCountFor() {
    JobRow targetJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, "targetJob", "the target of the symlink");
    createJobWithSymlinkTarget(
        jdbi, namespace, "symlinkJob", targetJob.getUuid(), "the symlink job");
    createJobWithoutSymlinkTarget(jdbi, namespace, "anotherJob", "a random other job");
    createJobWithoutSymlinkTarget(jdbi, namespace, "aThirdJob", "a random third job");

    NamespaceRow anotherNamespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), Instant.now(), "anotherNamespace", getClass().getName());
    createJobWithSymlinkTarget(
        jdbi, anotherNamespace, "othernamespacejob", null, "job in another namespace");

    assertThat(jobDao.count()).isEqualTo(4);

    assertThat(jobDao.countFor(null)).isEqualTo(4);
    assertThat(jobDao.countFor(namespace.getName())).isEqualTo(3);
    assertThat(jobDao.countJobRuns(namespace.getName(), "targetJob")).isEqualTo(0);
  }

  @Test
  public void testUpsertJobWithNewSymlink() {
    JobRow targetJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, "targetJob", "the target of the symlink");

    String symlinkJobName = "symlinkJob";
    JobRow symlinkJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, symlinkJobName, "the symlink job");

    // the job queried is returned, since there is no symlink
    Optional<Job> jobByName =
        jobDao.findJobByName(symlinkJob.getNamespaceName(), symlinkJob.getName());
    assertJobIdEquals(jobByName, symlinkJob.getNamespaceName(), symlinkJob.getName());

    createJobWithSymlinkTarget(
        jdbi, namespace, symlinkJobName, targetJob.getUuid(), "the symlink job");

    // now the symlink target should be returned
    assertJobIdEquals(
        jobDao.findJobByName(symlinkJob.getNamespaceName(), symlinkJob.getName()),
        targetJob.getNamespaceName(),
        targetJob.getName());

    // upsert without the symlink target - the previous value should be respected
    createJobWithoutSymlinkTarget(jdbi, namespace, symlinkJobName, "the symlink job");

    // the symlink target should still be returned
    assertJobIdEquals(
        jobDao.findJobByName(symlinkJob.getNamespaceName(), symlinkJob.getName()),
        targetJob.getNamespaceName(),
        targetJob.getName());

    // try to update the symlink target - it should be ignored
    JobRow anotherTargetJob =
        createJobWithoutSymlinkTarget(
            jdbi, namespace, "anotherTarget", "we'll attempt to update the symlink");
    createJobWithSymlinkTarget(
        jdbi, namespace, symlinkJobName, anotherTargetJob.getUuid(), "the symlink job");

    // the original symlink target should be returned
    assertJobIdEquals(
        jobDao.findJobByName(symlinkJob.getNamespaceName(), symlinkJob.getName()),
        targetJob.getNamespaceName(),
        targetJob.getName());
  }

  public void testSymlinkParentJobRenamesChildren() throws SQLException {
    String parentJobName = "parentJob";
    JobRow parentJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, parentJobName, "the original parent job");
    Instant now = Instant.now();
    PGobject inputs = new PGobject();
    inputs.setValue("[]");
    inputs.setType("JSON");
    String childJob1Name = "child1";
    JobRow childJob1 =
        jobDao.upsertJob(
            UUID.randomUUID(),
            parentJob.getUuid(),
            JobType.BATCH,
            now,
            namespace.getUuid(),
            namespace.getName(),
            childJob1Name,
            null,
            null,
            null,
            inputs,
            null);

    String childJob2Name = "child2";
    JobRow childJob2 =
        jobDao.upsertJob(
            UUID.randomUUID(),
            parentJob.getUuid(),
            JobType.BATCH,
            now,
            namespace.getUuid(),
            namespace.getName(),
            childJob2Name,
            null,
            null,
            null,
            inputs,
            null);

    // the job queried is returned, since there is no symlink
    String jobFqn = parentJobName + "." + childJob1Name;
    Optional<Job> jobByName = jobDao.findJobByName(parentJob.getNamespaceName(), jobFqn);
    assertJobIdEquals(jobByName, parentJob.getNamespaceName(), jobFqn);

    JobRow targetJob =
        createJobWithoutSymlinkTarget(jdbi, namespace, "newParentJob", "the target of the symlink");

    createJobWithSymlinkTarget(
        jdbi, namespace, parentJobName, targetJob.getUuid(), "the symlink job");

    // now the renamed job should be returned
    String newJobFqn = targetJob.getName() + "." + childJob1Name;
    assertJobIdEquals(
        jobDao.findJobByName(parentJob.getNamespaceName(), jobFqn),
        targetJob.getNamespaceName(),
        newJobFqn);

    // query the second child by only its simple name
    String child2Fqn = targetJob.getName() + "." + childJob2Name;
    assertJobIdEquals(
        jobDao.findJobByName(parentJob.getNamespaceName(), child2Fqn),
        targetJob.getNamespaceName(),
        child2Fqn);
  }

  private AbstractObjectAssert<?, Job> assertJobIdEquals(
      Optional<Job> jobByName, String namespaceName, String jobName) {
    return assertThat(jobByName)
        .isPresent()
        .get()
        .hasFieldOrPropertyWithValue("id", DbModelGenerator.jobIdFor(namespaceName, jobName));
  }

  @Test
  public void pgObjectException() throws JsonProcessingException {
    ObjectMapper objectMapper = mock(ObjectMapper.class);
    when(objectMapper.writeValueAsString(any())).thenThrow(new RuntimeException());
    assertNull(jobDao.toJson(null, objectMapper));
  }

  private static List<RunState> allRunStates() {
    return List.of(RunState.values());
  }

  private CurrentRunFixture createCurrentRunFixture(NamespaceRow fixtureNamespace, String jobName) {
    return createCurrentRunFixture(fixtureNamespace, jobName, RunState.RUNNING);
  }

  private CurrentRunFixture createCurrentRunFixture(
      NamespaceRow fixtureNamespace, String jobName, RunState runState) {
    NamespaceName namespaceName = NamespaceName.of(fixtureNamespace.getName());
    JobMeta generatedJobMeta = newJobMetaWith(namespaceName, 2, 2);
    RunId currentRunId = RunId.of(UUID.randomUUID());
    JobMeta jobMeta =
        new JobMeta(
            generatedJobMeta.getType(),
            generatedJobMeta.getInputs(),
            generatedJobMeta.getOutputs(),
            generatedJobMeta.getLocation().orElse(null),
            generatedJobMeta.getDescription().orElse(null),
            currentRunId,
            generatedJobMeta.getTags());
    JobRow jobRow = DbTestUtils.newJobWith(jdbi, fixtureNamespace.getName(), jobName, jobMeta);
    RunMeta generatedRunMeta = newRunMeta();
    RunMeta runMeta =
        new RunMeta(
            currentRunId,
            generatedRunMeta.getNominalStartTime().orElse(null),
            generatedRunMeta.getNominalEndTime().orElse(null),
            generatedRunMeta.getArgs());
    RunDao runDao = jdbi.onDemand(RunDao.class);
    RunRow currentRun = runDao.upsertRunMeta(namespaceName, jobRow, runMeta, runState);
    runDao.upsertOutputDatasetsFor(currentRun.getUuid(), jobMeta.getOutputs());
    return new CurrentRunFixture(currentRunId, jobMeta);
  }

  private static JobRow createJobWithUuid(
      NamespaceRow fixtureNamespace, String jobName, UUID jobUuid, Instant updatedAt) {
    return jobDao.upsertJob(
        jobUuid,
        JobType.BATCH,
        updatedAt,
        fixtureNamespace.getUuid(),
        fixtureNamespace.getName(),
        jobName,
        "pagination fixture",
        null,
        null,
        jobDao.toJson(Collections.emptySet(), Utils.getMapper()),
        null);
  }

  private static void setJobUpdatedAt(
      NamespaceRow fixtureNamespace, String jobName, Instant updatedAt) {
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    "UPDATE jobs SET updated_at=:updatedAt "
                        + "WHERE namespace_uuid=:namespaceUuid AND name=:jobName")
                .bind("updatedAt", updatedAt)
                .bind("namespaceUuid", fixtureNamespace.getUuid())
                .bind("jobName", jobName)
                .execute());
  }

  private static Job jobInNamespace(List<Job> jobs, NamespaceRow expectedNamespace) {
    return jobs.stream()
        .filter(job -> job.getNamespace().getValue().equals(expectedNamespace.getName()))
        .findFirst()
        .orElseThrow();
  }

  private static void assertCurrentRunProjection(Job projectedJob, CurrentRunFixture fixture) {
    assertThat(projectedJob.getLatestRun())
        .isPresent()
        .get()
        .extracting(Run::getId)
        .isEqualTo(fixture.currentRunId());
    assertThat(projectedJob.getLatestRuns().orElseThrow()).hasSize(1);
    assertThat(projectedJob.getInputs())
        .containsExactlyInAnyOrderElementsOf(fixture.jobMeta().getInputs());
    assertThat(projectedJob.getOutputs())
        .containsExactlyInAnyOrderElementsOf(fixture.jobMeta().getOutputs());
  }

  private record CurrentRunFixture(RunId currentRunId, JobMeta jobMeta) {}
}
