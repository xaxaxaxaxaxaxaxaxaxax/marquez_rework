/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.common.models.CommonModelGenerator.newJobName;
import static marquez.db.DbTestUtils.createJobWithSymlinkTarget;
import static marquez.db.DbTestUtils.createJobWithoutSymlinkTarget;
import static marquez.db.DbTestUtils.newJobWith;
import static marquez.service.models.ServiceModelGenerator.newJobMetaWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import marquez.api.JdbiUtils;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetVersionId;
import marquez.common.models.InputDatasetVersion;
import marquez.common.models.NamespaceName;
import marquez.common.models.OutputDatasetVersion;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.models.DatasetRow;
import marquez.db.models.ExtendedRunRow;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.JobMeta;
import marquez.service.models.Run;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class RunDaoTest {

  private static RunDao runDao;
  private static Jdbi jdbi;
  private static JobVersionDao jobVersionDao;
  private static OpenLineageDao openLineageDao;

  static NamespaceRow namespaceRow;
  static JobRow jobRow;

  @BeforeAll
  public static void setUpOnce(Jdbi jdbi) {
    RunDaoTest.jdbi = jdbi;
    runDao = jdbi.onDemand(RunDao.class);
    jobVersionDao = jdbi.onDemand(JobVersionDao.class);
    openLineageDao = jdbi.onDemand(OpenLineageDao.class);
    namespaceRow = DbTestUtils.newNamespace(jdbi);
    jobRow = DbTestUtils.newJob(jdbi, namespaceRow.getName(), newJobName().getValue());
  }

  @AfterEach
  public void tearDown(Jdbi jdbi) {
    JdbiUtils.cleanDatabase(jdbi);
  }

  @Test
  public void getRun() {

    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        newJobWith(jdbi, namespaceRow.getName(), newJobName().getValue(), jobMeta);

    final RunRow runRow = DbTestUtils.newRun(jdbi, jobRow);
    DbTestUtils.transitionRunWithOutputs(
        jdbi, runRow.getUuid(), RunState.COMPLETED, jobMeta.getOutputs());

    jobVersionDao.upsertJobVersionOnRunTransition(
        jobVersionDao.loadJobRowRunDetails(jobRow, runRow.getUuid()),
        RunState.COMPLETED,
        Instant.now(),
        true);

    Optional<Run> run = runDao.findRunByUuid(runRow.getUuid());
    assertThat(run)
        .isPresent()
        .get()
        .extracting(
            Run::getInputDatasetVersions, InstanceOfAssertFactories.list(InputDatasetVersion.class))
        .hasSize(jobMeta.getInputs().size())
        .map(InputDatasetVersion::getDatasetVersionId)
        .map(DatasetVersionId::getName)
        .containsAll(
            jobMeta.getInputs().stream().map(DatasetId::getName).collect(Collectors.toSet()));

    assertThat(run)
        .get()
        .extracting(
            Run::getOutputDatasetVersions,
            InstanceOfAssertFactories.list(OutputDatasetVersion.class))
        .hasSize(jobMeta.getOutputs().size())
        .map(OutputDatasetVersion::getDatasetVersionId)
        .map(DatasetVersionId::getName)
        .containsAll(
            jobMeta.getOutputs().stream().map(DatasetId::getName).collect(Collectors.toSet()));
  }

  @Test
  public void findRunsByUuidsMatchesPointReadsAndHandlesEmptyInput() {
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        newJobWith(jdbi, namespaceRow.getName(), newJobName().getValue(), jobMeta);
    List<RunRow> runRows =
        createRunsForJob(jobRow, 3, jobMeta.getOutputs()).collect(Collectors.toList());
    List<UUID> requestedRunUuids =
        List.of(runRows.get(0).getUuid(), runRows.get(2).getUuid(), UUID.randomUUID());

    List<Run> expectedRuns =
        requestedRunUuids.stream()
            .map(runDao::findRunByUuid)
            .flatMap(Optional::stream)
            .collect(Collectors.toList());
    List<Run> batchRuns = runDao.findRunsByUuids(requestedRunUuids);

    assertThat(batchRuns)
        .map(Run::getId)
        .containsExactlyInAnyOrderElementsOf(
            expectedRuns.stream().map(Run::getId).collect(Collectors.toList()));
    expectedRuns.forEach(
        expectedRun -> {
          Run batchRun =
              batchRuns.stream()
                  .filter(run -> run.getId().equals(expectedRun.getId()))
                  .findFirst()
                  .orElseThrow();
          assertThat(batchRun)
              .usingRecursiveComparison()
              .ignoringCollectionOrder()
              .isEqualTo(expectedRun);
        });
    assertThat(runDao.findRunsByUuids(List.of())).isEmpty();
  }

  @Test
  public void findRunsByUuidsBindsLargeUuidArray() {
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        newJobWith(jdbi, namespaceRow.getName(), newJobName().getValue(), jobMeta);
    final RunRow existingRun = DbTestUtils.newRun(jdbi, jobRow);
    final List<UUID> requestedRunUuids =
        IntStream.range(0, 4096)
            .mapToObj(value -> new UUID(0L, value))
            .collect(Collectors.toCollection(ArrayList::new));
    requestedRunUuids.add(existingRun.getUuid());

    assertThat(runDao.findRunsByUuids(requestedRunUuids))
        .map(Run::getId)
        .containsExactly(RunId.of(existingRun.getUuid()));
  }

  @Test
  public void updateInputMappingsSkipsEmptyInputAndPreservesSingularCompatibility() {
    RunDao batchingDao = mock(RunDao.class, CALLS_REAL_METHODS);
    UUID runUuid = UUID.randomUUID();

    batchingDao.updateInputMappings(runUuid, List.of());

    verify(batchingDao, never()).insertInputMappingsChunk(eq(runUuid), any(UUID[].class));

    UUID datasetVersionUuid = UUID.randomUUID();
    batchingDao.updateInputMapping(runUuid, datasetVersionUuid);

    ArgumentCaptor<UUID[]> chunk = ArgumentCaptor.forClass(UUID[].class);
    verify(batchingDao).insertInputMappingsChunk(eq(runUuid), chunk.capture());
    assertThat(chunk.getValue()).containsExactly(datasetVersionUuid);
  }

  @Test
  public void updateInputMappingsUsesOneChunkAtTheBoundary() {
    RunDao batchingDao = mock(RunDao.class, CALLS_REAL_METHODS);
    UUID runUuid = UUID.randomUUID();
    List<UUID> datasetVersionUuids = uuidSequence(RunDao.RUN_INPUT_MAPPING_CHUNK_SIZE);

    batchingDao.updateInputMappingsInTransaction(runUuid, datasetVersionUuids);

    ArgumentCaptor<UUID[]> chunk = ArgumentCaptor.forClass(UUID[].class);
    verify(batchingDao).insertInputMappingsChunk(eq(runUuid), chunk.capture());
    assertThat(chunk.getValue()).containsExactlyElementsOf(datasetVersionUuids);
  }

  @Test
  public void updateInputMappingsChunksAndDeduplicatesAcrossChunkBoundaries() {
    RunDao batchingDao = mock(RunDao.class, CALLS_REAL_METHODS);
    UUID runUuid = UUID.randomUUID();
    List<UUID> uniqueDatasetVersionUuids = uuidSequence(RunDao.RUN_INPUT_MAPPING_CHUNK_SIZE + 1);
    List<UUID> repeatedDatasetVersionUuids = new ArrayList<>(uniqueDatasetVersionUuids);
    repeatedDatasetVersionUuids.add(1, uniqueDatasetVersionUuids.get(0));
    repeatedDatasetVersionUuids.add(uniqueDatasetVersionUuids.get(999));

    batchingDao.updateInputMappingsInTransaction(runUuid, repeatedDatasetVersionUuids);

    ArgumentCaptor<UUID[]> chunks = ArgumentCaptor.forClass(UUID[].class);
    verify(batchingDao, times(2)).insertInputMappingsChunk(eq(runUuid), chunks.capture());
    assertThat(chunks.getAllValues().get(0)).hasSize(RunDao.RUN_INPUT_MAPPING_CHUNK_SIZE);
    assertThat(chunks.getAllValues().get(1)).hasSize(1);
    List<UUID> actualDatasetVersionUuids = new ArrayList<>();
    for (UUID[] chunk : chunks.getAllValues()) {
      actualDatasetVersionUuids.addAll(List.of(chunk));
    }
    assertThat(actualDatasetVersionUuids).containsExactlyElementsOf(uniqueDatasetVersionUuids);
  }

  @Test
  public void updateInputMappingsRollsBackEarlierChunksWhenALaterChunkFails() {
    JobRow rollbackJob = DbTestUtils.newJob(jdbi, namespaceRow.getName(), newJobName().getValue());
    RunRow rollbackRun = DbTestUtils.newRun(jdbi, rollbackJob);
    String datasetName = "run-input-chunk-rollback";
    DbTestUtils.newDataset(jdbi, rollbackJob.getNamespaceName(), datasetName);
    DatasetRow datasetRow =
        jdbi.onDemand(DatasetDao.class)
            .findDatasetAsRow(rollbackJob.getNamespaceName(), datasetName)
            .orElseThrow();
    List<UUID> validDatasetVersionUuids =
        Stream.generate(UUID::randomUUID)
            .limit(RunDao.RUN_INPUT_MAPPING_CHUNK_SIZE)
            .collect(Collectors.toList());
    List<UUID> versions =
        Stream.generate(UUID::randomUUID)
            .limit(RunDao.RUN_INPUT_MAPPING_CHUNK_SIZE)
            .collect(Collectors.toList());
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    INSERT INTO dataset_versions (
                      uuid, created_at, dataset_uuid, version, namespace_name, dataset_name)
                    SELECT generated.uuid, NOW(), :datasetUuid, generated.version,
                           :namespaceName, :datasetName
                    FROM unnest(
                      CAST(:datasetVersionUuids AS uuid[]), CAST(:versions AS uuid[]))
                      AS generated(uuid, version)
                    """)
                .bind("datasetUuid", datasetRow.getUuid())
                .bind("namespaceName", rollbackJob.getNamespaceName())
                .bind("datasetName", datasetName)
                .bind("datasetVersionUuids", validDatasetVersionUuids.toArray(UUID[]::new))
                .bind("versions", versions.toArray(UUID[]::new))
                .execute());
    int mappingsBefore = countRunInputMappings(rollbackRun.getUuid());
    List<UUID> mappingsWithInvalidLastChunk = new ArrayList<>(validDatasetVersionUuids);
    mappingsWithInvalidLastChunk.add(UUID.randomUUID());

    assertThatThrownBy(
            () -> runDao.updateInputMappings(rollbackRun.getUuid(), mappingsWithInvalidLastChunk))
        .isInstanceOf(RuntimeException.class);

    assertThat(countRunInputMappings(rollbackRun.getUuid())).isEqualTo(mappingsBefore);
  }

  @Test
  public void getFindAll() {

    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        newJobWith(jdbi, namespaceRow.getName(), newJobName().getValue(), jobMeta);

    Set<RunRow> expectedRuns =
        createRunsForJob(jobRow, 5, jobMeta.getOutputs()).collect(Collectors.toSet());
    List<Run> runs = runDao.findAll(jobRow.getNamespaceName(), jobRow.getName(), 10, 0);
    assertThat(runs)
        .hasSize(expectedRuns.size())
        .map(Run::getId)
        .map(RunId::getValue)
        .containsAll(expectedRuns.stream().map(RunRow::getUuid).collect(Collectors.toSet()));
  }

  @Test
  public void getFindAllUsesStableUuidTieBreakerAcrossPages() {
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        newJobWith(jdbi, namespaceRow.getName(), newJobName().getValue(), jobMeta);
    final RunArgsRow runArgsRow = DbTestUtils.newRunArgs(jdbi);
    final Instant now = Instant.parse("2024-01-01T00:00:00Z");
    final UUID firstRunUuid = UUID.fromString("00000000-0000-0000-0000-000000000001");
    final UUID secondRunUuid = UUID.fromString("00000000-0000-0000-0000-000000000002");
    final UUID thirdRunUuid = UUID.fromString("00000000-0000-0000-0000-000000000003");

    for (UUID runUuid : List.of(firstRunUuid, secondRunUuid, thirdRunUuid)) {
      runDao.upsert(
          runUuid,
          null,
          runUuid.toString(),
          now,
          jobRow.getUuid(),
          null,
          runArgsRow.getUuid(),
          null,
          null,
          jobRow.getNamespaceName(),
          jobRow.getName(),
          null);
    }

    assertThat(runDao.findAll(jobRow.getNamespaceName(), jobRow.getName(), 2, 0))
        .map(Run::getId)
        .map(RunId::getValue)
        .containsExactly(thirdRunUuid, secondRunUuid);
    assertThat(runDao.findAll(jobRow.getNamespaceName(), jobRow.getName(), 2, 2))
        .map(Run::getId)
        .map(RunId::getValue)
        .containsExactly(firstRunUuid);
  }

  @Test
  public void getFindAllForSymlinkedJob() {
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        newJobWith(jdbi, namespaceRow.getName(), newJobName().getValue(), jobMeta);

    final JobRow symlinkJob =
        createJobWithSymlinkTarget(
            jdbi, namespaceRow, newJobName().getValue(), jobRow.getUuid(), "symlink job");

    Set<RunRow> expectedRuns =
        Stream.concat(
                createRunsForJob(symlinkJob, 3, jobMeta.getOutputs()),
                createRunsForJob(jobRow, 2, jobMeta.getOutputs()))
            .collect(Collectors.toSet());

    // all runs should be present
    List<Run> runs = runDao.findAll(jobRow.getNamespaceName(), jobRow.getName(), 10, 0);
    assertThat(runs)
        .hasSize(expectedRuns.size())
        .map(Run::getId)
        .map(RunId::getValue)
        .containsAll(expectedRuns.stream().map(RunRow::getUuid).collect(Collectors.toSet()));
  }

  @Test
  public void currentAndLatestRunQueriesIsolateUnrelatedAliasedJobs() {
    final JobRow aliasedTarget =
        createJobWithoutSymlinkTarget(
            jdbi, namespaceRow, newJobName().getValue(), "aliased target");
    final String aliasName = newJobName().getValue();
    createJobWithSymlinkTarget(jdbi, namespaceRow, aliasName, aliasedTarget.getUuid(), "job alias");
    final JobRow isolatedJob =
        createJobWithoutSymlinkTarget(jdbi, namespaceRow, newJobName().getValue(), "isolated job");

    final RunRow aliasedTargetRun = DbTestUtils.newRun(jdbi, aliasedTarget);
    final RunRow isolatedRun = DbTestUtils.newRun(jdbi, isolatedJob);
    jdbi.useHandle(
        handle -> {
          handle
              .createUpdate("UPDATE jobs SET current_run_uuid = :runUuid WHERE uuid = :jobUuid")
              .bind("runUuid", aliasedTargetRun.getUuid())
              .bind("jobUuid", aliasedTarget.getUuid())
              .execute();
          handle
              .createUpdate("UPDATE jobs SET current_run_uuid = :runUuid WHERE uuid = :jobUuid")
              .bind("runUuid", isolatedRun.getUuid())
              .bind("jobUuid", isolatedJob.getUuid())
              .execute();
        });

    assertThat(
            runDao.findCurrentRunByJob(
                isolatedJob.getNamespaceName(), isolatedJob.getName(), 10, 0))
        .map(Run::getId)
        .map(RunId::getValue)
        .containsExactly(isolatedRun.getUuid());
    assertThat(runDao.findByLatestJob(isolatedJob.getNamespaceName(), isolatedJob.getName(), 10, 0))
        .map(Run::getId)
        .map(RunId::getValue)
        .containsExactly(isolatedRun.getUuid());
    assertThat(runDao.findByLatestJob(aliasedTarget.getNamespaceName(), aliasName, 10, 0))
        .map(Run::getId)
        .map(RunId::getValue)
        .containsExactly(aliasedTargetRun.getUuid());
    assertThat(runDao.findCurrentRunByJob(aliasedTarget.getNamespaceName(), aliasName, 10, 0))
        .map(Run::getId)
        .map(RunId::getValue)
        .containsExactly(aliasedTargetRun.getUuid());
  }

  @Test
  public void testFindByLatestJob() {
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        newJobWith(jdbi, namespaceRow.getName(), newJobName().getValue(), jobMeta);
    Set<RunRow> runs =
        createRunsForJob(jobRow, 5, jobMeta.getOutputs()).collect(Collectors.toSet());

    TreeSet<RunRow> sortedRuns =
        new TreeSet<>(Comparator.comparing(RunRow::getUpdatedAt).reversed());
    sortedRuns.addAll(runs);
    Run byLatestJob =
        runDao.findByLatestJob(jobRow.getNamespaceName(), jobRow.getName(), 1, 0).get(0);
    assertThat(byLatestJob)
        .hasFieldOrPropertyWithValue("id", new RunId(sortedRuns.first().getUuid()));

    JobRow newTargetJob =
        createJobWithoutSymlinkTarget(jdbi, namespaceRow, "newTargetJob", "a symlink target");

    // update the old job to point to the new targets
    createJobWithSymlinkTarget(
        jdbi,
        namespaceRow,
        jobRow.getName(),
        newTargetJob.getUuid(),
        jobMeta.getDescription().orElse(null));

    // get the latest run for the *newTargetJob*. It should be the same as the old job's latest run
    byLatestJob =
        runDao
            .findByLatestJob(newTargetJob.getNamespaceName(), newTargetJob.getName(), 1, 0)
            .get(0);
    assertThat(byLatestJob)
        .hasFieldOrPropertyWithValue("id", new RunId(sortedRuns.first().getUuid()));
  }

  @NotNull
  private Stream<RunRow> createRunsForJob(
      JobRow jobRow, int count, ImmutableSet<DatasetId> outputs) {
    return IntStream.range(0, count)
        .mapToObj(
            i -> {
              final RunRow runRow = DbTestUtils.newRun(jdbi, jobRow);
              DbTestUtils.transitionRunWithOutputs(
                  jdbi, runRow.getUuid(), RunState.COMPLETED, outputs);

              jobVersionDao.upsertJobVersionOnRunTransition(
                  jobVersionDao.loadJobRowRunDetails(jobRow, runRow.getUuid()),
                  RunState.COMPLETED,
                  Instant.now(),
                  true);
              return runRow;
            });
  }

  @Test
  public void updateRowWithNullNominalTimeDoesNotUpdateNominalTime() {
    final RunDao runDao = jdbi.onDemand(RunDao.class);

    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        newJobWith(jdbi, namespaceRow.getName(), newJobName().getValue(), jobMeta);

    RunRow row = DbTestUtils.newRun(jdbi, jobRow);

    RunRow updatedRow =
        runDao.upsert(
            row.getUuid(),
            null,
            row.getUuid().toString(),
            row.getUpdatedAt(),
            jobRow.getUuid(),
            null,
            row.getRunArgsUuid(),
            null,
            null,
            namespaceRow.getName(),
            jobRow.getName(),
            null);

    assertThat(row.getUuid()).isEqualTo(updatedRow.getUuid());
    assertThat(row.getNominalStartTime()).isNotNull();
    assertThat(row.getNominalEndTime()).isNotNull();
    assertThat(updatedRow.getNominalStartTime()).isEqualTo(row.getNominalStartTime());
    assertThat(updatedRow.getNominalEndTime()).isEqualTo(row.getNominalEndTime());
  }

  @Test
  void runUpsertAddsAParentOnceAndNeverReplacesIt() {
    JobRow childJob =
        newJobWith(
            jdbi,
            namespaceRow.getName(),
            newJobName().getValue(),
            newJobMetaWith(NamespaceName.of(namespaceRow.getName())));
    JobRow parentJob =
        newJobWith(
            jdbi,
            namespaceRow.getName(),
            newJobName().getValue(),
            newJobMetaWith(NamespaceName.of(namespaceRow.getName())));
    RunArgsRow args = DbTestUtils.newRunArgs(jdbi);
    UUID childRunUuid = UUID.randomUUID();
    UUID firstParentUuid = DbTestUtils.newRun(jdbi, parentJob).getUuid();
    UUID conflictingParentUuid = DbTestUtils.newRun(jdbi, parentJob).getUuid();
    Instant now = Instant.parse("2026-08-13T01:00:00Z");

    runDao.upsert(
        childRunUuid,
        null,
        childRunUuid.toString(),
        now,
        childJob.getUuid(),
        null,
        args.getUuid(),
        null,
        null,
        namespaceRow.getName(),
        childJob.getName(),
        null);
    assertThat(runDao.findRunByUuidAsRow(childRunUuid).orElseThrow().getParentRunUuid()).isEmpty();

    runDao.upsert(
        childRunUuid,
        firstParentUuid,
        childRunUuid.toString(),
        now.plusSeconds(1),
        childJob.getUuid(),
        null,
        args.getUuid(),
        null,
        null,
        namespaceRow.getName(),
        childJob.getName(),
        null);
    runDao.upsert(
        childRunUuid,
        conflictingParentUuid,
        childRunUuid.toString(),
        now.plusSeconds(2),
        childJob.getUuid(),
        null,
        args.getUuid(),
        null,
        null,
        RunState.RUNNING,
        now.plusSeconds(2),
        namespaceRow.getName(),
        childJob.getName(),
        null);

    assertThat(runDao.findRunByUuidAsRow(childRunUuid).orElseThrow().getParentRunUuid())
        .contains(firstParentUuid);
  }

  @Test
  void syntheticAndObservedNormalAndLegacyCandidatesConverge() {
    JobRow owner = newRunOwner();
    RunArgsRow neutralArgs = DbTestUtils.newRunArgs(jdbi);
    RunArgsRow observedArgs = DbTestUtils.newRunArgs(jdbi);
    String externalId = "legacy-parent-run";
    UUID normal = Utils.openLineageRunUuid(externalId);
    UUID legacy = Utils.toNameBasedUuid(owner.getNamespaceName(), owner.getName(), externalId);

    RunRow fromLegacy = syntheticParent(owner, neutralArgs, legacy, externalId);
    RunRow fromNormal = syntheticParent(owner, neutralArgs, normal, externalId);

    assertThat(fromLegacy.getUuid()).isEqualTo(legacy);
    assertThat(fromNormal.getUuid()).isEqualTo(legacy);
    assertThat(fromLegacy.getCreatedAt()).isEqualTo(Instant.EPOCH);
    assertThat(parentPlaceholder(legacy)).isTrue();
    assertThat(runCount(owner.getUuid(), externalId)).isEqualTo(1);

    RunRow observed =
        runDao.upsertOpenLineageRun(
            observedRun(owner, observedArgs, externalId, Instant.parse("2026-08-14T00:30:00Z")));

    assertThat(observed.getUuid()).isEqualTo(legacy);
    assertThat(observed.getRunArgsUuid()).isEqualTo(observedArgs.getUuid());
    assertThat(parentPlaceholderIsNull(legacy)).isTrue();
    assertThat(runCount(owner.getUuid(), externalId)).isEqualTo(1);
  }

  @Test
  void observedRunPromotesMatchingPlaceholderOnceAndPreservesRealIdentityFields() {
    JobRow owner = newRunOwner();
    RunArgsRow neutralArgs = DbTestUtils.newRunArgs(jdbi);
    RunArgsRow observedArgs = DbTestUtils.newRunArgs(jdbi);
    RunArgsRow laterArgs = DbTestUtils.newRunArgs(jdbi);
    UUID normal = UUID.randomUUID();
    String syntheticSpelling = normal.toString().toUpperCase();
    String observedSpelling = normal.toString();
    syntheticParent(owner, neutralArgs, normal, syntheticSpelling);
    Instant observedAt = Instant.parse("2026-08-14T01:00:00Z");

    RunRow promoted =
        runDao.upsertOpenLineageRun(observedRun(owner, observedArgs, observedSpelling, observedAt));
    RunRow repeated =
        runDao.upsertOpenLineageRun(
            observedRun(owner, laterArgs, observedSpelling, observedAt.plusSeconds(30)));

    assertThat(promoted.getUuid()).isEqualTo(normal);
    assertThat(promoted.getCreatedAt()).isEqualTo(observedAt);
    assertThat(promoted.getRunArgsUuid()).isEqualTo(observedArgs.getUuid());
    assertThat(parentPlaceholderIsNull(normal)).isTrue();
    assertThat(repeated.getCreatedAt()).isEqualTo(observedAt);
    assertThat(repeated.getRunArgsUuid()).isEqualTo(observedArgs.getUuid());
    assertThat(runExternalId(normal)).isEqualTo(syntheticSpelling);
  }

  @Test
  void observedRunReusesUnclassifiedLegacyCandidateWithoutPromotingIdentityFields() {
    JobRow owner = newRunOwner();
    RunArgsRow legacyArgs = DbTestUtils.newRunArgs(jdbi);
    RunArgsRow observedArgs = DbTestUtils.newRunArgs(jdbi);
    String externalId = "unclassified-parent-" + UUID.randomUUID();
    UUID normal = Utils.openLineageRunUuid(externalId);
    Instant legacyCreatedAt = Instant.parse("2000-01-01T00:00:00Z");
    runDao.upsert(
        normal,
        null,
        externalId,
        legacyCreatedAt,
        owner.getUuid(),
        null,
        legacyArgs.getUuid(),
        null,
        null,
        owner.getNamespaceName(),
        owner.getName(),
        "legacy-location");
    assertThat(parentPlaceholderIsNull(normal)).isTrue();

    Instant observedAt = Instant.parse("2026-08-14T01:30:00Z");
    RunRow observed =
        runDao.upsertOpenLineageRun(observedRun(owner, observedArgs, externalId, observedAt));

    assertThat(observed.getUuid()).isEqualTo(normal);
    assertThat(observed.getCreatedAt()).isEqualTo(legacyCreatedAt);
    assertThat(observed.getRunArgsUuid()).isEqualTo(legacyArgs.getUuid());
    assertThat(observed.getUpdatedAt()).isEqualTo(observedAt);
    assertThat(observed.getCurrentRunState()).contains(RunState.RUNNING.name());
    assertThat(parentPlaceholderIsNull(normal)).isTrue();
    assertThat(runCount(owner.getUuid(), externalId)).isEqualTo(1);
  }

  @Test
  void foreignNormalIdentityUsesOneRepairWithoutMutatingForeignRun() {
    JobRow owner = newRunOwner();
    JobRow foreignOwner = newRunOwner();
    RunArgsRow foreignArgs = DbTestUtils.newRunArgs(jdbi);
    RunArgsRow observedArgs = DbTestUtils.newRunArgs(jdbi);
    String externalId = "shared-reported-run";
    UUID normal = Utils.openLineageRunUuid(externalId);
    Instant foreignTime = Instant.parse("2026-08-14T02:00:00Z");
    runDao.upsert(
        normal,
        null,
        externalId,
        foreignTime,
        foreignOwner.getUuid(),
        null,
        foreignArgs.getUuid(),
        foreignTime,
        foreignTime.plusSeconds(5),
        RunState.COMPLETED,
        foreignTime.plusSeconds(5),
        foreignOwner.getNamespaceName(),
        foreignOwner.getName(),
        "foreign-location");
    Map<String, Object> before = runIdentityState(normal);

    RunRow resolved =
        runDao.upsertOpenLineageRun(
            observedRun(owner, observedArgs, externalId, foreignTime.plusSeconds(30)));

    assertThat(resolved.getUuid())
        .isEqualTo(
            Utils.toNameBasedUuid(owner.getNamespaceName(), owner.getName(), normal.toString()));
    assertThat(runIdentityState(normal)).isEqualTo(before);
  }

  @Test
  void mismatchedSameJobCandidateIsSkippedWithoutMutation() {
    JobRow owner = newRunOwner();
    RunArgsRow existingArgs = DbTestUtils.newRunArgs(jdbi);
    RunArgsRow observedArgs = DbTestUtils.newRunArgs(jdbi);
    String externalId = "intended-run";
    UUID normal = Utils.openLineageRunUuid(externalId);
    Instant existingTime = Instant.parse("2026-08-14T03:00:00Z");
    runDao.upsert(
        normal,
        null,
        "different-run",
        existingTime,
        owner.getUuid(),
        null,
        existingArgs.getUuid(),
        null,
        null,
        owner.getNamespaceName(),
        owner.getName(),
        "existing-location");
    Map<String, Object> before = runIdentityState(normal);

    RunRow resolved =
        runDao.upsertOpenLineageRun(
            observedRun(owner, observedArgs, externalId, existingTime.plusSeconds(30)));

    assertThat(resolved.getUuid()).isNotEqualTo(normal);
    assertThat(runIdentityState(normal)).isEqualTo(before);
  }

  @Test
  void twoMatchingBoundedCandidatesFailWithoutMutation() {
    JobRow owner = newRunOwner();
    RunArgsRow args = DbTestUtils.newRunArgs(jdbi);
    String externalId = "ambiguous-run";
    UUID normal = Utils.openLineageRunUuid(externalId);
    UUID legacy = Utils.toNameBasedUuid(owner.getNamespaceName(), owner.getName(), externalId);
    Instant initial = Instant.parse("2026-08-14T04:00:00Z");
    insertRealRun(owner, args, normal, externalId, initial);
    insertRealRun(owner, args, legacy, externalId, initial);
    Map<String, Object> normalBefore = runIdentityState(normal);
    Map<String, Object> legacyBefore = runIdentityState(legacy);

    assertThatThrownBy(
            () ->
                runDao.upsertOpenLineageRun(
                    observedRun(owner, args, externalId, initial.plusSeconds(30))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Multiple OpenLineage runs match");
    assertThat(runIdentityState(normal)).isEqualTo(normalBefore);
    assertThat(runIdentityState(legacy)).isEqualTo(legacyBefore);
  }

  @Test
  void rolledBackPromotionRestoresNeutralMarkerAndFields() {
    JobRow owner = newRunOwner();
    RunArgsRow neutralArgs = DbTestUtils.newRunArgs(jdbi);
    RunArgsRow observedArgs = DbTestUtils.newRunArgs(jdbi);
    String externalId = "rollback-parent";
    UUID normal = Utils.openLineageRunUuid(externalId);
    syntheticParent(owner, neutralArgs, normal, externalId);

    assertThatThrownBy(
            () ->
                jdbi.useTransaction(
                    handle -> {
                      RunDao transactional = handle.attach(RunDao.class);
                      transactional.upsertOpenLineageRun(
                          observedRun(
                              owner,
                              observedArgs,
                              externalId,
                              Instant.parse("2026-08-14T05:00:00Z")));
                      assertThat(parentPlaceholder(handle, normal)).isFalse();
                      throw new IllegalStateException("rollback probe");
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("rollback probe");

    RunRow restored = runDao.findRunByUuidAsRow(normal).orElseThrow();
    assertThat(restored.getCreatedAt()).isEqualTo(Instant.EPOCH);
    assertThat(restored.getRunArgsUuid()).isEqualTo(neutralArgs.getUuid());
    assertThat(parentPlaceholder(normal)).isTrue();
  }

  @Test
  void ordinaryObservedRunInsertsDirectlyWithNullMarker() {
    JobRow owner = newRunOwner();
    RunArgsRow args = DbTestUtils.newRunArgs(jdbi);
    String externalId = UUID.randomUUID().toString().toUpperCase();
    Instant observedAt = Instant.parse("2026-08-14T06:00:00Z");

    RunRow inserted = runDao.upsertOpenLineageRun(observedRun(owner, args, externalId, observedAt));

    assertThat(inserted.getUuid()).isEqualTo(UUID.fromString(externalId));
    assertThat(inserted.getCreatedAt()).isEqualTo(observedAt);
    assertThat(inserted.getRunArgsUuid()).isEqualTo(args.getUuid());
    assertThat(parentPlaceholderIsNull(inserted.getUuid())).isTrue();
    assertThat(runExternalId(inserted.getUuid())).isEqualTo(externalId);
  }

  @Test
  public void findRunByUuidAsExtendedRowReturnsCoreEnrichmentAndUpdatedExternalId() {
    final RunDao runDao = jdbi.onDemand(RunDao.class);

    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        newJobWith(jdbi, namespaceRow.getName(), newJobName().getValue(), jobMeta);

    RunRow row = createRunsForJob(jobRow, 1, jobMeta.getOutputs()).findFirst().orElseThrow();

    runDao.upsert(
        row.getUuid(),
        null,
        row.getUuid().toString(),
        row.getUpdatedAt(),
        jobRow.getUuid(),
        null,
        row.getRunArgsUuid(),
        null,
        null,
        namespaceRow.getName(),
        jobRow.getName(),
        null);

    runDao.upsert(
        row.getUuid(),
        null,
        "updated-external-id",
        row.getUpdatedAt(),
        jobRow.getUuid(),
        null,
        row.getRunArgsUuid(),
        null,
        null,
        namespaceRow.getName(),
        jobRow.getName(),
        null);

    ExtendedRunRow actual = runDao.findRunByUuidAsExtendedRow(row.getUuid()).orElseThrow();
    assertThat(actual.getExternalId()).isEqualTo("updated-external-id");
    assertThat(actual.getArgs()).isNotBlank();
    assertThat(actual.getNamespaceName()).isEqualTo(jobRow.getNamespaceName());
    assertThat(actual.getJobName()).isEqualTo(jobRow.getName());
    assertThat(actual.getCurrentRunState()).contains(RunState.COMPLETED.name());
    assertThat(actual.getJobVersionUuid()).isPresent();
    assertThat(actual.getInputVersions())
        .map(DatasetVersionId::getName)
        .containsExactlyInAnyOrderElementsOf(
            jobMeta.getInputs().stream().map(DatasetId::getName).collect(Collectors.toSet()));
    assertThat(actual.getOutputVersions())
        .map(DatasetVersionId::getName)
        .containsExactlyInAnyOrderElementsOf(
            jobMeta.getOutputs().stream().map(DatasetId::getName).collect(Collectors.toSet()));
  }

  private JobRow newRunOwner() {
    return newJobWith(
        jdbi,
        namespaceRow.getName(),
        newJobName().getValue(),
        newJobMetaWith(NamespaceName.of(namespaceRow.getName())));
  }

  private RunRow syntheticParent(
      JobRow owner, RunArgsRow args, UUID requestedUuid, String externalId) {
    return runDao.getOrCreateSyntheticParentRun(
        requestedUuid,
        externalId,
        owner.getUuid(),
        args.getUuid(),
        owner.getNamespaceName(),
        owner.getName());
  }

  private RunDao.RunUpsert observedRun(
      JobRow owner, RunArgsRow args, String externalId, Instant now) {
    return RunDao.RunUpsert.builder()
        .runUuid(Utils.openLineageRunUuid(externalId))
        .externalId(externalId)
        .now(now)
        .jobUuid(owner.getUuid())
        .runArgsUuid(args.getUuid())
        .nominalStartTime(now.minusSeconds(1))
        .nominalEndTime(now.plusSeconds(1))
        .runStateType(RunState.RUNNING)
        .runStateTime(now)
        .namespaceName(owner.getNamespaceName())
        .jobName(owner.getName())
        .location("observed-location")
        .build();
  }

  private void insertRealRun(
      JobRow owner, RunArgsRow args, UUID runUuid, String externalId, Instant createdAt) {
    runDao.upsert(
        runUuid,
        null,
        externalId,
        createdAt,
        owner.getUuid(),
        null,
        args.getUuid(),
        null,
        null,
        owner.getNamespaceName(),
        owner.getName(),
        "existing-location");
  }

  private boolean parentPlaceholder(UUID runUuid) {
    return jdbi.withHandle(handle -> parentPlaceholder(handle, runUuid));
  }

  private boolean parentPlaceholder(Handle handle, UUID runUuid) {
    return handle
        .createQuery(
            "SELECT open_lineage_parent_placeholder IS TRUE FROM runs WHERE uuid = :runUuid")
        .bind("runUuid", runUuid)
        .mapTo(Boolean.class)
        .one();
  }

  private boolean parentPlaceholderIsNull(UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT open_lineage_parent_placeholder IS NULL FROM runs WHERE uuid = :runUuid")
                .bind("runUuid", runUuid)
                .mapTo(Boolean.class)
                .one());
  }

  private String runExternalId(UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT external_id FROM runs WHERE uuid = :runUuid")
                .bind("runUuid", runUuid)
                .mapTo(String.class)
                .one());
  }

  private int runCount(UUID jobUuid, String externalId) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT count(*) FROM runs WHERE job_uuid = :jobUuid AND external_id = :externalId")
                .bind("jobUuid", jobUuid)
                .bind("externalId", externalId)
                .mapTo(Integer.class)
                .one());
  }

  private Map<String, Object> runIdentityState(UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT created_at, updated_at, external_id, job_uuid, run_args_uuid,
                           nominal_start_time, nominal_end_time, current_run_state,
                           transitioned_at, location, open_lineage_parent_placeholder
                    FROM runs
                    WHERE uuid = :runUuid
                    """)
                .bind("runUuid", runUuid)
                .mapToMap()
                .one());
  }

  private static List<UUID> uuidSequence(int size) {
    return IntStream.range(0, size)
        .mapToObj(index -> new UUID(0L, index + 1L))
        .collect(Collectors.toList());
  }

  private int countRunInputMappings(UUID runUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT count(*) FROM runs_input_mapping WHERE run_uuid = :runUuid")
                .bind("runUuid", runUuid)
                .mapTo(Integer.class)
                .one());
  }
}
