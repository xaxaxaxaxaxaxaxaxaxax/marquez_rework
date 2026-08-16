/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.Generator.newTimestamp;
import static marquez.common.models.CommonModelGenerator.newDescription;
import static marquez.common.models.CommonModelGenerator.newJobName;
import static marquez.common.models.CommonModelGenerator.newJobType;
import static marquez.common.models.CommonModelGenerator.newLocation;
import static marquez.common.models.CommonModelGenerator.newVersion;
import static marquez.db.JobVersionDao.BagOfJobVersionInfo;
import static marquez.db.JobVersionDao.IoType.INPUT;
import static marquez.db.JobVersionDao.IoType.OUTPUT;
import static marquez.db.models.DbModelGenerator.newRowUuid;
import static marquez.service.models.ServiceModelGenerator.newInputsWith;
import static marquez.service.models.ServiceModelGenerator.newJobMetaWith;
import static marquez.service.models.ServiceModelGenerator.newOutputsWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import marquez.BaseIntegrationTest;
import marquez.api.models.JobVersion;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunState;
import marquez.common.models.Version;
import marquez.db.models.DatasetRow;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.ExtendedJobVersionRow;
import marquez.db.models.JobRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.ProjectionOrder;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunIoSnapshot;
import marquez.db.models.RunRow;
import marquez.db.models.UpdateLineageRow.DatasetRecord;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobMeta;
import marquez.service.models.LineageEvent;
import marquez.service.models.Run;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/** The test suite for {@link JobVersionDao}. */
@org.junit.jupiter.api.Tag("IntegrationTests")
@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
public class JobVersionDaoTest extends BaseIntegrationTest {
  static Jdbi jdbiForTesting;
  static DatasetVersionDao datasetVersionDao;
  static DatasetDao datasetDao;
  static JobDao jobDao;
  static RunDao runDao;
  static OpenLineageDao openLineageDao;
  static JobVersionDao jobVersionDao;
  static NamespaceRow namespaceRow;
  static JobRow jobRow;

  @BeforeAll
  public static void setUpOnce(final Jdbi jdbi) {
    jdbiForTesting = jdbi;
    datasetDao = jdbiForTesting.onDemand(DatasetDao.class);
    datasetVersionDao = jdbiForTesting.onDemand(DatasetVersionDao.class);
    jobDao = jdbi.onDemand(JobDao.class);
    runDao = jdbi.onDemand(RunDao.class);
    openLineageDao = jdbi.onDemand(OpenLineageDao.class);
    jobVersionDao = jdbiForTesting.onDemand(JobVersionDao.class);

    // Each tests requires both a namespace and job row.
    namespaceRow = DbTestUtils.newNamespace(jdbiForTesting);
    jobRow = DbTestUtils.newJob(jdbiForTesting, namespaceRow.getName(), newJobName().getValue());
  }

  @Test
  void jobGatePrecedesRunMutationForConcurrentTransition() throws Exception {
    JobRow gatedJob =
        DbTestUtils.newJob(jdbiForTesting, namespaceRow.getName(), newJobName().getValue());
    RunRow gatedRun = DbTestUtils.newRun(jdbiForTesting, gatedJob);
    JobVersionDao.JobRowRunDetails details =
        jobVersionDao.loadJobRowRunDetails(gatedJob, gatedRun.getUuid());
    String applicationName = "job-version-gate-" + UUID.randomUUID();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);

    try (Handle first = jdbiForTesting.open()) {
      first.begin();
      first.attach(JobDao.class).lockJobBeforeRunMutation(gatedJob.getUuid());
      Future<BagOfJobVersionInfo> transition =
          executor.submit(
              () -> {
                try (Handle second = jdbiForTesting.open()) {
                  second.begin();
                  second
                      .createQuery("SELECT set_config('application_name', :name, true)")
                      .bind("name", applicationName)
                      .mapTo(String.class)
                      .one();
                  started.countDown();
                  BagOfJobVersionInfo result =
                      second
                          .attach(JobVersionDao.class)
                          .upsertJobVersionOnRunTransitionInTransaction(
                              details, RunState.COMPLETED, Instant.now(), true);
                  second.commit();
                  return result;
                }
              });

      assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
      awaitLockWait(applicationName);
      assertThat(
              first
                  .createQuery("SELECT uuid FROM runs WHERE uuid = :runUuid FOR UPDATE NOWAIT")
                  .bind("runUuid", gatedRun.getUuid())
                  .mapTo(UUID.class)
                  .one())
          .isEqualTo(gatedRun.getUuid());
      first.commit();

      BagOfJobVersionInfo result = transition.get(20, TimeUnit.SECONDS);
      assertThat(runDao.findRunByUuidAsRow(gatedRun.getUuid()).orElseThrow().getJobVersionUuid())
          .contains(result.getJobVersionRow().getUuid());
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static void awaitLockWait(String applicationName) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    boolean waiting;
    do {
      waiting =
          jdbiForTesting.withHandle(
              handle ->
                  handle
                      .createQuery(
                          """
                          SELECT EXISTS (
                            SELECT 1
                            FROM pg_stat_activity
                            WHERE application_name = :applicationName
                              AND wait_event_type = 'Lock')
                          """)
                      .bind("applicationName", applicationName)
                      .mapTo(Boolean.class)
                      .one());
    } while (!waiting && System.nanoTime() < deadline);
    assertThat(waiting).as("transition should wait on the canonical job gate").isTrue();
  }

  private enum IoMutation {
    FLUSH,
    REPLACE
  }

  private enum IoShape {
    EMPTY,
    INPUT_ONLY,
    OUTPUT_ONLY,
    BOTH
  }

  private static List<UUID> uuidRange(long first, long last) {
    return java.util.stream.LongStream.rangeClosed(first, last)
        .mapToObj(value -> new UUID(0L, value))
        .toList();
  }

  @ParameterizedTest(name = "{0} / {1}")
  @CsvSource({
    "FLUSH, EMPTY",
    "FLUSH, INPUT_ONLY",
    "FLUSH, OUTPUT_ONLY",
    "FLUSH, BOTH",
    "REPLACE, EMPTY",
    "REPLACE, INPUT_ONLY",
    "REPLACE, OUTPUT_ONLY",
    "REPLACE, BOTH"
  })
  public void testCurrentJobVersionIoDispatchMatrix(IoMutation mutation, IoShape shape) {
    JobVersionDao batchingDao = mock(JobVersionDao.class, CALLS_REAL_METHODS);
    UUID jobVersionUuid = UUID.randomUUID();
    UUID jobUuid = UUID.randomUUID();
    UUID symlinkTargetJobUuid = UUID.randomUUID();
    UUID inputUuid = new UUID(0L, 1L);
    UUID outputUuid = new UUID(0L, 2L);
    boolean hasInputs = shape == IoShape.INPUT_ONLY || shape == IoShape.BOTH;
    boolean hasOutputs = shape == IoShape.OUTPUT_ONLY || shape == IoShape.BOTH;
    JobVersionDao.CurrentJobVersionIoWrite write =
        JobVersionDao.CurrentJobVersionIoWrite.of(
            jobVersionUuid,
            jobUuid,
            symlinkTargetJobUuid,
            hasInputs ? List.of(inputUuid) : List.of(),
            hasOutputs ? List.of(outputUuid) : List.of());

    if (mutation == IoMutation.FLUSH) {
      batchingDao.flushCurrentJobVersionIoInCurrentTransaction(write);
    } else {
      batchingDao.replaceOpenLineageCurrentJobVersionIoInCurrentTransaction(write);
    }
    if (mutation == IoMutation.FLUSH && shape == IoShape.EMPTY) {
      batchingDao.upsertInputDatasetsFor(jobVersionUuid, List.of(), jobUuid, symlinkTargetJobUuid);
      batchingDao.upsertOutputDatasetsFor(jobVersionUuid, List.of(), jobUuid, symlinkTargetJobUuid);
    }

    InOrder calls = inOrder(batchingDao);
    if (mutation == IoMutation.FLUSH) {
      calls.verify(batchingDao).flushCurrentJobVersionIoInCurrentTransaction(write);
    } else {
      calls.verify(batchingDao).replaceOpenLineageCurrentJobVersionIoInCurrentTransaction(write);
    }
    if (mutation == IoMutation.REPLACE) {
      calls.verify(batchingDao).markInputAndOutputDatasetsAsPreviousFor(jobUuid);
    } else if (shape == IoShape.BOTH) {
      calls.verify(batchingDao).markInputAndOutputDatasetsAsPreviousFor(jobVersionUuid, jobUuid);
    } else if (shape != IoShape.EMPTY) {
      calls
          .verify(batchingDao)
          .markInputOrOutputDatasetAsPreviousFor(
              jobVersionUuid, jobUuid, shape == IoShape.INPUT_ONLY ? INPUT : OUTPUT);
    }
    ArgumentCaptor<UUID[]> datasetUuids = ArgumentCaptor.forClass(UUID[].class);
    if (shape == IoShape.BOTH) {
      ArgumentCaptor<UUID[]> outputUuids = ArgumentCaptor.forClass(UUID[].class);
      calls
          .verify(batchingDao)
          .upsertCurrentInputAndOutputDatasetsChunk(
              eq(jobVersionUuid),
              datasetUuids.capture(),
              outputUuids.capture(),
              eq(jobUuid),
              eq(symlinkTargetJobUuid));
      assertThat(datasetUuids.getValue()).containsExactly(inputUuid);
      assertThat(outputUuids.getValue()).containsExactly(outputUuid);
    } else if (shape != IoShape.EMPTY) {
      calls
          .verify(batchingDao)
          .upsertCurrentInputOrOutputDatasetsChunk(
              eq(jobVersionUuid),
              datasetUuids.capture(),
              eq(jobUuid),
              eq(symlinkTargetJobUuid),
              eq(shape == IoShape.INPUT_ONLY ? INPUT : OUTPUT));
      assertThat(datasetUuids.getValue()).containsExactly(hasInputs ? inputUuid : outputUuid);
    }
    if (mutation == IoMutation.FLUSH && shape == IoShape.EMPTY) {
      calls
          .verify(batchingDao)
          .upsertInputDatasetsFor(jobVersionUuid, List.of(), jobUuid, symlinkTargetJobUuid);
      calls.verify(batchingDao).flushCurrentJobVersionIoInCurrentTransaction(write);
      calls
          .verify(batchingDao)
          .upsertOutputDatasetsFor(jobVersionUuid, List.of(), jobUuid, symlinkTargetJobUuid);
      calls.verify(batchingDao).flushCurrentJobVersionIoInCurrentTransaction(write);
    }
    calls.verifyNoMoreInteractions();
  }

  @Test
  public void testIoChunksUsePostgresqlUnsignedUuidOrderAcrossBatchBoundary() {
    JobVersionDao batchingDao = mock(JobVersionDao.class, CALLS_REAL_METHODS);
    UUID jobVersionUuid = UUID.randomUUID();
    UUID jobUuid = UUID.randomUUID();
    List<UUID> inputUuids = new ArrayList<>();
    UUID unsignedHighMsb = new UUID(Long.MIN_VALUE, 0L);
    UUID unsignedHighLsb = new UUID(0L, Long.MIN_VALUE);
    inputUuids.add(unsignedHighMsb);
    inputUuids.add(unsignedHighLsb);
    for (long value = JobVersionDao.JOB_VERSION_IO_BATCH_SIZE; value >= 0; value--) {
      inputUuids.add(new UUID(0L, value));
    }
    inputUuids.add(new UUID(0L, 0L));

    batchingDao.flushCurrentJobVersionIoInCurrentTransaction(
        JobVersionDao.CurrentJobVersionIoWrite.of(
            jobVersionUuid, jobUuid, null, inputUuids, List.of()));

    verify(batchingDao).markInputOrOutputDatasetAsPreviousFor(jobVersionUuid, jobUuid, INPUT);
    ArgumentCaptor<UUID[]> chunks = ArgumentCaptor.forClass(UUID[].class);
    verify(batchingDao, times(2))
        .upsertCurrentInputOrOutputDatasetsChunk(
            eq(jobVersionUuid), chunks.capture(), eq(jobUuid), eq(null), eq(INPUT));
    assertThat(chunks.getAllValues().get(0))
        .containsExactlyElementsOf(uuidRange(0, JobVersionDao.JOB_VERSION_IO_BATCH_SIZE - 1L));
    assertThat(chunks.getAllValues().get(1))
        .containsExactly(
            new UUID(0L, JobVersionDao.JOB_VERSION_IO_BATCH_SIZE),
            unsignedHighLsb,
            unsignedHighMsb);
  }

  @Test
  public void testBothSidedIoWriteUsesTotalPairBatchLimitAndInputBeforeOutputOrder() {
    JobVersionDao batchingDao = mock(JobVersionDao.class, CALLS_REAL_METHODS);
    UUID jobVersionUuid = UUID.randomUUID();
    UUID jobUuid = UUID.randomUUID();
    List<UUID> descendingInputUuids = new ArrayList<>();
    List<UUID> descendingOutputUuids = new ArrayList<>();
    for (long value = 750; value > 0; value--) {
      descendingInputUuids.add(new UUID(0L, value));
    }
    for (long value = 1750; value > 1000; value--) {
      descendingOutputUuids.add(new UUID(0L, value));
    }

    batchingDao.flushCurrentJobVersionIoInCurrentTransaction(
        JobVersionDao.CurrentJobVersionIoWrite.of(
            jobVersionUuid, jobUuid, null, descendingInputUuids, descendingOutputUuids));

    verify(batchingDao).markInputAndOutputDatasetsAsPreviousFor(jobVersionUuid, jobUuid);
    ArgumentCaptor<UUID[]> inputChunks = ArgumentCaptor.forClass(UUID[].class);
    ArgumentCaptor<UUID[]> outputChunks = ArgumentCaptor.forClass(UUID[].class);
    verify(batchingDao, times(2))
        .upsertCurrentInputAndOutputDatasetsChunk(
            eq(jobVersionUuid),
            inputChunks.capture(),
            outputChunks.capture(),
            eq(jobUuid),
            eq(null));

    assertThat(inputChunks.getAllValues().get(0)).containsExactlyElementsOf(uuidRange(1, 750));
    assertThat(outputChunks.getAllValues().get(0)).containsExactlyElementsOf(uuidRange(1001, 1250));
    assertThat(inputChunks.getAllValues().get(1)).isEmpty();
    assertThat(outputChunks.getAllValues().get(1)).containsExactlyElementsOf(uuidRange(1251, 1750));
    for (int index = 0; index < inputChunks.getAllValues().size(); index++) {
      assertThat(
              inputChunks.getAllValues().get(index).length
                  + outputChunks.getAllValues().get(index).length)
          .isLessThanOrEqualTo(JobVersionDao.JOB_VERSION_IO_BATCH_SIZE);
    }
  }

  @Test
  public void testCombinedIoWriteInvalidatesBothSidesAndPreservesMadeCurrentAt() {
    JobRow combinedJob =
        DbTestUtils.newJob(jdbiForTesting, namespaceRow.getName(), newJobName().getValue());
    String inputName = "combined-input-" + UUID.randomUUID();
    String outputName = "combined-output-" + UUID.randomUUID();
    DbTestUtils.newDataset(jdbiForTesting, namespaceRow.getName(), inputName);
    DbTestUtils.newDataset(jdbiForTesting, namespaceRow.getName(), outputName);
    UUID inputUuid =
        datasetDao.findDatasetAsRow(namespaceRow.getName(), inputName).orElseThrow().getUuid();
    UUID outputUuid =
        datasetDao.findDatasetAsRow(namespaceRow.getName(), outputName).orElseThrow().getUuid();
    ExtendedJobVersionRow oldVersion =
        DbTestUtils.newJobVersion(
            jdbiForTesting,
            combinedJob.getUuid(),
            UUID.randomUUID(),
            combinedJob.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());
    ExtendedJobVersionRow newVersion =
        DbTestUtils.newJobVersion(
            jdbiForTesting,
            combinedJob.getUuid(),
            UUID.randomUUID(),
            combinedJob.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());

    flushCombinedIo(oldVersion, combinedJob, inputUuid, outputUuid);
    flushCombinedIo(newVersion, combinedJob, inputUuid, outputUuid);

    assertThat(countMappings(oldVersion.getUuid(), INPUT, false)).isEqualTo(1);
    assertThat(countMappings(oldVersion.getUuid(), OUTPUT, false)).isEqualTo(1);
    assertThat(countMappings(newVersion.getUuid(), INPUT, true)).isEqualTo(1);
    assertThat(countMappings(newVersion.getUuid(), OUTPUT, true)).isEqualTo(1);

    Instant sentinel = Instant.parse("2000-01-01T00:00:00Z");
    jdbiForTesting.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    UPDATE job_versions_io_mapping
                    SET made_current_at = :sentinel,
                        is_current_job_version = FALSE
                    WHERE job_version_uuid = :jobVersionUuid
                    """)
                .bind("sentinel", sentinel)
                .bind("jobVersionUuid", newVersion.getUuid())
                .execute());

    flushCombinedIo(newVersion, combinedJob, inputUuid, outputUuid);

    int currentMappingsWithSentinel =
        jdbiForTesting.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT count(*)
                        FROM job_versions_io_mapping
                        WHERE job_version_uuid = :jobVersionUuid
                          AND io_type IN ('INPUT', 'OUTPUT')
                          AND is_current_job_version = TRUE
                          AND made_current_at = :sentinel
                        """)
                    .bind("jobVersionUuid", newVersion.getUuid())
                    .bind("sentinel", sentinel)
                    .mapTo(Integer.class)
                    .one());
    assertThat(currentMappingsWithSentinel).isEqualTo(2);

    jobVersionDao.markInputAndOutputDatasetsAsPreviousFor(combinedJob.getUuid());
    assertThat(countMappings(newVersion.getUuid(), INPUT, false)).isEqualTo(1);
    assertThat(countMappings(newVersion.getUuid(), OUTPUT, false)).isEqualTo(1);
  }

  @Test
  public void testPluralDatasetUpsertRollsBackInvalidationWhenArrayInsertFails() {
    final JobMeta jobMeta =
        new JobMeta(
            newJobType(),
            newInputsWith(NamespaceName.of(namespaceRow.getName()), 1),
            newOutputsWith(NamespaceName.of(namespaceRow.getName()), 0),
            newLocation(),
            newDescription(),
            null,
            null);
    final JobRow transactionJob =
        DbTestUtils.newJobWith(
            jdbiForTesting, namespaceRow.getName(), newJobName().getValue(), jobMeta);
    DatasetId inputDatasetId = jobMeta.getInputs().stream().findFirst().orElseThrow();
    UUID inputDatasetUuid =
        datasetDao
            .getUuid(inputDatasetId.getNamespace().getValue(), inputDatasetId.getName().getValue())
            .orElseThrow()
            .getUuid();

    ExtendedJobVersionRow oldVersion =
        DbTestUtils.newJobVersion(
            jdbiForTesting,
            transactionJob.getUuid(),
            UUID.randomUUID(),
            transactionJob.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());
    jobVersionDao.upsertInputDatasetsFor(
        oldVersion.getUuid(),
        List.of(inputDatasetUuid),
        transactionJob.getUuid(),
        transactionJob.getSymlinkTargetId());

    ExtendedJobVersionRow newVersion =
        DbTestUtils.newJobVersion(
            jdbiForTesting,
            transactionJob.getUuid(),
            UUID.randomUUID(),
            transactionJob.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());

    assertThatThrownBy(
            () ->
                jobVersionDao.upsertInputDatasetsFor(
                    newVersion.getUuid(),
                    List.of(inputDatasetUuid, UUID.randomUUID()),
                    transactionJob.getUuid(),
                    transactionJob.getSymlinkTargetId()))
        .isInstanceOf(RuntimeException.class);

    assertThat(countMappings(oldVersion.getUuid(), INPUT, true)).isEqualTo(1);
    assertThat(countMappings(newVersion.getUuid(), INPUT, true)).isZero();
  }

  @Test
  public void testSetBasedReactivationPreservesMadeCurrentAt() {
    JobRow timestampJob =
        DbTestUtils.newJob(jdbiForTesting, namespaceRow.getName(), newJobName().getValue());
    String datasetName = "made-current-" + UUID.randomUUID();
    DbTestUtils.newDataset(jdbiForTesting, namespaceRow.getName(), datasetName);
    UUID datasetUuid =
        datasetDao.findDatasetAsRow(namespaceRow.getName(), datasetName).orElseThrow().getUuid();
    ExtendedJobVersionRow firstVersion =
        DbTestUtils.newJobVersion(
            jdbiForTesting,
            timestampJob.getUuid(),
            UUID.randomUUID(),
            timestampJob.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());
    ExtendedJobVersionRow secondVersion =
        DbTestUtils.newJobVersion(
            jdbiForTesting,
            timestampJob.getUuid(),
            UUID.randomUUID(),
            timestampJob.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());
    jobVersionDao.upsertInputDatasetsFor(
        firstVersion.getUuid(), List.of(datasetUuid), timestampJob.getUuid(), null);
    Instant sentinel = Instant.parse("2000-01-01T00:00:00Z");
    jdbiForTesting.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    UPDATE job_versions_io_mapping
                    SET made_current_at = :sentinel
                    WHERE job_version_uuid = :jobVersionUuid
                      AND dataset_uuid = :datasetUuid
                      AND io_type = 'INPUT'
                    """)
                .bind("sentinel", sentinel)
                .bind("jobVersionUuid", firstVersion.getUuid())
                .bind("datasetUuid", datasetUuid)
                .execute());

    jobVersionDao.upsertInputDatasetsFor(
        secondVersion.getUuid(), List.of(datasetUuid), timestampJob.getUuid(), null);
    jobVersionDao.upsertInputDatasetsFor(
        firstVersion.getUuid(), List.of(datasetUuid), timestampJob.getUuid(), null);

    boolean timestampPreserved =
        jdbiForTesting.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT made_current_at = :sentinel
                        FROM job_versions_io_mapping
                        WHERE job_version_uuid = :jobVersionUuid
                          AND dataset_uuid = :datasetUuid
                          AND io_type = 'INPUT'
                        """)
                    .bind("sentinel", sentinel)
                    .bind("jobVersionUuid", firstVersion.getUuid())
                    .bind("datasetUuid", datasetUuid)
                    .mapTo(Boolean.class)
                    .one());
    assertThat(timestampPreserved).isTrue();
    assertThat(countMappings(firstVersion.getUuid(), INPUT, true)).isEqualTo(1);
    assertThat(countMappings(secondVersion.getUuid(), INPUT, false)).isEqualTo(1);
  }

  @Test
  public void testTargetJobVersionMarksSymlinkMappingsPrevious() {
    final JobRow targetJob =
        DbTestUtils.createJobWithoutSymlinkTarget(
            jdbiForTesting, namespaceRow, newJobName().getValue(), "the target of the symlink");
    final JobMeta symlinkJobMeta =
        new JobMeta(
            newJobType(),
            newInputsWith(NamespaceName.of(namespaceRow.getName()), 1),
            newOutputsWith(NamespaceName.of(namespaceRow.getName()), 0),
            newLocation(),
            newDescription(),
            null,
            null);
    String symlinkJobName = newJobName().getValue();
    DbTestUtils.newJobWith(
        jdbiForTesting,
        namespaceRow.getName(),
        symlinkJobName,
        targetJob.getUuid(),
        symlinkJobMeta);
    UUID symlinkJobUuid =
        jdbiForTesting.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT uuid
                        FROM jobs
                        WHERE namespace_name = :namespaceName AND name = :jobName
                        """)
                    .bind("namespaceName", namespaceRow.getName())
                    .bind("jobName", symlinkJobName)
                    .mapTo(UUID.class)
                    .one());
    DatasetId inputDatasetId = symlinkJobMeta.getInputs().stream().findFirst().orElseThrow();
    UUID inputDatasetUuid =
        datasetDao
            .getUuid(inputDatasetId.getNamespace().getValue(), inputDatasetId.getName().getValue())
            .orElseThrow()
            .getUuid();

    ExtendedJobVersionRow symlinkVersion =
        DbTestUtils.newJobVersion(
            jdbiForTesting,
            symlinkJobUuid,
            UUID.randomUUID(),
            symlinkJobName,
            namespaceRow.getUuid(),
            namespaceRow.getName());
    jobVersionDao.upsertInputDatasetsFor(
        symlinkVersion.getUuid(), List.of(inputDatasetUuid), symlinkJobUuid, targetJob.getUuid());

    ExtendedJobVersionRow targetVersion =
        DbTestUtils.newJobVersion(
            jdbiForTesting,
            targetJob.getUuid(),
            UUID.randomUUID(),
            targetJob.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());
    jobVersionDao.upsertInputDatasetsFor(
        targetVersion.getUuid(),
        List.of(inputDatasetUuid),
        targetJob.getUuid(),
        targetJob.getSymlinkTargetId());

    assertThat(countMappings(symlinkVersion.getUuid(), INPUT, false)).isEqualTo(1);
    assertThat(countMappings(targetVersion.getUuid(), INPUT, true)).isEqualTo(1);
  }

  @Test
  public void testUpsertJobVersion() {
    // Use a randomly generated job version. We'll attempt to associate multiple job versions with
    // the same version; only the first attempt will insert the job version row successfully.
    final Version version = newVersion();

    // (1) Add a new job version; no conflict on version.
    final int rowsBefore = jobVersionDao.count();
    jobVersionDao.upsertJobVersion(
        newRowUuid(),
        newTimestamp(),
        jobRow.getUuid(),
        newLocation().toString(),
        version.getValue(),
        jobRow.getName(),
        namespaceRow.getUuid(),
        namespaceRow.getName());

    final int rowsAfter = jobVersionDao.count();
    assertThat(rowsAfter).isEqualTo(rowsBefore + 1);

    // (2) Add another job version; conflict on version, not inserted.
    final int rowsBeforeConflict = jobVersionDao.count();
    jobVersionDao.upsertJobVersion(
        newRowUuid(),
        newTimestamp(),
        jobRow.getUuid(),
        newLocation().toString(),
        version.getValue(),
        jobRow.getName(),
        namespaceRow.getUuid(),
        namespaceRow.getName());

    final int rowsAfterConflict = jobVersionDao.count();
    assertThat(rowsAfterConflict).isEqualTo(rowsBeforeConflict);
    Optional<JobVersion> jobVersion =
        jobVersionDao.findJobVersion(
            jobRow.getNamespaceName(), jobRow.getName(), version.getValue());
    assertThat(jobVersion).isPresent();
  }

  @Test
  public void testUpdateLatestRunFor() {
    // (1) Add a new job version.
    final ExtendedJobVersionRow jobVersionRow =
        jobVersionDao.upsertJobVersion(
            newRowUuid(),
            newTimestamp(),
            jobRow.getUuid(),
            newLocation().toString(),
            newVersion().getValue(),
            jobRow.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());
    assertThat(jobVersionRow.getLatestRunUuid()).isNotPresent();

    // (2) Add a new run.
    final RunArgsRow runArgsRow = DbTestUtils.newRunArgs(jdbiForTesting);
    final RunRow runRow =
        DbTestUtils.newRun(
            jdbiForTesting,
            jobVersionRow.getJobUuid(),
            jobVersionRow.getUuid(),
            runArgsRow.getUuid(),
            namespaceRow.getUuid(),
            namespaceRow.getName(),
            jobVersionRow.getJobName(),
            jobVersionRow.getLocation().orElse(null));

    // Ensure the latest run is not associated with the job version.
    final Optional<UUID> noLatestRunUuid = jobVersionDao.findLatestRunFor(jobVersionRow.getUuid());
    assertThat(noLatestRunUuid).isNotPresent();

    // (3) Link latest run with the job version.
    jobVersionDao.updateLatestRunFor(jobVersionRow.getUuid(), newTimestamp(), runRow.getUuid());

    // Ensure the latest run is associated with the job version.
    final Optional<UUID> latestRunUuid = jobVersionDao.findLatestRunFor(jobVersionRow.getUuid());
    assertThat(latestRunUuid).isPresent().contains(runRow.getUuid());
  }

  @Test
  void legacyLatestRunWriteClearsWatermarkWithoutLoweringUpdatedAt() {
    Instant highWater = Instant.parse("2030-08-13T01:00:30Z");
    Instant olderLegacyTime = highWater.minusSeconds(20);
    ExtendedJobVersionRow version =
        jobVersionDao.upsertJobVersion(
            newRowUuid(),
            olderLegacyTime.minusSeconds(10),
            jobRow.getUuid(),
            "legacy-location",
            newVersion().getValue(),
            jobRow.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());
    RunRow orderedRun = DbTestUtils.newRun(jdbiForTesting, jobRow);
    RunRow legacyRun = DbTestUtils.newRun(jdbiForTesting, jobRow);

    assertThat(
            jobVersionDao.updateLatestRunFor(
                version.getUuid(),
                orderedRun.getUuid(),
                new ProjectionOrder(highWater, marquez.common.Utils.sha256Utf8("ordered latest"))))
        .isTrue();
    jobVersionDao.updateLatestRunFor(version.getUuid(), olderLegacyTime, legacyRun.getUuid());

    jdbiForTesting.useHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT updated_at, latest_run_uuid,
                           open_lineage_latest_run_time, open_lineage_latest_run_key
                    FROM job_versions WHERE uuid = :versionUuid
                    """)
                .bind("versionUuid", version.getUuid())
                .map(
                    (resultSet, context) -> {
                      assertThat(resultSet.getTimestamp("updated_at").toInstant())
                          .isEqualTo(highWater);
                      assertThat(resultSet.getObject("latest_run_uuid", UUID.class))
                          .isEqualTo(legacyRun.getUuid());
                      assertThat(resultSet.getTimestamp("open_lineage_latest_run_time")).isNull();
                      assertThat(resultSet.getBytes("open_lineage_latest_run_key")).isNull();
                      return true;
                    })
                .one());
  }

  @Test
  public void testGetJobVersion() {
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        DbTestUtils.newJobWith(
            jdbiForTesting, namespaceRow.getName(), newJobName().getValue(), jobMeta);
    Version version = newVersion();
    final ExtendedJobVersionRow jobVersionRow =
        jobVersionDao.upsertJobVersion(
            newRowUuid(),
            newTimestamp(),
            jobRow.getUuid(),
            newLocation().toString(),
            version.getValue(),
            jobRow.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());
    DatasetDao datasetDao = jdbiForTesting.onDemand(DatasetDao.class);
    for (DatasetId ds : jobMeta.getInputs()) {
      DatasetRow dataset =
          datasetDao
              .findDatasetAsRow(ds.getNamespace().getValue(), ds.getName().getValue())
              .orElseThrow(
                  () -> new IllegalStateException("Can't find test dataset " + ds.getName()));

      jobVersionDao.upsertInputDatasetFor(
          jobVersionRow.getUuid(),
          dataset.getUuid(),
          jobVersionRow.getJobUuid(),
          jobRow.getSymlinkTargetId());
    }
    for (DatasetId ds : jobMeta.getOutputs()) {
      DatasetRow dataset =
          datasetDao
              .findDatasetAsRow(ds.getNamespace().getValue(), ds.getName().getValue())
              .orElseThrow(
                  () -> new IllegalStateException("Can't find test dataset " + ds.getName()));

      jobVersionDao.upsertOutputDatasetFor(
          jobVersionRow.getUuid(),
          dataset.getUuid(),
          jobVersionRow.getJobUuid(),
          jobRow.getSymlinkTargetId());
    }
    Optional<JobVersion> jobVersion =
        jobVersionDao.findJobVersion(namespaceRow.getName(), jobRow.getName(), version.getValue());
    assertThat(jobVersion)
        .isPresent()
        .get()
        .extracting(JobVersion::getInputs, InstanceOfAssertFactories.list(DatasetId.class))
        .containsAll(jobMeta.getInputs());
    assertThat(jobVersion)
        .get()
        .extracting(JobVersion::getOutputs, InstanceOfAssertFactories.list(DatasetId.class))
        .containsAll(jobMeta.getOutputs());
    assertThat(jobVersion).get().extracting(JobVersion::getLatestRun).isNull();
  }

  @Test
  public void testGetJobVersions() {
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow jobRow =
        DbTestUtils.newJobWith(
            jdbiForTesting, namespaceRow.getName(), newJobName().getValue(), jobMeta);

    final RunRow runRow = DbTestUtils.newRun(jdbiForTesting, jobRow);
    final Run runCompleted =
        DbTestUtils.transitionRunWithOutputs(
            jdbiForTesting, runRow.getUuid(), RunState.COMPLETED, jobMeta.getOutputs());

    jobVersionDao.upsertJobVersionOnRunTransition(
        jobVersionDao.loadJobRowRunDetails(jobRow, runRow.getUuid()),
        RunState.COMPLETED,
        Instant.now(),
        true);

    List<JobVersion> jobVersions =
        jobVersionDao.findAllJobVersions(namespaceRow.getName(), jobRow.getName(), 10, 0);
    assertThat(jobVersions)
        .hasSize(1)
        .first()
        .extracting(JobVersion::getInputs, InstanceOfAssertFactories.list(DatasetId.class))
        .containsAll(jobMeta.getInputs());

    assertThat(jobVersions)
        .hasSize(1)
        .first()
        .extracting(JobVersion::getLatestRun)
        .isNotNull()
        .extracting(Run::getId)
        .isEqualTo(runCompleted.getId());
  }

  @Test
  public void testUpsertJobVersionOnRunTransition() {
    // Generate a new job meta object with an existing namespace; the namespace will also be
    // associated with the input and output datasets for the job.
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    // (1) Add a new job; the input and output datasets for the job will also be added.
    final JobRow jobRow =
        DbTestUtils.newJobWith(
            jdbiForTesting, namespaceRow.getName(), newJobName().getValue(), jobMeta);

    // (2) Add a new run; the input dataset versions will also be associated with the run.
    final RunRow runRow = DbTestUtils.newRun(jdbiForTesting, jobRow);

    // Ensure the input dataset versions have been associated with the run.
    final List<ExtendedDatasetVersionRow> inputDatasetVersions =
        datasetVersionDao.findInputDatasetVersionsFor(runRow.getUuid());
    assertThat(inputDatasetVersions).hasSize(jobMeta.getInputs().size());

    // Ensure a run with the state NEW has no output dataset versions.
    final List<ExtendedDatasetVersionRow> noOutputDatasetVersions =
        datasetVersionDao.findOutputDatasetVersionsFor(runRow.getUuid());
    assertThat(noOutputDatasetVersions).isEmpty();

    // (4) Transition the run from NEW to RUNNING.
    final Run runStarted =
        DbTestUtils.transitionRunTo(jdbiForTesting, runRow.getUuid(), RunState.RUNNING);
    assertThat(runStarted.getState()).isEqualTo(RunState.RUNNING);
    assertThat(runStarted.getStartedAt()).isNotNull();

    // (5) Transition the run from RUNNING to COMPLETED.
    final Run runCompleted =
        DbTestUtils.transitionRunWithOutputs(
            jdbiForTesting, runRow.getUuid(), RunState.COMPLETED, jobMeta.getOutputs());
    assertThat(runCompleted.getState()).isEqualTo(RunState.COMPLETED);
    assertThat(runCompleted.getEndedAt()).isNotNull();
    assertThat(runCompleted.getDurationMs()).isPresent();

    // Ensure the output dataset versions have been associated with the run.
    final List<ExtendedDatasetVersionRow> outputDatasetVersions =
        datasetVersionDao.findOutputDatasetVersionsFor(runRow.getUuid());
    assertThat(outputDatasetVersions).hasSize(jobMeta.getOutputs().size());

    // Ensure the latest run not associated with a job version.
    final Optional<ExtendedJobVersionRow> jobVersionRow =
        jobVersionDao.findJobVersionFor(runRow.getUuid());
    assertThat(jobVersionRow).isNotPresent();

    // (6) Add a new job version on the run state transition to COMPLETED.
    final BagOfJobVersionInfo bagOfJobVersionInfo =
        jobVersionDao.upsertJobVersionOnRunTransition(
            jobVersionDao.loadJobRowRunDetails(jobRow, runRow.getUuid()),
            RunState.COMPLETED,
            newTimestamp(),
            true);

    // Ensure the job version is associated with the latest run.
    final RunRow latestRunRowForJobVersion = runDao.findRunByUuidAsRow(runRow.getUuid()).get();
    assertThat(latestRunRowForJobVersion.getJobVersionUuid())
        .isPresent()
        .contains(bagOfJobVersionInfo.getJobVersionRow().getUuid());

    // Ensure the latest run is associated with the job version.
    final Optional<UUID> latestRunUuid =
        jobVersionDao.findLatestRunFor(bagOfJobVersionInfo.getJobVersionRow().getUuid());
    assertThat(latestRunUuid).isPresent().contains(runRow.getUuid());

    // Ensure the latest version is associated with the job.
    final JobRow jobRowForLatestRun =
        jobDao.findJobByNameAsRow(jobRow.getNamespaceName(), jobRow.getName()).get();
    assertThat(jobRowForLatestRun.getCurrentVersionUuid())
        .isPresent()
        .contains(bagOfJobVersionInfo.getJobVersionRow().getUuid());

    // Ensure the input datasets have been linked to the job version.
    final List<UUID> jobVersionInputDatasetUuids =
        jobVersionDao.findInputDatasetsFor(bagOfJobVersionInfo.getJobVersionRow().getUuid());
    assertThat(jobVersionInputDatasetUuids).hasSize(bagOfJobVersionInfo.getInputs().size());
    for (final ExtendedDatasetVersionRow jobVersionInputDatasetUuid :
        bagOfJobVersionInfo.getInputs()) {
      assertThat(jobVersionInputDatasetUuids).contains(jobVersionInputDatasetUuid.getDatasetUuid());
    }

    // Ensure the output datasets have been linked to the job version.
    final List<UUID> jobVersionOutputDatasetUuids =
        jobVersionDao.findOutputDatasetsFor(bagOfJobVersionInfo.getJobVersionRow().getUuid());
    assertThat(jobVersionOutputDatasetUuids).hasSize(bagOfJobVersionInfo.getOutputs().size());
    for (final ExtendedDatasetVersionRow outputDatasetVersion : bagOfJobVersionInfo.getOutputs()) {
      assertThat(jobVersionOutputDatasetUuids).contains(outputDatasetVersion.getDatasetUuid());
    }
    Optional<JobVersion> jobVersion =
        jobVersionDao.findJobVersion(
            jobRow.getNamespaceName(),
            jobRow.getName(),
            bagOfJobVersionInfo.getJobVersionRow().getVersion());
    assertThat(jobVersion)
        .isPresent()
        .get()
        .extracting(JobVersion::getInputs, InstanceOfAssertFactories.list(UUID.class))
        .isNotEmpty();
    assertThat(jobVersion)
        .isPresent()
        .get()
        .extracting(JobVersion::getOutputs, InstanceOfAssertFactories.list(DatasetId.class))
        .isNotEmpty();
  }

  @Test
  public void testRunIoSnapshotMatchesLegacyQueriesAndPreservesDirection() {
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));
    final JobRow snapshotJob =
        DbTestUtils.newJobWith(
            jdbiForTesting, namespaceRow.getName(), newJobName().getValue(), jobMeta);
    final RunRow runRow = DbTestUtils.newRun(jdbiForTesting, snapshotJob);
    DbTestUtils.transitionRunWithOutputs(
        jdbiForTesting, runRow.getUuid(), RunState.COMPLETED, jobMeta.getOutputs());

    List<ExtendedDatasetVersionRow> legacyInputs =
        datasetVersionDao.findInputDatasetVersionsFor(runRow.getUuid());
    List<ExtendedDatasetVersionRow> legacyOutputs =
        datasetVersionDao.findOutputDatasetVersionsFor(runRow.getUuid());
    RunIoSnapshot snapshot = jobVersionDao.findRunIoSnapshot(runRow.getUuid());

    assertThat(snapshot.getInputs()).containsExactlyInAnyOrderElementsOf(legacyInputs);
    assertThat(snapshot.getOutputs()).containsExactlyInAnyOrderElementsOf(legacyOutputs);

    ExtendedDatasetVersionRow outputAlsoUsedAsInput = legacyOutputs.get(0);
    runDao.updateInputMapping(runRow.getUuid(), outputAlsoUsedAsInput.getUuid());
    RunIoSnapshot overlappingSnapshot = jobVersionDao.findRunIoSnapshot(runRow.getUuid());

    assertThat(overlappingSnapshot.getInputs()).contains(outputAlsoUsedAsInput);
    assertThat(overlappingSnapshot.getOutputs()).contains(outputAlsoUsedAsInput);
    assertThatThrownBy(() -> overlappingSnapshot.getInputs().add(outputAlsoUsedAsInput))
        .isInstanceOf(UnsupportedOperationException.class);

    JobRow emptyJob =
        DbTestUtils.createJobWithoutSymlinkTarget(
            jdbiForTesting, namespaceRow, newJobName().getValue(), "");
    RunIoSnapshot emptySnapshot =
        jobVersionDao.findRunIoSnapshot(DbTestUtils.newRun(jdbiForTesting, emptyJob).getUuid());
    assertThat(emptySnapshot.getInputs()).isEmpty();
    assertThat(emptySnapshot.getOutputs()).isEmpty();
  }

  @Test
  public void testKnownNamespaceAndSnapshotLoaderDoesNotReadThemAgain() {
    JobVersionDao intakeDao = mock(JobVersionDao.class, CALLS_REAL_METHODS);
    UUID runUuid = UUID.randomUUID();
    RunIoSnapshot snapshot = RunIoSnapshot.empty();

    JobVersionDao.JobRowRunDetails details =
        intakeDao.loadJobRowRunDetails(jobRow, namespaceRow, runUuid, snapshot);

    assertThat(details.namespaceRow()).isSameAs(namespaceRow);
    assertThat(details.jobVersionInputs()).isSameAs(snapshot.getInputs());
    assertThat(details.jobVersionOutputs()).isSameAs(snapshot.getOutputs());
    verify(intakeDao, never()).createNamespaceDao();
    verify(intakeDao, never()).findRunIoRows(any(UUID.class));
  }

  @Test
  public void testLegacyLoaderUsesOneCombinedRunIoRead() {
    JobVersionDao legacyDao = mock(JobVersionDao.class, CALLS_REAL_METHODS);
    NamespaceDao namespaceDao = mock(NamespaceDao.class);
    UUID runUuid = UUID.randomUUID();
    when(legacyDao.createNamespaceDao()).thenReturn(namespaceDao);
    when(namespaceDao.findNamespaceByName(jobRow.getNamespaceName()))
        .thenReturn(Optional.of(namespaceRow));
    when(legacyDao.findRunIoRows(runUuid)).thenReturn(List.of());

    JobVersionDao.JobRowRunDetails details = legacyDao.loadJobRowRunDetails(jobRow, runUuid);

    assertThat(details.jobVersionInputs()).isEmpty();
    assertThat(details.jobVersionOutputs()).isEmpty();
    verify(legacyDao).createNamespaceDao();
    verify(namespaceDao).findNamespaceByName(jobRow.getNamespaceName());
    verify(legacyDao).findRunIoRows(runUuid);
    verify(legacyDao, never()).createDatasetVersionDao();
  }

  @Test
  public void testUpsertRunlessJobVersion() {
    // Generate a new job meta object with an existing namespace; the namespace will also be
    // associated with the input and output datasets for the job.
    final JobMeta jobMeta = newJobMetaWith(NamespaceName.of(namespaceRow.getName()));

    // (1) Add a new job; the input and output datasets for the job will also be added.
    final JobRow jobRow =
        DbTestUtils.newJobWith(
            jdbiForTesting, namespaceRow.getName(), newJobName().getValue(), jobMeta);

    // (2) Attach job datasets
    List<DatasetRecord> datasetInputs = new ArrayList<>();
    for (DatasetId di : jobMeta.getInputs()) {
      datasetInputs.add(projectDataset(di, jobRow.getCreatedAt()));
    }

    // Attach output datasets.
    List<DatasetRecord> datasetOutputs = new ArrayList<>();
    for (DatasetId di : jobMeta.getOutputs()) {
      datasetOutputs.add(projectDataset(di, jobRow.getCreatedAt()));
    }

    // (2) Upsert runless job version
    final BagOfJobVersionInfo bagOfJobVersionInfo =
        jobVersionDao.upsertRunlessJobVersion(jobRow, namespaceRow, datasetInputs, datasetOutputs);

    // Ensure the latest version is associated with the job.
    final JobRow jobRowForLatestRun =
        jobDao.findJobByNameAsRow(jobRow.getNamespaceName(), jobRow.getName()).get();
    assertThat(jobRowForLatestRun.getCurrentVersionUuid())
        .isPresent()
        .contains(bagOfJobVersionInfo.getJobVersionRow().getUuid());

    // Ensure the input datasets have been linked to the job version.
    final List<UUID> jobVersionInputDatasetUuids =
        jobVersionDao.findInputDatasetsFor(bagOfJobVersionInfo.getJobVersionRow().getUuid());
    assertThat(jobVersionInputDatasetUuids).hasSize(bagOfJobVersionInfo.getInputs().size());
    for (final ExtendedDatasetVersionRow jobVersionInputDatasetUuid :
        bagOfJobVersionInfo.getInputs()) {
      assertThat(jobVersionInputDatasetUuids).contains(jobVersionInputDatasetUuid.getDatasetUuid());
    }

    // Ensure the output datasets have been linked to the job version.
    final List<UUID> jobVersionOutputDatasetUuids =
        jobVersionDao.findOutputDatasetsFor(bagOfJobVersionInfo.getJobVersionRow().getUuid());
    assertThat(jobVersionOutputDatasetUuids).hasSize(bagOfJobVersionInfo.getOutputs().size());
    for (final ExtendedDatasetVersionRow outputDatasetVersion : bagOfJobVersionInfo.getOutputs()) {
      assertThat(jobVersionOutputDatasetUuids).contains(outputDatasetVersion.getDatasetUuid());
    }

    Optional<JobVersion> jobVersion =
        jobVersionDao.findJobVersion(
            jobRow.getNamespaceName(),
            jobRow.getName(),
            bagOfJobVersionInfo.getJobVersionRow().getVersion());
    assertThat(jobVersion)
        .isPresent()
        .get()
        .extracting(JobVersion::getInputs, InstanceOfAssertFactories.list(UUID.class))
        .isNotEmpty();
    assertThat(jobVersion)
        .isPresent()
        .get()
        .extracting(JobVersion::getOutputs, InstanceOfAssertFactories.list(DatasetId.class))
        .isNotEmpty();
  }

  private DatasetRecord projectDataset(DatasetId datasetId, Instant eventTime) {
    DatasetEvent event =
        DatasetEvent.builder()
            .eventTime(eventTime.atZone(java.time.ZoneOffset.UTC))
            .dataset(
                LineageEvent.Dataset.builder()
                    .namespace(datasetId.getNamespace().getValue())
                    .name(datasetId.getName().getValue())
                    .build())
            .producer("job-version-dao-test")
            .build();
    String eventJson = Utils.toJson(event);
    OpenLineageProjector.ProjectionResult projected =
        openLineageDao.inTransaction(
            transaction ->
                OpenLineageProjector.getInstance()
                    .projectInTransaction(
                        transaction,
                        Utils.getMapper(),
                        new OpenLineageProjector.ProjectionRequest(event, eventJson, false)));
    OpenLineageProjector.DatasetProjection dataset =
        ((OpenLineageProjector.DatasetProjectionResult) projected).outputs().get(0);
    return new DatasetRecord(
        dataset.dataset(), dataset.version(), dataset.namespace(), dataset.columnLineage());
  }

  @Test
  public void testUpsertDatasetMarksOtherRowsObsolete() {
    // (1) Add a new job; the input and output datasets for the job will also be added.
    final JobMeta jobMeta =
        new JobMeta(
            newJobType(),
            newInputsWith(NamespaceName.of(namespaceRow.getName()), 1),
            newOutputsWith(NamespaceName.of(namespaceRow.getName()), 1),
            newLocation(),
            newDescription(),
            null,
            null);

    final JobRow jobRow =
        DbTestUtils.newJobWith(
            jdbiForTesting, namespaceRow.getName(), newJobName().getValue(), jobMeta);

    // (2) Get UUID of the datasets
    DatasetId inputDatasetId = jobMeta.getInputs().stream().findFirst().get();
    DatasetId outputDatasetId = jobMeta.getOutputs().stream().findFirst().get();

    UUID inputDatasetUuid =
        datasetDao
            .getUuid(inputDatasetId.getNamespace().getValue(), inputDatasetId.getName().getValue())
            .get()
            .getUuid();
    UUID outputDatasetUuid =
        datasetDao
            .getUuid(
                outputDatasetId.getNamespace().getValue(), outputDatasetId.getName().getValue())
            .get()
            .getUuid();

    // (3) Upsert job version row
    UUID jobVersionUuid =
        jobVersionDao
            .upsertJobVersion(
                newRowUuid(),
                newTimestamp(),
                jobRow.getUuid(),
                newLocation().toString(),
                UUID.randomUUID(),
                jobRow.getName(),
                namespaceRow.getUuid(),
                namespaceRow.getName())
            .getUuid();

    // (4) upsert job_versions_io rows for each dataset
    jobVersionDao.upsertInputDatasetFor(
        jobVersionUuid, inputDatasetUuid, jobRow.getUuid(), jobRow.getSymlinkTargetId());
    jobVersionDao.upsertOutputDatasetFor(
        jobVersionUuid, outputDatasetUuid, jobRow.getUuid(), jobRow.getSymlinkTargetId());

    // (5) there should be 2 rows in job_versions_io_mapping
    assertThat(
            jdbiForTesting
                .withHandle(
                    h ->
                        h.createQuery(
                                "SELECT count(*) as cnt FROM job_versions_io_mapping WHERE job_uuid = :jobUuid AND is_current_job_version = TRUE")
                            .bind("jobUuid", jobRow.getUuid())
                            .map(rv -> rv.getColumn("cnt", Integer.class))
                            .one())
                .intValue())
        .isEqualTo(2);

    // (2) Modify job - create a new version of it
    UUID newJobVersion = UUID.randomUUID();
    ExtendedJobVersionRow newVersionRow =
        DbTestUtils.newJobVersion(
            jdbiForTesting,
            jobRow.getUuid(),
            newJobVersion,
            jobRow.getName(),
            namespaceRow.getUuid(),
            namespaceRow.getName());

    // (4) upsert job_versions_io rows for each dataset
    jobVersionDao.upsertInputDatasetFor(
        newVersionRow.getUuid(),
        inputDatasetUuid,
        jobRow.getUuid(),
        jobRow.getUuid()); // for testing use symlink job uuid same as job uuid
    jobVersionDao.upsertOutputDatasetFor(
        newVersionRow.getUuid(),
        outputDatasetUuid,
        jobRow.getUuid(),
        jobRow.getUuid()); // for testing use symlink job uuid same as job uuid

    // (5) Verify input and output datasets if they are the current ones
    assertThat(
            jdbiForTesting
                .withHandle(
                    h ->
                        h.createQuery(
                                "SELECT count(*) as cnt FROM job_versions_io_mapping WHERE job_uuid = :jobUuid")
                            .bind("jobUuid", jobRow.getUuid())
                            .map(rv -> rv.getColumn("cnt", Integer.class))
                            .one())
                .intValue())
        .isEqualTo(4);

    assertThat(
            jdbiForTesting
                .withHandle(
                    h ->
                        h.createQuery(
                                """
                            SELECT count(*) as cnt FROM job_versions_io_mapping
                            WHERE job_uuid = :jobUuid AND is_current_job_version = TRUE
                            AND job_symlink_target_uuid = :symlinkTargetId
                            """)
                            .bind("jobUuid", jobRow.getUuid())
                            .bind("symlinkTargetId", jobRow.getUuid())
                            .map(rv -> rv.getColumn("cnt", Integer.class))
                            .one())
                .intValue())
        .isEqualTo(2);
  }

  private void flushCombinedIo(
      ExtendedJobVersionRow jobVersion, JobRow job, UUID inputUuid, UUID outputUuid) {
    jdbiForTesting.useTransaction(
        handle ->
            handle
                .attach(JobVersionDao.class)
                .flushCurrentJobVersionIoInCurrentTransaction(
                    JobVersionDao.CurrentJobVersionIoWrite.of(
                        jobVersion.getUuid(),
                        job.getUuid(),
                        job.getSymlinkTargetId(),
                        List.of(inputUuid),
                        List.of(outputUuid))));
  }

  private int countMappings(UUID jobVersionUuid, JobVersionDao.IoType ioType, boolean current) {
    return jdbiForTesting.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT count(*)
                    FROM job_versions_io_mapping
                    WHERE job_version_uuid = :jobVersionUuid
                      AND io_type = :ioType
                      AND is_current_job_version = :current
                    """)
                .bind("jobVersionUuid", jobVersionUuid)
                .bind("ioType", ioType.name())
                .bind("current", current)
                .mapTo(Integer.class)
                .one());
  }
}
