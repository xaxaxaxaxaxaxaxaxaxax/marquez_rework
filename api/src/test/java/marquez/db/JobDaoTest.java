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
import static org.assertj.core.api.Assertions.catchThrowable;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import marquez.db.models.ProjectionOrder;
import marquez.db.models.RunRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.Job;
import marquez.service.models.JobMeta;
import marquez.service.models.Run;
import marquez.service.models.RunMeta;
import org.assertj.core.api.AbstractObjectAssert;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
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
  public void orderedPointersDoNotMutateHiddenJobs() {
    JobRow job =
        createJobWithoutSymlinkTarget(
            jdbi, namespace, "hidden-pointer-" + UUID.randomUUID(), "hidden pointer test");
    jobDao.delete(job.getNamespaceName(), job.getName());
    Instant projectionTime = Instant.now().plusSeconds(60);
    ProjectionOrder order =
        new ProjectionOrder(projectionTime, Utils.sha256Utf8("hidden job pointer"));

    assertThat(jobDao.updateCurrentRunFor(job.getUuid(), UUID.randomUUID(), order)).isFalse();
    assertThat(jobDao.updateVersionFor(job.getUuid(), UUID.randomUUID(), order)).isFalse();
    assertThat(jobDao.canProjectCurrentIo(job.getUuid(), order)).isFalse();

    JobRow hidden = jobDao.lockJobByUuid(job.getUuid());
    assertThat(hidden.getCurrentRunUuid()).isEmpty();
    assertThat(hidden.getCurrentVersionUuid()).isEmpty();
  }

  @Test
  void unifiedJobUpsertSupportsParentAndProjectionAxes() {
    String suffix = UUID.randomUUID().toString();
    Instant eventTime = Instant.parse("2026-08-15T00:00:00Z");
    JobRow parent =
        createJobWithoutSymlinkTarget(
            jdbi, namespace, "unified-parent-" + suffix, "unified parent");

    for (boolean parentAware : List.of(false, true)) {
      for (boolean ordered : List.of(false, true)) {
        String simpleName = "unified-" + parentAware + "-" + ordered + "-" + suffix;
        UUID parentUuid = parentAware ? parent.getUuid() : null;
        byte[] projectionKey = ordered ? Utils.sha256Utf8(simpleName) : null;
        ProjectionOrder order = ordered ? new ProjectionOrder(eventTime, projectionKey) : null;

        JobRow inserted = upsertProjection(parentUuid, simpleName, eventTime, order);

        assertThat(inserted.getParentJobUuid()).isEqualTo(parentUuid);
        SnapshotWatermark watermark = snapshotWatermark(inserted.getUuid());
        assertThat(watermark.eventTime()).isEqualTo(ordered ? eventTime : null);
        assertThat(watermark.eventKey()).isEqualTo(projectionKey);
      }
    }
  }

  @ParameterizedTest(
      name = "raw order remains database-validated: parent={0}, hasTime={1}, hasKey={2}")
  @CsvSource({"false,true,true", "true,true,false", "true,false,true"})
  void rawOrderedCompatibilityPreservesPostgresConstraintFailures(
      boolean parentAware, boolean hasTime, boolean hasKey) {
    String suffix = UUID.randomUUID().toString();
    Instant now = Instant.parse("2026-08-15T01:00:00Z");
    Instant projectionTime = hasTime ? now : null;
    JobRow parent =
        parentAware
            ? createJobWithoutSymlinkTarget(jdbi, namespace, "raw-parent-" + suffix, "raw parent")
            : null;
    byte[] malformedKey = hasKey ? new byte[hasTime ? 31 : 32] : null;

    Throwable failure =
        catchThrowable(
            () -> {
              if (parentAware) {
                jobDao.upsertOpenLineageJob(
                    UUID.randomUUID(),
                    parent.getUuid(),
                    JobType.BATCH,
                    now,
                    namespace.getUuid(),
                    namespace.getName(),
                    "raw-child-" + suffix,
                    null,
                    null,
                    null,
                    jobDao.toJson(Collections.emptySet(), Utils.getMapper()),
                    projectionTime,
                    malformedKey);
              } else {
                jobDao.upsertOpenLineageJob(
                    UUID.randomUUID(),
                    JobType.BATCH,
                    now,
                    namespace.getUuid(),
                    namespace.getName(),
                    "raw-parentless-" + suffix,
                    null,
                    null,
                    null,
                    jobDao.toJson(Collections.emptySet(), Utils.getMapper()),
                    projectionTime,
                    malformedKey);
              }
            });

    assertThat(failure).isInstanceOf(UnableToExecuteStatementException.class);
    Throwable rootCause = rootCause(failure);
    assertThat(rootCause).isInstanceOf(SQLException.class);
    assertThat(((SQLException) rootCause).getSQLState()).isEqualTo("23514");
    assertThat(rootCause).hasMessageContaining("jobs_open_lineage_snapshot_order_pair");
  }

  @Test
  void losingOrderedWriteAfterLegacyDeleteReturnsHiddenBaseRow() {
    String name = "hidden-snapshot-" + UUID.randomUUID();
    JobRow job = createJobWithoutSymlinkTarget(jdbi, namespace, name, "legacy winner");
    Instant losingTime = job.getUpdatedAt().minusSeconds(1);
    jobDao.delete(job.getNamespaceName(), job.getName());

    JobRow returned =
        upsertProjection(
            null,
            name,
            losingTime,
            new ProjectionOrder(losingTime, Utils.sha256Utf8("losing hidden snapshot")));

    assertThat(returned.getUuid()).isEqualTo(job.getUuid());
    assertThat(returned.getDescription()).contains("legacy winner");
    assertThat(jobDao.findJobByNameAsRow(namespace.getName(), name)).isEmpty();
  }

  @Test
  void legacyJobWritesClearOwnedWatermarksWithoutLoweringUpdatedAt() {
    String name = "legacy-job-watermark-" + UUID.randomUUID();
    JobRow job = createJobWithoutSymlinkTarget(jdbi, namespace, name, "initial legacy snapshot");
    RunRow legacyRun = DbTestUtils.newRun(jdbi, job);
    Instant highWater = Instant.parse("2030-08-13T00:00:30Z");
    Instant olderLegacyTime = highWater.minusSeconds(20);
    byte[] digest = Utils.sha256Utf8("ordered-before-legacy");
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    UPDATE jobs
                    SET updated_at = :highWater,
                        open_lineage_snapshot_time = :highWater,
                        open_lineage_snapshot_key = :digest,
                        open_lineage_current_run_time = :highWater,
                        open_lineage_current_run_key = :digest
                    WHERE uuid = :jobUuid
                    """)
                .bind("highWater", highWater)
                .bind("digest", digest)
                .bind("jobUuid", job.getUuid())
                .execute());

    jobDao.upsertJob(
        UUID.randomUUID(),
        JobType.BATCH,
        olderLegacyTime,
        namespace.getUuid(),
        namespace.getName(),
        name,
        "legacy winner",
        "legacy-location",
        null,
        jobDao.toJson(Collections.emptySet(), Utils.getMapper()),
        legacyRun.getUuid());
    jobDao.upsertJob(
        UUID.randomUUID(),
        JobType.BATCH,
        olderLegacyTime,
        namespace.getUuid(),
        namespace.getName(),
        name,
        "legacy winner",
        "legacy-location",
        null,
        jobDao.toJson(Collections.emptySet(), Utils.getMapper()),
        null);

    Map<String, Object> snapshot =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT updated_at, description, current_run_uuid,
                               open_lineage_snapshot_time, open_lineage_current_run_time
                        FROM jobs WHERE uuid = :jobUuid
                        """)
                    .bind("jobUuid", job.getUuid())
                    .mapToMap()
                    .one());
    assertThat(((java.sql.Timestamp) snapshot.get("updated_at")).toInstant()).isEqualTo(highWater);
    assertThat(snapshot.get("description")).isEqualTo("legacy winner");
    assertThat(snapshot.get("current_run_uuid")).isEqualTo(legacyRun.getUuid());
    assertThat(snapshot.get("open_lineage_snapshot_time")).isNull();
    assertThat(snapshot.get("open_lineage_current_run_time")).isNull();

    UUID legacyVersion = UUID.randomUUID();
    jdbi.useHandle(
        handle ->
            handle
                .createUpdate(
                    """
                    UPDATE jobs
                    SET open_lineage_current_version_time = :highWater,
                        open_lineage_current_version_key = :digest
                    WHERE uuid = :jobUuid
                    """)
                .bind("highWater", highWater)
                .bind("digest", digest)
                .bind("jobUuid", job.getUuid())
                .execute());
    jobDao.updateVersionFor(job.getUuid(), olderLegacyTime, legacyVersion);

    Map<String, Object> pointer =
        jdbi.withHandle(
            handle ->
                handle
                    .createQuery(
                        """
                        SELECT updated_at, current_version_uuid,
                               open_lineage_current_version_time
                        FROM jobs WHERE uuid = :jobUuid
                        """)
                    .bind("jobUuid", job.getUuid())
                    .mapToMap()
                    .one());
    assertThat(((java.sql.Timestamp) pointer.get("updated_at")).toInstant()).isEqualTo(highWater);
    assertThat(pointer.get("current_version_uuid")).isEqualTo(legacyVersion);
    assertThat(pointer.get("open_lineage_current_version_time")).isNull();
  }

  @Test
  void syntheticParentInsertInitializesOnlyANewIdentityWithNeutralSnapshotOrder() {
    String insertedName = "synthetic-parent-" + UUID.randomUUID();
    UUID insertedUuid = UUID.randomUUID();
    jobDao.insertSyntheticParentJobIfAbsent(
        insertedUuid, Instant.EPOCH, namespace.getUuid(), namespace.getName(), insertedName);

    SyntheticSnapshotState inserted = syntheticSnapshotState(namespace.getUuid(), insertedName);
    assertThat(inserted.uuid()).isEqualTo(insertedUuid);
    assertThat(inserted.eventTime()).isEqualTo("-infinity");
    assertThat(inserted.eventKey()).containsExactly(new byte[32]);

    String existingName = "existing-parent-" + UUID.randomUUID();
    JobRow existing =
        createJobWithoutSymlinkTarget(jdbi, namespace, existingName, "legacy parent identity");
    assertThat(syntheticSnapshotState(namespace.getUuid(), existingName).eventTime()).isNull();

    jobDao.insertSyntheticParentJobIfAbsent(
        UUID.randomUUID(), Instant.EPOCH, namespace.getUuid(), namespace.getName(), existingName);

    SyntheticSnapshotState unchanged = syntheticSnapshotState(namespace.getUuid(), existingName);
    assertThat(unchanged.uuid()).isEqualTo(existing.getUuid());
    assertThat(unchanged.eventTime()).isNull();
    assertThat(unchanged.eventKey()).isNull();
  }

  @Test
  void newerSyntheticParentPointerDoesNotBlockOlderOrEqualTimeSnapshots() {
    String parentName = "pointer-before-parent-snapshot-" + UUID.randomUUID();
    Instant parentTime = Instant.parse("2026-08-13T02:00:00Z");
    Instant childTime = parentTime.plusSeconds(1);
    JobRow syntheticParent =
        jobDao.getOrCreateSyntheticParentJob(
            UUID.randomUUID(), namespace.getUuid(), namespace.getName(), parentName);
    RunRow parentRun = DbTestUtils.newRun(jdbi, syntheticParent);
    byte[] pointerKey = Utils.sha256Utf8("child-t2-parent-pointer");

    assertThat(
            jobDao.updateCurrentRunFor(
                syntheticParent.getUuid(),
                parentRun.getUuid(),
                new ProjectionOrder(childTime, pointerKey)))
        .isTrue();

    byte[] digestA = Utils.sha256Utf8("parent-t1-snapshot-a");
    byte[] digestB = Utils.sha256Utf8("parent-t1-snapshot-b");
    boolean digestAIsLower = Arrays.compareUnsigned(digestA, digestB) < 0;
    byte[] lowDigest = digestAIsLower ? digestA : digestB;
    byte[] highDigest = digestAIsLower ? digestB : digestA;
    PGobject inputs = jobDao.toJson(Collections.emptySet(), Utils.getMapper());

    JobRow firstSnapshot =
        jobDao.upsertOpenLineageJob(
            UUID.randomUUID(),
            JobType.BATCH,
            parentTime,
            namespace.getUuid(),
            namespace.getName(),
            parentName,
            "parent T1 low snapshot",
            "parent-t1-low",
            null,
            inputs,
            new ProjectionOrder(parentTime, lowDigest));
    assertThat(firstSnapshot.getUuid()).isEqualTo(syntheticParent.getUuid());
    assertThat(firstSnapshot.getDescription()).contains("parent T1 low snapshot");
    assertThat(firstSnapshot.getUpdatedAt()).isEqualTo(childTime);
    assertThat(firstSnapshot.getCurrentRunUuid()).contains(parentRun.getUuid());

    jobDao.upsertOpenLineageJob(
        UUID.randomUUID(),
        JobType.BATCH,
        parentTime,
        namespace.getUuid(),
        namespace.getName(),
        parentName,
        "parent T1 high snapshot",
        "parent-t1-high",
        null,
        inputs,
        new ProjectionOrder(parentTime, highDigest));
    jobDao.upsertOpenLineageJob(
        UUID.randomUUID(),
        JobType.BATCH,
        parentTime,
        namespace.getUuid(),
        namespace.getName(),
        parentName,
        "parent T1 losing replay",
        "parent-t1-losing-replay",
        null,
        inputs,
        new ProjectionOrder(parentTime, lowDigest));

    JobRow winner = jobDao.lockJobByUuid(syntheticParent.getUuid());
    assertThat(winner.getDescription()).contains("parent T1 high snapshot");
    assertThat(winner.getLocation()).isEqualTo("parent-t1-high");
    assertThat(winner.getUpdatedAt()).isEqualTo(childTime);
    assertThat(winner.getCurrentRunUuid()).contains(parentRun.getUuid());
    SnapshotWatermark snapshot = snapshotWatermark(syntheticParent.getUuid());
    assertThat(snapshot.eventTime()).isEqualTo(parentTime);
    assertThat(snapshot.eventKey()).containsExactly(highDigest);
    PointerWatermark pointer = pointerWatermark(syntheticParent.getUuid());
    assertThat(pointer.eventTime()).isEqualTo(childTime);
    assertThat(pointer.eventKey()).containsExactly(pointerKey);
  }

  @ParameterizedTest(name = "parent snapshot has higher equal-time digest: {0}")
  @ValueSource(booleans = {false, true})
  public void hierarchyEnrichmentIsAddOnlyAndIndependentOfSnapshotOrder(
      boolean parentSnapshotHasHigherDigest) {
    String suffix = UUID.randomUUID().toString();
    Instant eventTime = Instant.parse("2026-08-13T00:00:00Z");
    NamespaceRow firstParentNamespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(),
            eventTime,
            "hierarchy-parent-one-" + suffix,
            JobDaoTest.class.getName());
    NamespaceRow conflictingParentNamespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(),
            eventTime,
            "hierarchy-parent-two-" + suffix,
            JobDaoTest.class.getName());
    String parentName = "hierarchy-parent-" + suffix;
    JobRow firstParent =
        createJobWithoutSymlinkTarget(
            jdbi, firstParentNamespace, parentName, "first hierarchy parent");
    JobRow conflictingParent =
        createJobWithoutSymlinkTarget(
            jdbi, conflictingParentNamespace, parentName, "conflicting hierarchy parent");

    byte[] digestA = Utils.sha256Utf8("hierarchy-snapshot-a");
    byte[] digestB = Utils.sha256Utf8("hierarchy-snapshot-b");
    boolean digestAIsLower = Arrays.compareUnsigned(digestA, digestB) < 0;
    byte[] lowDigest = digestAIsLower ? digestA : digestB;
    byte[] highDigest = digestAIsLower ? digestB : digestA;
    byte[] parentlessDigest = parentSnapshotHasHigherDigest ? lowDigest : highDigest;
    byte[] parentDigest = parentSnapshotHasHigherDigest ? highDigest : lowDigest;
    String simpleName = "task";
    String fullName = parentName + "." + simpleName;
    PGobject inputs = jobDao.toJson(Collections.emptySet(), Utils.getMapper());

    JobRow parentless =
        jobDao.upsertOpenLineageJob(
            UUID.randomUUID(),
            JobType.BATCH,
            eventTime,
            namespace.getUuid(),
            namespace.getName(),
            fullName,
            "parentless snapshot",
            "parentless location",
            null,
            inputs,
            new ProjectionOrder(eventTime, parentlessDigest));
    jobDao.upsertOpenLineageJob(
        UUID.randomUUID(),
        firstParent.getUuid(),
        JobType.BATCH,
        eventTime,
        namespace.getUuid(),
        namespace.getName(),
        simpleName,
        "parent snapshot",
        "parent location",
        null,
        inputs,
        new ProjectionOrder(eventTime, parentDigest));

    JobRow enriched = jobDao.lockJobByUuid(parentless.getUuid());
    assertThat(enriched.getParentJobUuid()).isEqualTo(firstParent.getUuid());
    assertThat(enriched.getSimpleName()).isEqualTo(simpleName);
    assertThat(enriched.getDescription())
        .contains(parentSnapshotHasHigherDigest ? "parent snapshot" : "parentless snapshot");
    assertThat(enriched.getLocation())
        .isEqualTo(parentSnapshotHasHigherDigest ? "parent location" : "parentless location");
    SnapshotWatermark equalTimeWatermark = snapshotWatermark(parentless.getUuid());
    assertThat(equalTimeWatermark.eventTime()).isEqualTo(eventTime);
    assertThat(equalTimeWatermark.eventKey()).containsExactly(highDigest);

    Instant missingParentTime = eventTime.plusSeconds(1);
    byte[] missingParentDigest = Utils.sha256Utf8("newer-missing-parent");
    jobDao.upsertOpenLineageJob(
        UUID.randomUUID(),
        JobType.BATCH,
        missingParentTime,
        namespace.getUuid(),
        namespace.getName(),
        fullName,
        "newer missing-parent snapshot",
        "newer missing-parent location",
        null,
        inputs,
        new ProjectionOrder(missingParentTime, missingParentDigest));

    JobRow afterMissingParent = jobDao.lockJobByUuid(parentless.getUuid());
    assertThat(afterMissingParent.getParentJobUuid()).isEqualTo(firstParent.getUuid());
    assertThat(afterMissingParent.getSimpleName()).isEqualTo(simpleName);
    assertThat(afterMissingParent.getDescription()).contains("newer missing-parent snapshot");
    assertThat(afterMissingParent.getLocation()).isEqualTo("newer missing-parent location");
    SnapshotWatermark missingParentWatermark = snapshotWatermark(parentless.getUuid());
    assertThat(missingParentWatermark.eventTime()).isEqualTo(missingParentTime);
    assertThat(missingParentWatermark.eventKey()).containsExactly(missingParentDigest);

    Instant conflictingParentTime = eventTime.plusSeconds(2);
    byte[] conflictingParentDigest = Utils.sha256Utf8("newer-conflicting-parent");
    jobDao.upsertOpenLineageJob(
        UUID.randomUUID(),
        conflictingParent.getUuid(),
        JobType.BATCH,
        conflictingParentTime,
        namespace.getUuid(),
        namespace.getName(),
        simpleName,
        "newer conflicting-parent snapshot",
        "newer conflicting-parent location",
        null,
        inputs,
        new ProjectionOrder(conflictingParentTime, conflictingParentDigest));

    JobRow afterConflict = jobDao.lockJobByUuid(parentless.getUuid());
    assertThat(afterConflict.getParentJobUuid()).isEqualTo(firstParent.getUuid());
    assertThat(afterConflict.getSimpleName()).isEqualTo(simpleName);
    assertThat(afterConflict.getDescription()).contains("newer conflicting-parent snapshot");
    assertThat(afterConflict.getLocation()).isEqualTo("newer conflicting-parent location");
    SnapshotWatermark finalWatermark = snapshotWatermark(parentless.getUuid());
    assertThat(finalWatermark.eventTime()).isEqualTo(conflictingParentTime);
    assertThat(finalWatermark.eventKey()).containsExactly(conflictingParentDigest);
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

  private static JobRow upsertProjection(
      UUID parentJobUuid, String name, Instant eventTime, ProjectionOrder order) {
    return jobDao.upsertJob(
        JobDao.JobUpsertRequest.forOpenLineageProjection(
            UUID.randomUUID(),
            parentJobUuid,
            JobType.BATCH,
            eventTime,
            namespace.getUuid(),
            namespace.getName(),
            name,
            "projection snapshot",
            null,
            null,
            jobDao.toJson(Collections.emptySet(), Utils.getMapper()),
            order));
  }

  private static SnapshotWatermark snapshotWatermark(UUID jobUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT open_lineage_snapshot_time, open_lineage_snapshot_key "
                        + "FROM jobs WHERE uuid = :jobUuid")
                .bind("jobUuid", jobUuid)
                .map(
                    (resultSet, context) ->
                        new SnapshotWatermark(
                            Optional.ofNullable(
                                    resultSet.getTimestamp("open_lineage_snapshot_time"))
                                .map(java.sql.Timestamp::toInstant)
                                .orElse(null),
                            resultSet.getBytes("open_lineage_snapshot_key")))
                .one());
  }

  private static Throwable rootCause(Throwable throwable) {
    Throwable cause = throwable;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  private static PointerWatermark pointerWatermark(UUID jobUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT open_lineage_current_run_time, open_lineage_current_run_key "
                        + "FROM jobs WHERE uuid = :jobUuid")
                .bind("jobUuid", jobUuid)
                .map(
                    (resultSet, context) ->
                        new PointerWatermark(
                            resultSet.getTimestamp("open_lineage_current_run_time").toInstant(),
                            resultSet.getBytes("open_lineage_current_run_key")))
                .one());
  }

  private static SyntheticSnapshotState syntheticSnapshotState(UUID namespaceUuid, String name) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    """
                    SELECT uuid, open_lineage_snapshot_time::text AS snapshot_time,
                           open_lineage_snapshot_key AS snapshot_key
                    FROM jobs
                    WHERE namespace_uuid = :namespaceUuid AND name = :name
                    """)
                .bind("namespaceUuid", namespaceUuid)
                .bind("name", name)
                .map(
                    (resultSet, context) ->
                        new SyntheticSnapshotState(
                            resultSet.getObject("uuid", UUID.class),
                            resultSet.getString("snapshot_time"),
                            resultSet.getBytes("snapshot_key")))
                .one());
  }

  private record SnapshotWatermark(Instant eventTime, byte[] eventKey) {}

  private record PointerWatermark(Instant eventTime, byte[] eventKey) {}

  private record SyntheticSnapshotState(UUID uuid, String eventTime, byte[] eventKey) {}

  private record CurrentRunFixture(RunId currentRunId, JobMeta jobMeta) {}
}
