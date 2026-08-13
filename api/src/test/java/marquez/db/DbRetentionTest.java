/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static java.time.temporal.ChronoUnit.DAYS;
import static marquez.api.models.ApiModelGenerator.newRunEvents;
import static marquez.common.models.CommonModelGenerator.newJobName;
import static marquez.common.models.CommonModelGenerator.newNamespaceName;
import static marquez.db.models.DbModelGenerator.newDatasetRowWith;
import static marquez.db.models.DbModelGenerator.newDatasetRowsWith;
import static marquez.db.models.DbModelGenerator.newDatasetVersionRowWith;
import static marquez.db.models.DbModelGenerator.newDatasetVersionsRowWith;
import static marquez.db.models.DbModelGenerator.newJobRowWith;
import static marquez.db.models.DbModelGenerator.newJobRowsWith;
import static marquez.db.models.DbModelGenerator.newJobVersionRowWith;
import static marquez.db.models.DbModelGenerator.newJobVersionRowsWith;
import static marquez.db.models.DbModelGenerator.newNamespaceRow;
import static marquez.db.models.DbModelGenerator.newRunArgRow;
import static marquez.db.models.DbModelGenerator.newRunRowWith;
import static marquez.db.models.DbModelGenerator.newRunRowsWith;
import static marquez.db.models.DbModelGenerator.newSourceRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import marquez.db.exceptions.DbRetentionException;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.JobRow;
import marquez.db.models.JobVersionRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.OpenLineageQueueRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunRow;
import marquez.db.models.SourceRow;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.jdbi.v3.jackson2.Jackson2Plugin;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.testing.junit5.JdbiExtension;
import org.jdbi.v3.testing.junit5.tc.JdbiTestcontainersExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;
import org.testcontainers.utility.DockerImageName;

/** The test suite for {@link DbRetention}. */
@Tag("DataAccessTests, IntegrationTests")
@Testcontainers
public class DbRetentionTest {
  private static final int NUMBER_OF_ROWS_PER_BATCH = 10;
  private static final int RETENTION_DAYS = 30;
  private static final boolean DRY_RUN = true;
  private static final Instant OLDER_THAN_X_DAYS = Instant.now().minus(RETENTION_DAYS + 1, DAYS);
  private static final Instant LAST_X_DAYS = Instant.now().minus(RETENTION_DAYS - 1, DAYS);

  static final DockerImageName POSTGRES_16 = DockerImageName.parse("postgres:16");

  @Container
  @Order(1)
  static final PostgreSQLContainer<?> DB_CONTAINER = new PostgreSQLContainer<>(POSTGRES_16);

  // Defined statically to significantly improve overall test execution.
  @RegisterExtension
  @Order(2)
  static final JdbiExtension jdbiExtension =
      JdbiTestcontainersExtension.instance(DB_CONTAINER)
          .withPlugin(new SqlObjectPlugin())
          .withPlugin(new PostgresPlugin())
          .withPlugin(new Jackson2Plugin())
          .withInitializer(
              (source, handle) -> {
                // Apply migrations.
                DbMigration.migrateDbOrError(source);
              });

  // Wraps test database connection.
  static TestingDb DB;

  @BeforeAll
  public static void setUpOnce() {
    // Wrap jdbi configured for running container.
    DB = TestingDb.newInstance(jdbiExtension.getJdbi());
  }

  @Test
  public void testRetentionRejectsNonPositiveArgumentsBeforeDatabaseAccess() {
    final Jdbi jdbi = mock(Jdbi.class);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> DbRetention.retentionOnDbOrError(jdbi, 0, RETENTION_DAYS, DRY_RUN))
        .withMessage("numberOfRowsPerBatch must be positive");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DbRetention.retentionOnDbOrError(jdbi, -1, RETENTION_DAYS))
        .withMessage("numberOfRowsPerBatch must be positive");
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> DbRetention.retentionOnDbOrError(jdbi, NUMBER_OF_ROWS_PER_BATCH, 0, DRY_RUN))
        .withMessage("retentionDays must be positive");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DbRetention.retentionOnDbOrError(jdbi, NUMBER_OF_ROWS_PER_BATCH, -1))
        .withMessage("retentionDays must be positive");
    verifyNoInteractions(jdbi);
  }

  @Test
  public void testRetentionOnDbOrErrorWithJobsOlderThanXDays() {
    // (1) Add namespace.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());

    // (2) Add jobs older than X days.
    final Set<JobRow> rowsOlderThanXDays =
        DB.upsertAll(
            newJobRowsWith(OLDER_THAN_X_DAYS, namespaceRow.getUuid(), namespaceRow.getName(), 4));

    // (3) Add jobs within last X days.
    final Set<JobRow> rowsLastXDays =
        DB.upsertAll(
            newJobRowsWith(LAST_X_DAYS, namespaceRow.getUuid(), namespaceRow.getName(), 2));

    // (4) Apply retention policy as dry run on jobs older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS, DRY_RUN);
      // (5) Query 'jobs' table for rows. We want to ensure: jobs older than X days
      // have not deleted; jobs within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isTrue();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply dry run", e);
    }

    // (6) Apply retention policy on jobs older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      // (7) Query 'jobs' table for rows deleted. We want to ensure: jobs older than X days
      // have been deleted; jobs within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isFalse();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void testRetentionOnDbOrErrorWithJobVersionsOlderThanXDays() {
    // (1) Add namespace and source.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());
    final SourceRow sourceRow = DB.upsert(newSourceRow());

    // (2) Add dataset (as inputs) associated with job version.
    final Set<DatasetRow> datasetsAsInput =
        DB.upsertAll(
            newDatasetRowsWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                2));

    // (3) Add dataset (as outputs) associated with job version.
    final Set<DatasetRow> datasetsAsOutput =
        DB.upsertAll(
            newDatasetRowsWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                4));

    // (4) Use any output dataset for job versions to obtain namespace and associate with job.
    final DatasetRow datasetAsOutput = datasetsAsOutput.stream().findAny().orElseThrow();
    final UUID namespaceUuid = datasetAsOutput.getNamespaceUuid();
    final String namespaceName = datasetAsOutput.getNamespaceName();

    // (5) Add job.
    final JobRow jobRow = DB.upsert(newJobRowWith(namespaceUuid, namespaceName));

    // (6) Add job versions older than X days associated with job.
    final Set<JobVersionRow> rowsOlderThanXDays =
        DB.upsertAll(
            newJobVersionRowsWith(
                OLDER_THAN_X_DAYS,
                namespaceUuid,
                namespaceName,
                jobRow.getUuid(),
                jobRow.getName(),
                datasetsAsInput,
                datasetsAsOutput,
                4));

    // (7) Add job versions within last X days associated with job.
    final Set<JobVersionRow> rowsLastXDays =
        DB.upsertAll(
            newJobVersionRowsWith(
                LAST_X_DAYS,
                namespaceUuid,
                namespaceName,
                jobRow.getUuid(),
                jobRow.getName(),
                datasetsAsInput,
                datasetsAsOutput,
                2));

    // (8) Apply retention policy as dry run on job versions older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS, DRY_RUN);
      // (9) Query 'job versions' table for rows. We want to ensure: job versions older
      // than X days have not been deleted; job versions within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isTrue();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply dry run", e);
    }

    // (10) Apply retention policy on job versions older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      // (11) Query 'job versions' table for rows deleted. We want to ensure: job versions older
      // than X days have been deleted; job versions within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isFalse();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void testRetentionOnDbOrErrorWithRunsOlderThanXDays() {
    // (1) Add namespace and source.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());
    final SourceRow sourceRow = DB.upsert(newSourceRow());

    // (2) Add dataset (as inputs) associated with job.
    final Set<DatasetRow> datasetsAsInput =
        DB.upsertAll(
            newDatasetRowsWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                2));

    // (3) Add dataset (as outputs) associated with job.
    final Set<DatasetRow> datasetsAsOutput =
        DB.upsertAll(
            newDatasetRowsWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                4));

    // (4) Use any output dataset for run to obtain namespace and associate with job.
    final DatasetRow datasetAsOutput = datasetsAsOutput.stream().findAny().orElseThrow();
    final UUID namespaceUuid = datasetAsOutput.getNamespaceUuid();
    final String namespaceName = datasetAsOutput.getNamespaceName();

    // (5) Add version for job.
    final JobRow jobRow = DB.upsert(newJobRowWith(namespaceUuid, namespaceName));
    final JobVersionRow jobVersionRow =
        DB.upsert(
            newJobVersionRowWith(
                namespaceUuid,
                namespaceName,
                jobRow.getUuid(),
                jobRow.getName(),
                datasetsAsInput,
                datasetsAsOutput));

    // (6) Add args for run.
    final RunArgsRow runArgsRow = DB.upsert(newRunArgRow());

    // (7) Add runs older than X days.
    final Set<RunRow> rowsOlderThanXDays =
        DB.upsertAll(
            newRunRowsWith(
                OLDER_THAN_X_DAYS,
                jobRow.getUuid(),
                jobVersionRow.getUuid(),
                runArgsRow.getUuid(),
                4));

    // (8) Add runs within last X days.
    final Set<RunRow> rowsLastXDays =
        DB.upsertAll(
            newRunRowsWith(
                LAST_X_DAYS, jobRow.getUuid(), jobVersionRow.getUuid(), runArgsRow.getUuid(), 2));

    // (9) Apply retention policy as dry run on runs older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS, DRY_RUN);
      // (10) Query 'runs' table for rows. We want to ensure: runs older than X days have not been
      // deleted; runs within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isTrue();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply dry run", e);
    }

    // (11) Apply retention policy on runs older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      // (12) Query 'runs' table for rows deleted. We want to ensure: runs older than X days have
      // been deleted; runs within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isFalse();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void testRetentionKeepsAnOldParentUntilItsChildIsEligible() {
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());
    final JobRow parentJob =
        DB.upsert(newJobRowWith(namespaceRow.getUuid(), namespaceRow.getName()));
    final JobVersionRow parentJobVersion =
        DB.upsert(
            newJobVersionRowWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                parentJob.getUuid(),
                parentJob.getName(),
                ImmutableSet.of(),
                ImmutableSet.of()));
    final JobRow childJob =
        DB.upsert(newJobRowWith(namespaceRow.getUuid(), namespaceRow.getName()));
    final JobVersionRow childJobVersion =
        DB.upsert(
            newJobVersionRowWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                childJob.getUuid(),
                childJob.getName(),
                ImmutableSet.of(),
                ImmutableSet.of()));
    final RunArgsRow runArgsRow = DB.upsert(newRunArgRow());
    final RunRow parent =
        DB.upsert(
            newRunRowWith(
                OLDER_THAN_X_DAYS,
                parentJob.getUuid(),
                parentJobVersion.getUuid(),
                runArgsRow.getUuid()));
    final RunRow child =
        DB.upsert(
            newRunRowWith(
                LAST_X_DAYS, childJob.getUuid(), childJobVersion.getUuid(), runArgsRow.getUuid()));
    jdbiExtension
        .getJdbi()
        .useHandle(
            handle -> {
              handle
                  .createUpdate("UPDATE jobs SET updated_at = :old WHERE uuid = :jobUuid")
                  .bind("old", OLDER_THAN_X_DAYS)
                  .bind("jobUuid", parentJob.getUuid())
                  .execute();
              handle
                  .createUpdate(
                      "UPDATE job_versions SET created_at = :old WHERE uuid = :jobVersionUuid")
                  .bind("old", OLDER_THAN_X_DAYS)
                  .bind("jobVersionUuid", parentJobVersion.getUuid())
                  .execute();
              handle
                  .createUpdate(
                      """
                      UPDATE runs
                         SET parent_run_uuid = :parentRunUuid
                       WHERE uuid = :childRunUuid
                      """)
                  .bind("parentRunUuid", parent.getUuid())
                  .bind("childRunUuid", child.getUuid())
                  .execute();
            });

    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowExists(handle, parentJob)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, parentJobVersion)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, parent)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, childJob)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, childJobVersion)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, child)).isTrue();
      }

      jdbiExtension
          .getJdbi()
          .useHandle(
              handle ->
                  handle
                      .createUpdate("UPDATE runs SET updated_at = :updatedAt WHERE uuid = :runUuid")
                      .bind("updatedAt", OLDER_THAN_X_DAYS)
                      .bind("runUuid", child.getUuid())
                      .execute());
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowExists(handle, parent)).isFalse();
        assertThat(DbTestUtils.rowExists(handle, child)).isFalse();
        // Job and job-version retention ran before the child became deletable in this invocation.
        assertThat(DbTestUtils.rowExists(handle, parentJob)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, parentJobVersion)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, childJob)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, childJobVersion)).isTrue();
      }

      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowExists(handle, parentJob)).isFalse();
        assertThat(DbTestUtils.rowExists(handle, parentJobVersion)).isFalse();
        assertThat(DbTestUtils.rowExists(handle, childJob)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, childJobVersion)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void testRetentionOnDbOrErrorWithDatasetsOlderThanXDays() {
    // (1) Add namespace and source.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());
    final SourceRow sourceRow = DB.upsert(newSourceRow());

    // (2) Add datasets older than X days.
    final Set<DatasetRow> rowsOlderThanXDays =
        DB.upsertAll(
            newDatasetRowsWith(
                OLDER_THAN_X_DAYS,
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                4));

    // (3) Add datasets within last X days.
    final Set<DatasetRow> rowsLastXDays =
        DB.upsertAll(
            newDatasetRowsWith(
                LAST_X_DAYS,
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                2));

    // (4) Apply retention policy as dry run on datasets older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS, DRY_RUN);
      // (5) Query 'datasets' table for rows. We want to ensure: datasets older than X days
      // have not been deleted; datasets within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isTrue();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply dry run", e);
    }

    // (6) Apply retention policy on datasets older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      // (7) Query 'datasets' table for rows deleted. We want to ensure: datasets older than X days
      // have been deleted; datasets within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isFalse();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void
      testRetentionOnDbOrErrorWithDatasetsOlderThanXDays_skipIfDatasetAsInputOrOutputForJobVersion() {
    // (1) Add namespace and source.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());
    final SourceRow sourceRow = DB.upsert(newSourceRow());

    // (2) Add datasets older than X days not associated with a job version; therefore, datasets
    // will be deleted when applying retention policy.
    final Set<DatasetRow> rowsOlderThanXDays =
        DB.upsertAll(
            newDatasetRowsWith(
                OLDER_THAN_X_DAYS,
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                4));

    // (3) Add datasets (as inputs) older than X days associated with a job version; therefore,
    // datasets will be skipped when applying retention policy.
    final Set<DatasetRow> rowsOlderThanXDaysAsInput =
        DB.upsertAll(
            newDatasetRowsWith(
                OLDER_THAN_X_DAYS,
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                2));

    // (4) Add datasets (as outputs) within last X days associated with a job version; therefore,
    // datasets will be skipped when applying retention policy.
    final Set<DatasetRow> rowsLastXDaysAsOutput =
        DB.upsertAll(
            newDatasetRowsWith(
                LAST_X_DAYS,
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                4));

    // (5) Use any output dataset to obtain namespace and associate with job.
    final DatasetRow rowLastXDaysAsOutput = rowsLastXDaysAsOutput.stream().findAny().orElseThrow();
    final UUID namespaceUuid = rowLastXDaysAsOutput.getNamespaceUuid();
    final String namespaceName = rowLastXDaysAsOutput.getNamespaceName();

    // (6) Add job and associate with job version; the job version will have input and output
    // datasets older than X days and within last X days, respectively.
    final JobRow jobRow = DB.upsert(newJobRowWith(namespaceUuid, namespaceName));
    DB.upsert(
        newJobVersionRowWith(
            namespaceUuid,
            namespaceName,
            jobRow.getUuid(),
            jobRow.getName(),
            rowsOlderThanXDaysAsInput,
            rowsLastXDaysAsOutput));

    // (7) Apply retention policy on datasets older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      // (8) Query 'datasets' table for rows deleted. We want to ensure: datasets older than X days
      // not associated with a job version have been deleted; datasets older than X days associated
      // with a job version have not been deleted; datasets within last X days associated with a job
      // version have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isFalse();
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDaysAsInput)).isTrue();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDaysAsOutput)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void testRetentionOnDbOrErrorWithDatasetVersionsOlderThanXDays() {
    // (1) Add namespace and source.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());
    final SourceRow sourceRow = DB.upsert(newSourceRow());

    // (2) Add dataset.
    final DatasetRow datasetRow =
        DB.upsert(
            newDatasetRowWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName()));

    // (3) Add versions for dataset older than X days.
    final Set<DatasetVersionRow> rowsOlderThanXDays =
        DB.upsertAll(
            newDatasetVersionsRowWith(
                OLDER_THAN_X_DAYS,
                datasetRow.getUuid(),
                datasetRow.getName(),
                datasetRow.getNamespaceName(),
                4));

    // (4) Add versions for dataset within last X days.
    final Set<DatasetVersionRow> rowsLastXDays =
        DB.upsertAll(
            newDatasetVersionsRowWith(
                LAST_X_DAYS,
                datasetRow.getUuid(),
                datasetRow.getName(),
                datasetRow.getNamespaceName(),
                2));

    // (5) Apply retention policy on dataset versions older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      // (6) Query 'dataset versions' table for rows deleted. We want to ensure: dataset versions
      // older than X days have been deleted; datasets within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isFalse();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void
      testRetentionOnDbOrErrorWithDatasetVersionsOlderThanXDays_skipIfVersionAsCurrentForDataset() {
    // (1) Add namespace and source.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());
    final SourceRow sourceRow = DB.upsert(newSourceRow());
    final DatasetRow datasetRow =
        DB.upsert(
            newDatasetRowWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName()));

    // (2) Add dataset versions older than X days.
    final Set<DatasetVersionRow> rowsOlderThanXDays =
        DB.upsertAll(
            newDatasetVersionsRowWith(
                OLDER_THAN_X_DAYS,
                datasetRow.getUuid(),
                datasetRow.getName(),
                datasetRow.getNamespaceName(),
                4));

    // (3) Add dataset versions within last X days.
    final Set<DatasetVersionRow> rowsLastXDays =
        DB.upsertAll(
            newDatasetVersionsRowWith(
                LAST_X_DAYS,
                datasetRow.getUuid(),
                datasetRow.getName(),
                datasetRow.getNamespaceName(),
                2));

    // (4) Add dataset version older than X days associated with dataset (as current version);
    // therefore, the dataset version will be skipped when applying retention policy.
    final DatasetVersionRow rowOlderThanXDaysAsCurrent =
        DB.upsert(
            newDatasetVersionRowWith(
                LAST_X_DAYS,
                datasetRow.getUuid(),
                datasetRow.getName(),
                datasetRow.getNamespaceName()),
            true);

    // (5) Apply retention policy on dataset versions older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      // (6) Query 'dataset versions' table for rows deleted. We want to ensure: dataset versions
      // older than X days have been deleted; dataset versions within last X days have not been
      // deleted; dataset versions older than X days associated with a dataset (as current version)
      // has not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDays)).isFalse();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDays)).isTrue();
        assertThat(DbTestUtils.rowExists(handle, rowOlderThanXDaysAsCurrent)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void
      testRetentionOnDbOrErrorWithDatasetVersionsOlderThanXDays_skipIfVersionAsInputForRun() {
    // (1) Add namespace and source.
    final NamespaceRow namespaceRow = DB.upsert(newNamespaceRow());
    final SourceRow sourceRow = DB.upsert(newSourceRow());

    // (2) Add dataset (as inputs) associated with job.
    final Set<DatasetRow> datasetsAsInput =
        DB.upsertAll(
            newDatasetRowsWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                2));

    // (3) Add dataset (as outputs) associated with job.
    final Set<DatasetRow> datasetsAsOutput =
        DB.upsertAll(
            newDatasetRowsWith(
                namespaceRow.getUuid(),
                namespaceRow.getName(),
                sourceRow.getUuid(),
                sourceRow.getName(),
                4));

    // (4) Add dataset versions older than X days for each input datasets associated with run.
    final ImmutableSet.Builder<DatasetVersionRow> builderRowsOlderThanXDaysAsInput =
        ImmutableSet.builder();
    for (final DatasetRow rowAsInput : datasetsAsInput) {
      builderRowsOlderThanXDaysAsInput.addAll(
          DB.upsertAll(
              newDatasetVersionsRowWith(
                  OLDER_THAN_X_DAYS,
                  rowAsInput.getUuid(),
                  rowAsInput.getName(),
                  rowAsInput.getNamespaceName(),
                  4)));
    }
    final Set<DatasetVersionRow> rowsOlderThanXDaysAsInput =
        builderRowsOlderThanXDaysAsInput.build();

    // (5) Add dataset versions within last X days for each output datasets associated with run.
    final ImmutableSet.Builder<DatasetVersionRow> builderRowsLastXDaysAsOutput =
        ImmutableSet.builder();
    for (final DatasetRow rowAsOutput : datasetsAsOutput) {
      builderRowsLastXDaysAsOutput.addAll(
          DB.upsertAll(
              newDatasetVersionsRowWith(
                  LAST_X_DAYS,
                  rowAsOutput.getUuid(),
                  rowAsOutput.getName(),
                  rowAsOutput.getNamespaceName(),
                  2)));
    }
    final Set<DatasetVersionRow> rowsLastXDaysAsOutput = builderRowsLastXDaysAsOutput.build();

    // (6) Use any output dataset for run to obtain namespace and associate with job.
    final DatasetRow datasetAsOutput = datasetsAsOutput.stream().findAny().orElseThrow();
    final UUID namespaceUuid = datasetAsOutput.getNamespaceUuid();
    final String namespaceName = datasetAsOutput.getNamespaceName();

    // (7) Add version for job.
    final JobRow jobRow = DB.upsert(newJobRowWith(namespaceUuid, namespaceName));
    final JobVersionRow jobVersionRow =
        DB.upsert(
            newJobVersionRowWith(
                namespaceUuid,
                namespaceName,
                jobRow.getUuid(),
                jobRow.getName(),
                datasetsAsInput,
                datasetsAsOutput));

    // (8) Add run and associate with job and version.
    final RunArgsRow runArgsRow = DB.upsert(newRunArgRow());
    final RunRow runRow =
        newRunRowWith(jobRow.getUuid(), jobVersionRow.getUuid(), runArgsRow.getUuid());

    // (9) Add dataset version (as input) older than X days associated with run;
    // therefore, the dataset version will be skipped when applying retention policy.
    final DatasetRow datasetAsInput = datasetsAsInput.stream().findAny().orElseThrow();
    final DatasetVersionRow rowOlderThanXDaysAsInput =
        DB.upsert(
            newDatasetVersionRowWith(
                LAST_X_DAYS,
                datasetAsInput.getUuid(),
                datasetAsInput.getName(),
                datasetAsInput.getNamespaceName(),
                runRow.getUuid()));
    DB.upsertWith(runRow, rowOlderThanXDaysAsInput.getUuid());

    // (10) Apply retention policy on dataset versions older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      // (11) Query 'dataset versions' table for rows deleted. We want to ensure: dataset versions
      // older than X days associated with a run (as input) has not been deleted; dataset versions
      // older than X days have been deleted; dataset versions within last X days have not been
      // deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.rowExists(handle, rowOlderThanXDaysAsInput)).isTrue();
        assertThat(DbTestUtils.rowsExist(handle, rowsOlderThanXDaysAsInput)).isFalse();
        assertThat(DbTestUtils.rowsExist(handle, rowsLastXDaysAsOutput)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void testRetentionOnDbOrErrorWithOlEventsOlderThanXDays() {
    // (1) Configure OL.
    final URI olProducer = URI.create("https://test.com/test");
    final OpenLineage ol = new OpenLineage(olProducer);

    // (2) Add namespace and job for OL events.
    final String namespaceName = newNamespaceName().getValue();
    final String jobName = newJobName().getValue();

    // (3) Add OL events older than X days.
    final Set<OpenLineage.RunEvent> olEventsOlderThanXDays =
        newRunEvents(ol, OLDER_THAN_X_DAYS, namespaceName, jobName, 4);
    DB.insertAll(olEventsOlderThanXDays);

    // (4) Add OL events within last X days.
    final Set<OpenLineage.RunEvent> olEventsLastXDays =
        newRunEvents(ol, LAST_X_DAYS, namespaceName, jobName, 2);
    DB.insertAll(olEventsLastXDays);

    // (5) Apply retention policy as dry run on OL events older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS, DRY_RUN);
      // (6) Query 'lineage events' table for events. We want to ensure: OL events older than X
      // days have not been deleted; OL events within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.olEventsExist(handle, olEventsOlderThanXDays)).isTrue();
        assertThat(DbTestUtils.olEventsExist(handle, olEventsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply dry run", e);
    }

    // (7) Apply retention policy on OL events older than X days.
    try {
      DbRetention.retentionOnDbOrError(
          jdbiExtension.getJdbi(), NUMBER_OF_ROWS_PER_BATCH, RETENTION_DAYS);
      // (8) Query 'lineage events' table for events deleted. We want to ensure: OL events older
      // than X days have been deleted; OL events within last X days have not been deleted.
      try (final Handle handle = DB.open()) {
        assertThat(DbTestUtils.olEventsExist(handle, olEventsOlderThanXDays)).isFalse();
        assertThat(DbTestUtils.olEventsExist(handle, olEventsLastXDays)).isTrue();
      }
    } catch (DbRetentionException e) {
      fail("failed to apply retention policy", e);
    }
  }

  @Test
  public void testRetentionOnDbOrErrorPurgesOneDeadLetterBatchOnly() throws DbRetentionException {
    final int deadLetterBatchSize = 2;
    final OpenLineageQueueDao queueDao =
        jdbiExtension.getJdbi().onDemand(OpenLineageQueueDao.class);
    final long firstOldDeadLetter = deadLetterAt(queueDao, OLDER_THAN_X_DAYS.minusSeconds(2));
    final long secondOldDeadLetter = deadLetterAt(queueDao, OLDER_THAN_X_DAYS.minusSeconds(1));
    final long thirdOldDeadLetter = deadLetterAt(queueDao, OLDER_THAN_X_DAYS);
    final long recentDeadLetter = deadLetterAt(queueDao, LAST_X_DAYS);
    final UUID liveOrderingKey = UUID.randomUUID();
    final long liveEvent = queueDao.enqueue(liveOrderingKey, "{\"state\":\"live\"}");
    final String liveEventBeforeRetention = queueRowAsJson(liveOrderingKey, liveEvent);
    final String liveHeadBeforeRetention = queueHeadAsJson(liveOrderingKey);

    DbRetention.retentionOnDbOrError(
        jdbiExtension.getJdbi(), deadLetterBatchSize, RETENTION_DAYS, DRY_RUN);

    assertThat(deadLetterExists(firstOldDeadLetter)).isTrue();
    assertThat(deadLetterExists(secondOldDeadLetter)).isTrue();
    assertThat(deadLetterExists(thirdOldDeadLetter)).isTrue();
    assertThat(deadLetterExists(recentDeadLetter)).isTrue();
    assertThat(queueRowAsJson(liveOrderingKey, liveEvent)).isEqualTo(liveEventBeforeRetention);
    assertThat(queueHeadAsJson(liveOrderingKey)).isEqualTo(liveHeadBeforeRetention);

    DbRetention.retentionOnDbOrError(jdbiExtension.getJdbi(), deadLetterBatchSize, RETENTION_DAYS);

    assertThat(deadLetterExists(firstOldDeadLetter)).isFalse();
    assertThat(deadLetterExists(secondOldDeadLetter)).isFalse();
    assertThat(deadLetterExists(thirdOldDeadLetter)).isTrue();
    assertThat(deadLetterExists(recentDeadLetter)).isTrue();
    assertThat(queueRowAsJson(liveOrderingKey, liveEvent)).isEqualTo(liveEventBeforeRetention);
    assertThat(queueHeadAsJson(liveOrderingKey)).isEqualTo(liveHeadBeforeRetention);
  }

  private static long deadLetterAt(OpenLineageQueueDao queueDao, Instant deadAt) {
    final UUID orderingKey = UUID.randomUUID();
    final long id = queueDao.enqueue(orderingKey, "{\"state\":\"dead\"}");
    jdbiExtension
        .getJdbi()
        .useTransaction(
            TransactionIsolationLevel.READ_COMMITTED,
            handle -> {
              final OpenLineageQueueDao transactionalQueueDao =
                  handle.attach(OpenLineageQueueDao.class);
              final OpenLineageQueueRow row = transactionalQueueDao.lockNextDue().orElseThrow();
              assertThat(row.id()).isEqualTo(id);
              transactionalQueueDao.deadLetterLocked(
                  row.orderingKey(), row.id(), row.attemptCount(), "test dead letter");
            });
    final int updated =
        jdbiExtension
            .getJdbi()
            .withHandle(
                handle ->
                    handle
                        .createUpdate(
                            "UPDATE open_lineage_dead_letters "
                                + "SET dead_at = :deadAt WHERE id = :id")
                        .bind("deadAt", deadAt)
                        .bind("id", id)
                        .execute());
    assertThat(updated).isEqualTo(1);
    return id;
  }

  private static boolean deadLetterExists(long id) {
    return jdbiExtension
        .getJdbi()
        .withHandle(
            handle ->
                handle
                    .createQuery(
                        "SELECT EXISTS (SELECT 1 FROM open_lineage_dead_letters WHERE id = :id)")
                    .bind("id", id)
                    .mapTo(Boolean.class)
                    .one());
  }

  private static String queueRowAsJson(UUID orderingKey, long id) {
    return jdbiExtension
        .getJdbi()
        .withHandle(
            handle ->
                handle
                    .createQuery(
                        "SELECT to_jsonb(queued)::text "
                            + "FROM open_lineage_queue AS queued "
                            + "WHERE ordering_key = :orderingKey AND id = :id")
                    .bind("orderingKey", orderingKey)
                    .bind("id", id)
                    .mapTo(String.class)
                    .one());
  }

  private static String queueHeadAsJson(UUID orderingKey) {
    return jdbiExtension
        .getJdbi()
        .withHandle(
            handle ->
                handle
                    .createQuery(
                        "SELECT to_jsonb(head)::text "
                            + "FROM open_lineage_queue_heads AS head "
                            + "WHERE ordering_key = :orderingKey")
                    .bind("orderingKey", orderingKey)
                    .mapTo(String.class)
                    .one());
  }
}
