/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.jobs;

import com.google.common.util.concurrent.AbstractScheduledService;
import io.dropwizard.lifecycle.Managed;
import java.time.Duration;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.db.DbRetention;
import marquez.db.exceptions.DbRetentionException;
import org.jdbi.v3.core.Jdbi;

/**
 * A job that applies a retention policy on a fixed schedule to source, dataset, and job metadata in
 * Marquez. Use {@code frequencyMins} in {@link DbRetentionConfig} to override the default job run
 * frequency interval of {@code 15} mins. You can also use {@code retentionDays} to override the
 * default retention policy of {@code 7} days; metadata with a collection date {@code >
 * retentionDays} will be deleted. Each legacy metadata delete is limited to {@code
 * numberOfRowsPerBatch}, but one retention invocation may execute multiple such chunks; this value
 * is not a total metadata-row limit per invocation. Each invocation that reaches dead-letter
 * retention removes at most one such batch of queued OpenLineage dead letters older than {@code
 * retentionDays}; live queued events are never removed by age. If this job is not configured,
 * queued dead letters are retained indefinitely unless retention is invoked manually.
 */
@Slf4j
public class DbRetentionJob extends AbstractScheduledService implements Managed {
  private static final Duration NO_DELAY = Duration.ofMinutes(0);

  @FunctionalInterface
  interface RetentionIteration {
    void run(Jdbi jdbi, int numberOfRowsPerBatch, int retentionDays) throws DbRetentionException;
  }

  /* The retention policy frequency. */
  private final int frequencyMins;

  /* The legacy metadata delete chunk size and per-invocation dead-letter limit. */
  private final int numberOfRowsPerBatch;

  /* The retention days. */
  private final int retentionDays;

  private final Scheduler fixedRateScheduler;
  private final Jdbi jdbi;
  private final RetentionIteration retentionIteration;

  /**
   * Constructs a {@code DbRetentionJob} with run frequency {@code frequencyMins}, legacy metadata
   * delete chunk size and per-invocation dead-letter limit {@code numberOfRowsPerBatch}, and
   * retention period {@code retentionDays}.
   */
  public DbRetentionJob(
      @NonNull final Jdbi jdbi, @NonNull final DbRetentionConfig dbRetentionConfig) {
    this(jdbi, dbRetentionConfig, DbRetention::retentionOnDbOrError);
  }

  DbRetentionJob(
      @NonNull final Jdbi jdbi,
      @NonNull final DbRetentionConfig dbRetentionConfig,
      @NonNull final RetentionIteration retentionIteration) {
    this.frequencyMins = dbRetentionConfig.getFrequencyMins();
    this.numberOfRowsPerBatch = dbRetentionConfig.getNumberOfRowsPerBatch();
    this.retentionDays = dbRetentionConfig.getRetentionDays();

    // Connection to database retention policy will be applied.
    this.jdbi = jdbi;
    this.retentionIteration = retentionIteration;

    // Define fixed schedule with no delay.
    this.fixedRateScheduler =
        Scheduler.newFixedRateSchedule(
            NO_DELAY, Duration.ofMinutes(dbRetentionConfig.getFrequencyMins()));
  }

  @Override
  protected Scheduler scheduler() {
    return fixedRateScheduler;
  }

  @Override
  public void start() throws Exception {
    startAsync().awaitRunning();
    log.info(
        "Started db retention job with retention policy of '{}' days, "
            + "scheduled to be applied every '{}' mins.",
        retentionDays,
        frequencyMins);
  }

  @Override
  protected void runOneIteration() {
    try {
      // Keep the scheduled service running after retention or database failures so the next
      // iteration can retry.
      retentionIteration.run(jdbi, numberOfRowsPerBatch, retentionDays);
    } catch (DbRetentionException | RuntimeException errorOnDbRetention) {
      log.error(
          "Failed to apply retention policy of '{}' days to database; "
              + "retrying on the next scheduled run.",
          retentionDays,
          errorOnDbRetention);
    }
  }

  @Override
  public void stop() throws Exception {
    log.info("Stopping db retention job...");
    stopAsync().awaitTerminated();
  }
}
