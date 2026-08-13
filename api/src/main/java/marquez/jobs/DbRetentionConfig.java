/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.jobs;

import static marquez.db.DbRetention.DEFAULT_NUMBER_OF_ROWS_PER_BATCH;
import static marquez.db.DbRetention.DEFAULT_RETENTION_DAYS;

import javax.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Value;

/**
 * Configuration for {@link DbRetentionJob}. The retention age and batch size also apply to queued
 * OpenLineage dead letters. Live queued events are never age-purged, and dead letters are retained
 * indefinitely when scheduled database retention is not configured and no one-off retention command
 * is invoked.
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Value
public class DbRetentionConfig {
  public static final int DEFAULT_FREQUENCY_MINS = 15;

  @Builder.Default @Getter @Positive int frequencyMins = DEFAULT_FREQUENCY_MINS;
  @Builder.Default @Getter @Positive int numberOfRowsPerBatch = DEFAULT_NUMBER_OF_ROWS_PER_BATCH;
  @Builder.Default @Getter @Positive int retentionDays = DEFAULT_RETENTION_DAYS;
}
