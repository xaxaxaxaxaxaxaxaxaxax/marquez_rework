/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.models;

import java.time.Instant;
import java.util.UUID;

/** An OpenLineage event whose lane head is locked, including its proposed attempt ordinal. */
public record OpenLineageQueueRow(
    long id,
    UUID orderingKey,
    String eventJson,
    Instant enqueuedAt,
    int attemptCount,
    String lastError) {}
