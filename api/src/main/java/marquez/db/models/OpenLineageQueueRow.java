/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.models;

import java.util.UUID;

/**
 * An OpenLineage event protected by its locked lane head, including its proposed attempt ordinal. A
 * bounded batch claim may also return that head's immediate same-admission follower.
 */
public record OpenLineageQueueRow(
    long id, UUID orderingKey, String eventJson, int attemptCount, Long admissionId) {}
