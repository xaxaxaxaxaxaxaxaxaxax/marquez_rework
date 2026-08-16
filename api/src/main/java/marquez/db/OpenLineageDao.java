/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.time.Instant;
import java.util.UUID;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transactional;

/**
 * Transactional attachment anchor retained for existing service construction.
 *
 * <p>Raw events are owned by {@link OpenLineageEventDao}; relational projection is owned by {@link
 * OpenLineageProjector}. This interface intentionally contains no projection workflow or mutable
 * event state.
 */
public interface OpenLineageDao extends BaseDao, Transactional<OpenLineageDao> {
  /** Advances a current-run pointer for a projection that predates durable event ordering. */
  @SqlUpdate(
      """
      UPDATE jobs
      SET updated_at = GREATEST(updated_at, :updatedAt),
          current_run_uuid = :currentRunUuid,
          open_lineage_current_run_time = NULL,
          open_lineage_current_run_key = NULL
      WHERE uuid = :jobUuid AND is_hidden IS FALSE
      """)
  void updateCurrentRunForLegacyProjection(UUID jobUuid, UUID currentRunUuid, Instant updatedAt);
}
