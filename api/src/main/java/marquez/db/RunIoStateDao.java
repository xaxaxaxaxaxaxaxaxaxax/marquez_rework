/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import marquez.db.mappers.RunIoStateMapper;
import marquez.db.models.RunIoState;
import marquez.db.models.RunIoState.IoType;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBeanList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

/** Stores forward-only, authoritative OpenLineage input/output snapshots for runs. */
@RegisterRowMapper(RunIoStateMapper.class)
public interface RunIoStateDao extends BaseDao {
  int MAX_STATES_PER_UPSERT = 1000;

  @Transaction
  default boolean upsert(RunIoState state) {
    return upsertAllInTransaction(List.of(state)) == 1;
  }

  @Transaction
  default int upsertAll(List<RunIoState> states) {
    return upsertAllInTransaction(states);
  }

  default int upsertAllInTransaction(List<RunIoState> states) {
    if (states.isEmpty()) {
      return 0;
    }

    // Match PostgreSQL's lock order, then collapse duplicate conflict targets to their LWW winner.
    List<RunIoState> orderedStates =
        states.stream().sorted(RunIoStateDao::compareForWrite).toList();
    Map<StateIdentity, RunIoState> winnerByIdentity = new LinkedHashMap<>();
    for (RunIoState state : orderedStates) {
      StateIdentity identity = new StateIdentity(state.getRunUuid(), state.getIoType());
      winnerByIdentity.merge(identity, state, RunIoStateDao::laterState);
    }
    List<RunIoState> writes = List.copyOf(winnerByIdentity.values());

    int affected = 0;
    for (List<RunIoState> chunk : Lists.partition(writes, MAX_STATES_PER_UPSERT)) {
      affected += upsertChunk(chunk);
    }
    return affected;
  }

  @SqlUpdate(
      """
      WITH requested(
          run_uuid, io_type, event_time, event_key, dataset_version_uuids
      ) AS (VALUES <values>)
      INSERT INTO open_lineage_run_io_state (
          run_uuid, io_type, event_time, event_key, dataset_version_uuids)
      SELECT
          CAST(requested.run_uuid AS uuid),
          CAST(requested.io_type AS varchar),
          CAST(requested.event_time AS timestamptz),
          CAST(requested.event_key AS bytea),
          CAST(requested.dataset_version_uuids AS uuid[])
      FROM requested
      ORDER BY CAST(requested.run_uuid AS uuid), CAST(requested.io_type AS varchar)
      ON CONFLICT (run_uuid, io_type) DO UPDATE
      SET event_time = EXCLUDED.event_time,
          event_key = EXCLUDED.event_key,
          dataset_version_uuids = EXCLUDED.dataset_version_uuids
      WHERE ROW(EXCLUDED.event_time, EXCLUDED.event_key) >
            ROW(open_lineage_run_io_state.event_time, open_lineage_run_io_state.event_key)
      """)
  int upsertChunk(
      @BindBeanList(
              value = "values",
              propertyNames = {
                "runUuid",
                "ioType",
                "eventTime",
                "eventKey",
                "datasetVersionUuidArray"
              })
          List<RunIoState> states);

  @SqlQuery(
      """
      SELECT run_uuid, io_type, event_time, event_key, dataset_version_uuids
      FROM open_lineage_run_io_state
      WHERE run_uuid = :runUuid
      ORDER BY io_type
      """)
  List<RunIoState> findForRun(UUID runUuid);

  @SqlQuery(
      """
      SELECT run_uuid, io_type, event_time, event_key, dataset_version_uuids
      FROM open_lineage_run_io_state
      WHERE run_uuid = :runUuid AND io_type = :ioType
      """)
  Optional<RunIoState> findForRunAndType(UUID runUuid, IoType ioType);

  private static RunIoState laterState(RunIoState left, RunIoState right) {
    int compared = left.getEventTime().compareTo(right.getEventTime());
    if (compared == 0) {
      compared = Arrays.compareUnsigned(left.getEventKey(), right.getEventKey());
    }
    return compared < 0 ? right : left;
  }

  private static int compareForWrite(RunIoState left, RunIoState right) {
    int compared = compareUuidLikePostgres(left.getRunUuid(), right.getRunUuid());
    return compared != 0 ? compared : left.getIoType().name().compareTo(right.getIoType().name());
  }

  /** PostgreSQL compares UUIDs as unsigned bytes; {@link UUID#compareTo} compares signed longs. */
  private static int compareUuidLikePostgres(UUID left, UUID right) {
    int compared =
        Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
    return compared != 0
        ? compared
        : Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
  }

  record StateIdentity(UUID runUuid, IoType ioType) {}
}
