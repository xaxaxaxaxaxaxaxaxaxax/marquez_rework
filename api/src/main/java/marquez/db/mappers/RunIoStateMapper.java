/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.mappers;

import static marquez.db.Columns.stringOrThrow;
import static marquez.db.Columns.timestampOrThrow;
import static marquez.db.Columns.uuidArrayOrThrow;
import static marquez.db.Columns.uuidOrThrow;

import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.NonNull;
import marquez.db.models.RunIoState;
import marquez.db.models.RunIoState.IoType;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public final class RunIoStateMapper implements RowMapper<RunIoState> {
  @Override
  public RunIoState map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    return new RunIoState(
        uuidOrThrow(results, "run_uuid"),
        IoType.valueOf(stringOrThrow(results, "io_type")),
        timestampOrThrow(results, "event_time"),
        results.getBytes("event_key"),
        uuidArrayOrThrow(results, "dataset_version_uuids"));
  }
}
