/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.mappers;

import static marquez.db.Columns.stringOrNull;
import static marquez.db.Columns.stringOrThrow;
import static marquez.db.Columns.timestampOrThrow;
import static marquez.db.Columns.uuidOrNull;
import static marquez.db.Columns.uuidOrThrow;

import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.db.models.ExtendedDatasetVersionRow;
import marquez.db.models.RunIoRow;
import marquez.db.models.RunIoRow.IoType;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public final class RunIoRowMapper implements RowMapper<RunIoRow> {
  @Override
  public RunIoRow map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    return new RunIoRow(
        IoType.valueOf(stringOrThrow(results, Columns.IO_TYPE)),
        new ExtendedDatasetVersionRow(
            uuidOrThrow(results, Columns.ROW_UUID),
            timestampOrThrow(results, Columns.CREATED_AT),
            uuidOrThrow(results, Columns.DATASET_UUID),
            uuidOrThrow(results, Columns.VERSION),
            uuidOrNull(results, Columns.DATASET_SCHEMA_VERSION_UUID),
            stringOrNull(results, Columns.LIFECYCLE_STATE),
            uuidOrNull(results, Columns.RUN_UUID),
            stringOrThrow(results, Columns.NAMESPACE_NAME),
            stringOrThrow(results, Columns.DATASET_NAME)));
  }
}
