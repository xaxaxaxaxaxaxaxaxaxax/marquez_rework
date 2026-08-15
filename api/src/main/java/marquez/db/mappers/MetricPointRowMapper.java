/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.mappers;

import static marquez.db.Columns.longOrThrow;
import static marquez.db.Columns.timestampOrThrow;

import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.NonNull;
import marquez.db.Columns;
import marquez.db.models.MetricPoint;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public final class MetricPointRowMapper implements RowMapper<MetricPoint> {
  @Override
  public MetricPoint map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    return new MetricPoint(
        timestampOrThrow(results, Columns.START_AT),
        timestampOrThrow(results, Columns.END_AT),
        longOrThrow(results, Columns.VALUE));
  }
}
