/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.logging;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import marquez.service.DatabaseMetrics;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;
import org.slf4j.MDC;

/**
 * A {@link SqlLogger} implementation for JDBI which uses DAO class and method identities for
 * nanosecond-precision timers.
 */
public class LabelledSqlLogger implements SqlLogger {

  @Override
  public void logAfterExecution(StatementContext context) {
    log(context);
  }

  @Override
  public void logException(StatementContext context, SQLException ex) {
    log(context);
  }

  private void log(StatementContext context) {
    SqlStatementIdentity.resolve(context)
        .ifPresent(
            identity -> {
              final long elapsed = context.getElapsedTime(ChronoUnit.NANOS);
              if (MDC.get("method") != null && MDC.get("pathWithParams") != null) {
                DatabaseMetrics.recordDbDuration(
                    identity.typeName(),
                    identity.methodName(),
                    MDC.get("method"),
                    MDC.get("pathWithParams"),
                    elapsed / 1e9);
              }
            });
  }
}
