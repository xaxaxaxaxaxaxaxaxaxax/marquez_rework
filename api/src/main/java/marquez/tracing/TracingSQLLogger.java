/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.tracing;

import io.sentry.ISpan;
import io.sentry.Sentry;
import java.sql.SQLException;
import marquez.logging.SqlStatementIdentity;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;

public class TracingSQLLogger implements SqlLogger {
  private static final String CHILD_SPAN_ATTRIBUTE =
      TracingSQLLogger.class.getName() + ".childSpan";

  private final SqlLogger delegate;

  public TracingSQLLogger(SqlLogger delegate) {
    super();
    this.delegate = delegate;
  }

  private String taskName(StatementContext context) {
    return SqlStatementIdentity.statementName(context);
  }

  public void logBeforeExecution(StatementContext context) {
    ISpan parent = Sentry.getSpan();
    if (parent != null) {
      String taskName = taskName(context);
      String description =
          "Executed SQL: "
              + context.getParsedSql().getSql()
              + "\nJDBI SQL: "
              + context.getRenderedSql()
              + "\nBinding: "
              + context.getBinding();
      context.define(CHILD_SPAN_ATTRIBUTE, parent.startChild(taskName, description));
    }
    delegate.logBeforeExecution(context);
  }

  public void logAfterExecution(StatementContext context) {
    finishChildSpan(context);
    delegate.logAfterExecution(context);
  }

  public void logException(StatementContext context, SQLException ex) {
    Sentry.captureException(ex);
    finishChildSpan(context);
    delegate.logException(context, ex);
  }

  private static void finishChildSpan(StatementContext context) {
    Object value = context.getAttribute(CHILD_SPAN_ATTRIBUTE);
    if (value instanceof ISpan child) {
      // Clear before finishing so an unexpected duplicate completion cannot finish it twice.
      context.define(CHILD_SPAN_ATTRIBUTE, null);
      child.finish();
    }
  }
}
