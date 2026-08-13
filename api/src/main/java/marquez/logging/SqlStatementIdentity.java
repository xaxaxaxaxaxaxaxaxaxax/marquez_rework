/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.logging;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jdbi3.strategies.SmartNameStrategy;
import java.util.Objects;
import java.util.Optional;
import org.jdbi.v3.core.extension.ExtensionMethod;
import org.jdbi.v3.core.statement.SqlStatement;
import org.jdbi.v3.core.statement.StatementContext;

/** Identifies direct Jdbi statements as the DAO method whose work they perform. */
public record SqlStatementIdentity(Class<?> type, String methodName) {
  private static final String ATTRIBUTE = SqlStatementIdentity.class.getName();
  private static final SmartNameStrategy FALLBACK_NAME_STRATEGY = new SmartNameStrategy();

  public SqlStatementIdentity {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(methodName, "methodName");
    if (methodName.isBlank()) {
      throw new IllegalArgumentException("methodName must not be blank");
    }
  }

  /** Tags a direct statement and returns it so binding and execution can remain fluent. */
  public static <S extends SqlStatement<S>> S tag(S statement, Class<?> type, String methodName) {
    Objects.requireNonNull(statement, "statement")
        .getContext()
        .define(ATTRIBUTE, new SqlStatementIdentity(type, methodName));
    return statement;
  }

  static Optional<SqlStatementIdentity> resolve(StatementContext context) {
    Optional<SqlStatementIdentity> explicit = explicit(context);
    if (explicit.isPresent()) {
      return explicit;
    }

    ExtensionMethod extensionMethod = context.getExtensionMethod();
    return extensionMethod == null
        ? Optional.empty()
        : Optional.of(
            new SqlStatementIdentity(
                extensionMethod.getType(), extensionMethod.getMethod().getName()));
  }

  /** Returns the shared Codahale/Sentry statement name, preserving SmartName fallback behavior. */
  public static String statementName(StatementContext context) {
    return explicit(context)
        .map(SqlStatementIdentity::metricName)
        .orElseGet(() -> FALLBACK_NAME_STRATEGY.getStatementName(context));
  }

  public String typeName() {
    return type.getName();
  }

  public String metricName() {
    return MetricRegistry.name(type, methodName);
  }

  private static Optional<SqlStatementIdentity> explicit(StatementContext context) {
    Object value = context.getAttribute(ATTRIBUTE);
    return value instanceof SqlStatementIdentity identity
        ? Optional.of(identity)
        : Optional.empty();
  }
}
