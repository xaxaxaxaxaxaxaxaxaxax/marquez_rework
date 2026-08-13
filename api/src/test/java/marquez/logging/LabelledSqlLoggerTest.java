/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jdbi3.InstrumentedSqlLogger;
import com.codahale.metrics.jdbi3.strategies.SmartNameStrategy;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import marquez.db.OpenLineageQueueDao;
import marquez.service.DatabaseMetrics;
import org.jdbi.v3.core.extension.ExtensionMethod;
import org.jdbi.v3.core.statement.Query;
import org.jdbi.v3.core.statement.StatementContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.MDC;

public class LabelledSqlLoggerTest {

  private LabelledSqlLogger logger;
  private StatementContext context;

  @BeforeEach
  public void setUp() {
    logger = new LabelledSqlLogger();
    context = mock(StatementContext.class);
    when(context.getElapsedTime(ChronoUnit.NANOS)).thenReturn(1_000_000_000L);
  }

  @AfterEach
  public void tearDown() {
    MDC.clear();
  }

  @Test
  public void testLogAfterExecutionUsesExtensionMethod() throws NoSuchMethodException {
    MDC.put("method", "GET");
    MDC.put("pathWithParams", "/test/path");
    when(context.getExtensionMethod()).thenReturn(extensionMethod(TestClass.class, "testMethod"));

    try (MockedStatic<DatabaseMetrics> mockedDatabaseMetrics =
        Mockito.mockStatic(DatabaseMetrics.class)) {
      logger.logAfterExecution(context);

      mockedDatabaseMetrics.verify(
          () ->
              DatabaseMetrics.recordDbDuration(
                  TestClass.class.getName(), "testMethod", "GET", "/test/path", 1.0),
          times(1));
    }
  }

  @Test
  public void testLogExceptionUsesExtensionMethod() throws NoSuchMethodException {
    MDC.put("method", "POST");
    MDC.put("pathWithParams", "/test/exception");
    when(context.getExtensionMethod()).thenReturn(extensionMethod(TestClass.class, "testMethod"));

    try (MockedStatic<DatabaseMetrics> mockedDatabaseMetrics =
        Mockito.mockStatic(DatabaseMetrics.class)) {
      logger.logException(context, new SQLException("Test Exception"));

      mockedDatabaseMetrics.verify(
          () ->
              DatabaseMetrics.recordDbDuration(
                  TestClass.class.getName(), "testMethod", "POST", "/test/exception", 1.0),
          times(1));
    }
  }

  @Test
  public void testExplicitIdentityOverridesExtensionMethod() throws NoSuchMethodException {
    MDC.put("method", "POST");
    MDC.put("pathWithParams", "/api/v1/lineage");
    when(context.getExtensionMethod())
        .thenReturn(extensionMethod(FallbackClass.class, "fallbackMethod"));
    tagContext(OpenLineageQueueDao.class, "acquireOrderingKeyLock");

    try (MockedStatic<DatabaseMetrics> mockedDatabaseMetrics =
        Mockito.mockStatic(DatabaseMetrics.class)) {
      logger.logAfterExecution(context);

      mockedDatabaseMetrics.verify(
          () ->
              DatabaseMetrics.recordDbDuration(
                  OpenLineageQueueDao.class.getName(),
                  "acquireOrderingKeyLock",
                  "POST",
                  "/api/v1/lineage",
                  1.0),
          times(1));
    }
  }

  @Test
  public void testNoIdentityDoesNotRecordLabelledMetric() {
    MDC.put("method", "POST");
    MDC.put("pathWithParams", "/api/v1/lineage");

    try (MockedStatic<DatabaseMetrics> mockedDatabaseMetrics =
        Mockito.mockStatic(DatabaseMetrics.class)) {
      logger.logAfterExecution(context);

      mockedDatabaseMetrics.verifyNoInteractions();
    }
  }

  @Test
  public void testExplicitIdentityNamesInstrumentedMetric() {
    tagContext(OpenLineageQueueDao.class, "insertEventAndMaybeHeadAfterLock");
    MetricRegistry registry = new MetricRegistry();
    InstrumentedSqlLogger instrumented =
        new InstrumentedSqlLogger(registry, SqlStatementIdentity::statementName);

    instrumented.logAfterExecution(context);

    String expected =
        MetricRegistry.name(OpenLineageQueueDao.class, "insertEventAndMaybeHeadAfterLock");
    assertEquals(Set.of(expected), registry.getTimers().keySet());
    assertEquals(1L, registry.getTimers().get(expected).getCount());
  }

  @Test
  public void testStatementNamePreservesSqlObjectFallback() throws NoSuchMethodException {
    when(context.getRawSql()).thenReturn("select 1");
    when(context.getExtensionMethod()).thenReturn(extensionMethod(TestClass.class, "testMethod"));

    assertEquals(
        new SmartNameStrategy().getStatementName(context),
        SqlStatementIdentity.statementName(context));
    assertEquals(
        MetricRegistry.name(TestClass.class, "testMethod"),
        SqlStatementIdentity.statementName(context));
  }

  @Test
  public void testStatementNamePreservesRawFallback() {
    when(context.getRawSql()).thenReturn("select 1");

    assertEquals(
        new SmartNameStrategy().getStatementName(context),
        SqlStatementIdentity.statementName(context));
    assertEquals("sql.raw", SqlStatementIdentity.statementName(context));
  }

  @Test
  public void testStatementNamePreservesEmptyFallback() {
    when(context.getRawSql()).thenReturn("");

    assertEquals(
        new SmartNameStrategy().getStatementName(context),
        SqlStatementIdentity.statementName(context));
    assertEquals("sql.empty", SqlStatementIdentity.statementName(context));
  }

  @Test
  public void testIdentityRejectsBlankMethodName() {
    assertThrows(
        IllegalArgumentException.class, () -> new SqlStatementIdentity(TestClass.class, " "));
  }

  private void tagContext(Class<?> type, String methodName) {
    Map<String, Object> attributes = new HashMap<>();
    doAnswer(
            invocation -> {
              attributes.put(invocation.getArgument(0), invocation.getArgument(1));
              return null;
            })
        .when(context)
        .define(anyString(), any());
    when(context.getAttribute(anyString()))
        .thenAnswer(invocation -> attributes.get(invocation.getArgument(0)));

    Query statement = mock(Query.class);
    when(statement.getContext()).thenReturn(context);
    assertSame(statement, SqlStatementIdentity.tag(statement, type, methodName));
  }

  private static ExtensionMethod extensionMethod(Class<?> type, String methodName)
      throws NoSuchMethodException {
    return new ExtensionMethod(type, type.getDeclaredMethod(methodName));
  }

  private static class TestClass {
    public void testMethod() {}
  }

  private static class FallbackClass {
    public void fallbackMethod() {}
  }
}
