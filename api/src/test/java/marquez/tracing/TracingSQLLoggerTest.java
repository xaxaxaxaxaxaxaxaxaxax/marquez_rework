/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.tracing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import io.sentry.ISpan;
import io.sentry.Sentry;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import marquez.db.OpenLineageQueueDao;
import marquez.logging.SqlStatementIdentity;
import org.jdbi.v3.core.extension.ExtensionMethod;
import org.jdbi.v3.core.statement.Binding;
import org.jdbi.v3.core.statement.ParsedSql;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TracingSQLLoggerTest {
  private static final String DESCRIPTION =
      "Executed SQL: select :id\nJDBI SQL: select ?\nBinding: [binding]";

  private SqlLogger delegate;
  private TracingSQLLogger logger;
  private StatementContext context;
  private Map<String, Object> attributes;

  @BeforeEach
  public void setUp() {
    delegate = mock(SqlLogger.class);
    logger = new TracingSQLLogger(delegate);
    context = mock(StatementContext.class);
    attributes = new HashMap<>();

    when(context.getAttribute(anyString()))
        .thenAnswer(invocation -> attributes.get(invocation.getArgument(0)));
    doAnswer(
            invocation -> {
              attributes.put(invocation.getArgument(0), invocation.getArgument(1));
              return null;
            })
        .when(context)
        .define(anyString(), nullable(Object.class));

    ParsedSql parsedSql = mock(ParsedSql.class);
    when(parsedSql.getSql()).thenReturn("select :id");
    Binding binding = mock(Binding.class);
    when(binding.toString()).thenReturn("[binding]");
    when(context.getParsedSql()).thenReturn(parsedSql);
    when(context.getRenderedSql()).thenReturn("select ?");
    when(context.getBinding()).thenReturn(binding);
  }

  @Test
  public void testExplicitIdentityNamesSentryTask() {
    attributes.put(
        SqlStatementIdentity.class.getName(),
        new SqlStatementIdentity(OpenLineageQueueDao.class, "acquireOrderingKeyLock"));
    when(context.getRawSql()).thenReturn("select :id");

    assertBeforeTask(MetricRegistry.name(OpenLineageQueueDao.class, "acquireOrderingKeyLock"));
  }

  @Test
  public void testSqlObjectFallbackNamesSentryTask() throws NoSuchMethodException {
    when(context.getRawSql()).thenReturn("select :id");
    when(context.getExtensionMethod())
        .thenReturn(
            new ExtensionMethod(TestClass.class, TestClass.class.getDeclaredMethod("testMethod")));

    assertBeforeTask(MetricRegistry.name(TestClass.class, "testMethod"));
  }

  @Test
  public void testRawFallbackNamesSentryTask() {
    when(context.getRawSql()).thenReturn("select :id");

    assertBeforeTask("sql.raw");
  }

  @Test
  public void testEmptyFallbackNamesSentryTask() {
    when(context.getRawSql()).thenReturn("");

    assertBeforeTask("sql.empty");
  }

  @Test
  public void testSuccessFinishesStoredChildOnceWithoutFinishingAmbientParent() {
    when(context.getRawSql()).thenReturn("select :id");
    ISpan parent = mock(ISpan.class);
    ISpan child = mock(ISpan.class);
    when(parent.startChild("sql.raw", DESCRIPTION)).thenReturn(child);
    try (MockedStatic<Sentry> mockedSentry = Mockito.mockStatic(Sentry.class)) {
      mockedSentry.when(Sentry::getSpan).thenReturn(parent);

      logger.logBeforeExecution(context);
      logger.logAfterExecution(context);
      logger.logAfterExecution(context);

      verify(child).finish();
      verify(parent, never()).finish();
      verify(delegate, times(2)).logAfterExecution(context);
      mockedSentry.verify(Sentry::getSpan, times(1));
      assertFalse(attributes.containsValue(child));
    }
  }

  @Test
  public void testExceptionFinishesStoredChildAndPreservesCaptureAndDelegate() {
    when(context.getRawSql()).thenReturn("select :id");
    ISpan parent = mock(ISpan.class);
    ISpan child = mock(ISpan.class);
    when(parent.startChild("sql.raw", DESCRIPTION)).thenReturn(child);
    SQLException exception = new SQLException("failure");
    try (MockedStatic<Sentry> mockedSentry = Mockito.mockStatic(Sentry.class)) {
      mockedSentry.when(Sentry::getSpan).thenReturn(parent);

      logger.logBeforeExecution(context);
      logger.logException(context, exception);

      InOrder completionOrder = inOrder(child, delegate);
      completionOrder.verify(child).finish();
      completionOrder.verify(delegate).logException(context, exception);
      verify(parent, never()).finish();
      mockedSentry.verify(() -> Sentry.captureException(exception));
      mockedSentry.verify(Sentry::getSpan, times(1));
      assertFalse(attributes.containsValue(child));
    }
  }

  @Test
  public void testCompletionWithoutStoredChildDoesNotFinishAmbientParent() {
    ISpan ambientParent = mock(ISpan.class);
    try (MockedStatic<Sentry> mockedSentry = Mockito.mockStatic(Sentry.class)) {
      mockedSentry.when(Sentry::getSpan).thenReturn(null);
      logger.logBeforeExecution(context);
      mockedSentry.when(Sentry::getSpan).thenReturn(ambientParent);

      logger.logAfterExecution(context);

      verify(ambientParent, never()).finish();
      verify(delegate).logAfterExecution(context);
      mockedSentry.verify(Sentry::getSpan, times(1));
    }
  }

  private void assertBeforeTask(String taskName) {
    ISpan span = mock(ISpan.class);
    ISpan child = mock(ISpan.class);
    when(span.startChild(taskName, DESCRIPTION)).thenReturn(child);
    try (MockedStatic<Sentry> mockedSentry = Mockito.mockStatic(Sentry.class)) {
      mockedSentry.when(Sentry::getSpan).thenReturn(span);

      logger.logBeforeExecution(context);

      verify(span).startChild(taskName, DESCRIPTION);
      verify(delegate).logBeforeExecution(context);
      assertTrue(attributes.containsValue(child));
    }
  }

  private static class TestClass {
    public void testMethod() {}
  }
}
