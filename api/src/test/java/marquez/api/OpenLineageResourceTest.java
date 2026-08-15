/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSortedSet;
import io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import marquez.api.exceptions.JdbiExceptionExceptionMapper;
import marquez.common.Utils;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageQueueDao;
import marquez.db.OpenLineageQueueDao.PreparedEvent;
import marquez.service.JobService;
import marquez.service.LineageService;
import marquez.service.OpenLineageIntake;
import marquez.service.OpenLineageService;
import marquez.service.ServiceFactory;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.Lineage;
import marquez.service.models.LineageEvent;
import marquez.service.models.Node;
import marquez.service.models.NodeId;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
class OpenLineageResourceTest {
  private static final String INVALID_NAMESPACE = "namespace-\uD83D\uDE02";
  private static final OpenLineageDao OPEN_LINEAGE_DAO = mock(OpenLineageDao.class);
  private static final OpenLineageIntake OPEN_LINEAGE_INTAKE = mock(OpenLineageIntake.class);
  private static final OpenLineageResource RESOURCE;
  private static final ResourceExtension UNDER_TEST;
  private static final Lineage LINEAGE;

  static {
    LineageService lineageService = mock(LineageService.class);
    JobService jobService = mock(JobService.class);
    OpenLineageService openLineageService = mock(OpenLineageService.class);
    when(jobService.exists(anyString(), anyString())).thenReturn(true);

    Node testNode =
        Utils.fromJson(
            OpenLineageResourceTest.class.getResourceAsStream("/lineage/node.json"),
            new TypeReference<>() {});
    LINEAGE = new Lineage(ImmutableSortedSet.of(testNode));
    when(lineageService.lineage(any(NodeId.class), anyInt())).thenReturn(LINEAGE);

    ServiceFactory serviceFactory =
        ApiTestUtils.mockServiceFactory(
            Map.of(
                LineageService.class,
                lineageService,
                JobService.class,
                jobService,
                OpenLineageService.class,
                openLineageService));

    RESOURCE = new OpenLineageResource(serviceFactory, OPEN_LINEAGE_DAO, OPEN_LINEAGE_INTAKE);
    UNDER_TEST =
        ResourceExtension.builder()
            .setMapper(Utils.newObjectMapper())
            .addResource(RESOURCE)
            .addProvider(new JdbiExceptionExceptionMapper())
            .addProvider(new JsonProcessingExceptionMapper(false))
            .build();
  }

  @BeforeEach
  void setUpOpenLineageIntake() {
    reset(OPEN_LINEAGE_DAO, OPEN_LINEAGE_INTAKE);
    when(OPEN_LINEAGE_INTAKE.enqueue(any(PreparedEvent.class))).thenReturn(1L);
    when(OPEN_LINEAGE_INTAKE.enqueueAll(anyList()))
        .thenAnswer(invocation -> ((List<?>) invocation.getArgument(0)).size());
  }

  @Test
  void testCreateLineageEvent() throws IOException {
    BaseEvent event = eventFrom("/open_lineage/event_required_only.json");

    Response response = RESOURCE.create(event);

    assertEquals(201, response.getStatus());
    assertFalse(response.hasEntity());
    verify(OPEN_LINEAGE_INTAKE).enqueue(OpenLineageQueueDao.prepare(event));
  }

  @Test
  void testCreateDatasetEvent() throws IOException {
    BaseEvent event = eventFrom("/open_lineage/event_dataset_event.json");

    assertEquals(DatasetEvent.class, event.getClass());
    assertEquals(201, RESOURCE.create(event).getStatus());
    verify(OPEN_LINEAGE_INTAKE).enqueue(OpenLineageQueueDao.prepare(event));
  }

  @Test
  void testCreateJobEvent() throws IOException {
    BaseEvent event = eventFrom("/open_lineage/event_job_event.json");

    assertEquals(JobEvent.class, event.getClass());
    assertEquals(201, RESOURCE.create(event).getStatus());
    verify(OPEN_LINEAGE_INTAKE).enqueue(OpenLineageQueueDao.prepare(event));
  }

  @Test
  void testCreateLineageEventWithInvalidInputIdentifierReturnsBadRequest() throws IOException {
    LineageEvent event = (LineageEvent) eventFrom("/open_lineage/event_simple.json");
    event.getInputs().get(0).setNamespace(INVALID_NAMESPACE);

    Response response = RESOURCE.create(event);

    assertEquals(400, response.getStatus());
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateNestedNulReturnsBadRequestBeforeIntake() throws IOException {
    LineageEvent event = (LineageEvent) eventFrom("/open_lineage/event_required_only.json");
    LineageEvent.RunFacet facets = new LineageEvent.RunFacet();
    facets.setFacet("custom", Map.of("nested", Map.of("value", String.valueOf('\0'))));
    event.getRun().setFacets(facets);

    Response response = RESOURCE.create(event);

    assertEquals(400, response.getStatus());
    assertFalse(response.hasEntity());
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateJobEventWithInvalidInputIdentifierReturnsBadRequest() throws IOException {
    JobEvent event = (JobEvent) eventFrom("/open_lineage/event_job_event.json");
    event.getInputs().get(0).setNamespace(INVALID_NAMESPACE);

    Response response = RESOURCE.create(event);

    assertEquals(400, response.getStatus());
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateUnicodeBlankParentIdentityFailsValidationBeforeIntake() {
    String event =
        "{\"eventTime\": \"2021-11-03T10:53:52.427343Z\", \"eventType\": \"COMPLETE\", "
            + "\"inputs\": [], \"job\": {\"name\": \"child\", \"namespace\": \"namespace\"}, "
            + "\"outputs\": [], \"producer\": \"me\", "
            + "\"run\": {\"runId\": \"dae0d60a-6010-4c37-980e-c5270f5a6be4\", "
            + "\"facets\": {\"parent\": {\"_producer\": \"https://me\", "
            + "\"_schemaURL\": \"https://me\", \"run\": {\"runId\": \"\u2003\u00a0\"}, "
            + "\"job\": {\"namespace\": \"parent-ns\", \"name\": \"parent\"}}}}}";

    try (Response response =
        UNDER_TEST
            .target("/api/v1/lineage")
            .request()
            .post(Entity.entity(event, MediaType.APPLICATION_JSON_TYPE))) {
      assertEquals(422, response.getStatus());
    }
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateMalformedJsonReturnsGenericBadRequestBeforeIntake() {
    String malformedEvent = "{\"producer\":\"password=do-not-return\",";

    try (Response response =
        UNDER_TEST
            .target("/api/v1/lineage")
            .request()
            .post(Entity.entity(malformedEvent, MediaType.APPLICATION_JSON_TYPE))) {
      assertEquals(400, response.getStatus());
      String body = response.readEntity(String.class);
      assertTrue(body.contains("Unable to process JSON"));
      assertFalse(body.contains("password=do-not-return"));
    }
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateAdmissionIllegalArgumentReturnsInternalServerError() throws IOException {
    String event = eventJsonFrom("/open_lineage/event_required_only.json");
    when(OPEN_LINEAGE_INTAKE.enqueue(any(PreparedEvent.class)))
        .thenThrow(new IllegalArgumentException("password=do-not-return"));

    try (Response response =
        UNDER_TEST
            .target("/api/v1/lineage")
            .request()
            .post(Entity.entity(event, MediaType.APPLICATION_JSON_TYPE))) {
      assertEquals(500, response.getStatus());
      assertFalse(response.getHeaders().containsKey("Retry-After"));
      String body = response.readEntity(String.class);
      assertFalse(body.contains("password=do-not-return"));
    }
    verify(OPEN_LINEAGE_INTAKE).enqueue(any(PreparedEvent.class));
  }

  @Test
  void testCreateDatabaseAdmissionFailureReturnsInternalServerError() throws IOException {
    String event = eventJsonFrom("/open_lineage/event_required_only.json");
    UnableToExecuteStatementException databaseFailure =
        new UnableToExecuteStatementException(
            "password=do-not-return; SQL statement: insert into open_lineage_queue");
    when(OPEN_LINEAGE_INTAKE.enqueue(any(PreparedEvent.class))).thenThrow(databaseFailure);

    try (Response response =
        UNDER_TEST
            .target("/api/v1/lineage")
            .request()
            .post(Entity.entity(event, MediaType.APPLICATION_JSON_TYPE))) {
      assertEquals(500, response.getStatus());
      assertFalse(response.getHeaders().containsKey("Retry-After"));
      String body = response.readEntity(String.class);
      assertTrue(body.contains("Internal Server Error"));
      assertFalse(body.contains("password=do-not-return"));
      assertFalse(body.contains("insert into open_lineage_queue"));
    }
  }

  @Test
  void testCreateUnsupportedEventRetainsLegacyResponse() {
    BaseEvent event = new BaseEvent();

    Response response = RESOURCE.create(event);

    assertEquals(200, response.getStatus());
    assertSame(event, response.getEntity());
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateBatch() throws IOException {
    BaseEvent lineageEvent = eventFrom("/open_lineage/event_required_only.json");
    BaseEvent datasetEvent = eventFrom("/open_lineage/event_dataset_event.json");
    BaseEvent jobEvent = eventFrom("/open_lineage/event_job_event.json");

    Response response = RESOURCE.createBatch(List.of(lineageEvent, datasetEvent, jobEvent));

    assertEquals(204, response.getStatus());
    assertFalse(response.hasEntity());
    verify(OPEN_LINEAGE_INTAKE)
        .enqueueAll(
            List.of(
                OpenLineageQueueDao.prepare(lineageEvent),
                OpenLineageQueueDao.prepare(datasetEvent),
                OpenLineageQueueDao.prepare(jobEvent)));
    verifyNoMoreInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateBatchPreparesEveryEventBeforeIntake() throws IOException {
    BaseEvent valid = eventFrom("/open_lineage/event_required_only.json");
    LineageEvent invalid = (LineageEvent) eventFrom("/open_lineage/event_simple.json");
    invalid.getInputs().get(0).setNamespace(INVALID_NAMESPACE);

    Response response = RESOURCE.createBatch(List.of(valid, invalid));

    assertEquals(400, response.getStatus());
    assertFalse(response.hasEntity());
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateBatchRejectsUnsupportedEvent() throws IOException {
    BaseEvent valid = eventFrom("/open_lineage/event_required_only.json");

    Response response = RESOURCE.createBatch(List.of(valid, new BaseEvent()));

    assertEquals(400, response.getStatus());
    assertFalse(response.hasEntity());
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateBatchEmptyArrayFailsValidationBeforeIntake() {
    try (Response response = postBatch("[]")) {
      assertEquals(422, response.getStatus());
    }
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateBatchTooLargeFailsValidationBeforeIntake() throws IOException {
    String event = eventJsonFrom("/open_lineage/event_required_only.json");
    String batch =
        "["
            + String.join(",", Collections.nCopies(OpenLineageResource.MAX_BATCH_SIZE + 1, event))
            + "]";

    try (Response response = postBatch(batch)) {
      assertEquals(422, response.getStatus());
    }
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateBatchNullEventFailsValidationBeforeIntake() throws IOException {
    String event = eventJsonFrom("/open_lineage/event_required_only.json");

    try (Response response = postBatch("[" + event + ",null]")) {
      assertEquals(422, response.getStatus());
    }
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateBatchNestedValidationFailureBeforeIntake() throws IOException {
    String valid = eventJsonFrom("/open_lineage/event_required_only.json");
    String invalid =
        "{\"eventTime\": \"2021-11-03T10:53:52.427343Z\", \"eventType\": \"COMPLETE\", "
            + "\"inputs\": [], \"job\": {\"name\": \"child\", \"namespace\": \"namespace\"}, "
            + "\"outputs\": [], \"producer\": \"me\", "
            + "\"run\": {\"runId\": \"dae0d60a-6010-4c37-980e-c5270f5a6be4\", "
            + "\"facets\": {\"parent\": {\"_producer\": \"https://me\", "
            + "\"_schemaURL\": \"https://me\", \"run\": {\"runId\": \"\u2003\u00a0\"}, "
            + "\"job\": {\"namespace\": \"parent-ns\", \"name\": \"parent\"}}}}}";

    try (Response response = postBatch("[" + valid + "," + invalid + "]")) {
      assertEquals(422, response.getStatus());
    }
    verifyNoInteractions(OPEN_LINEAGE_INTAKE);
  }

  @Test
  void testCreateBatchAdmissionIllegalArgumentReturnsInternalServerError() throws IOException {
    String event = eventJsonFrom("/open_lineage/event_required_only.json");
    when(OPEN_LINEAGE_INTAKE.enqueueAll(anyList()))
        .thenThrow(new IllegalArgumentException("password=do-not-return"));

    try (Response response = postBatch("[" + event + "]")) {
      assertEquals(500, response.getStatus());
      assertFalse(response.getHeaders().containsKey("Retry-After"));
      assertFalse(response.readEntity(String.class).contains("password=do-not-return"));
    }
    verify(OPEN_LINEAGE_INTAKE).enqueueAll(anyList());
  }

  @Test
  void testGetLineage() {
    final Lineage lineage =
        UNDER_TEST
            .target("/api/v1/lineage")
            .queryParam("nodeId", "job:test-namespace:test-job")
            .request()
            .get()
            .readEntity(Lineage.class);

    assertEquals(lineage, LINEAGE);
  }

  @Test
  void testGetLineageEventsBadSort() {
    final Response response =
        UNDER_TEST
            .target("/api/v1/events/lineage")
            .queryParam("sortDirection", "asdf")
            .request()
            .get();

    assertEquals(response.getStatus(), 400);
  }

  private BaseEvent eventFrom(String resourceName) throws IOException {
    return Utils.newObjectMapper()
        .readValue(OpenLineageResourceTest.class.getResource(resourceName), BaseEvent.class);
  }

  private String eventJsonFrom(String resourceName) throws IOException {
    return new String(
        OpenLineageResourceTest.class.getResourceAsStream(resourceName).readAllBytes(),
        StandardCharsets.UTF_8);
  }

  private Response postBatch(String batch) {
    return UNDER_TEST
        .target("/api/v1/lineage/batch")
        .request()
        .post(Entity.entity(batch, MediaType.APPLICATION_JSON_TYPE));
  }
}
