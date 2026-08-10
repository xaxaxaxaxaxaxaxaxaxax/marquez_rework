/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSortedSet;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import marquez.common.Utils;
import marquez.db.OpenLineageDao;
import marquez.service.IntakeOverloadedException;
import marquez.service.JobService;
import marquez.service.LineageService;
import marquez.service.OpenLineageService;
import marquez.service.ServiceFactory;
import marquez.service.models.Lineage;
import marquez.service.models.LineageEvent;
import marquez.service.models.Node;
import marquez.service.models.NodeId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

@ExtendWith(DropwizardExtensionsSupport.class)
class OpenLineageResourceTest {
  private static ResourceExtension UNDER_TEST;
  private static OpenLineageResource RESOURCE;
  private static Lineage LINEAGE;
  private static OpenLineageService OPEN_LINEAGE_SERVICE;

  static {
    LineageService lineageService = mock(LineageService.class);
    OpenLineageDao openLineageDao = mock(OpenLineageDao.class);
    JobService jobService = mock(JobService.class);
    OPEN_LINEAGE_SERVICE = mock(OpenLineageService.class);
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
                OPEN_LINEAGE_SERVICE));

    RESOURCE = new OpenLineageResource(serviceFactory, openLineageDao);
    UNDER_TEST = ResourceExtension.builder().addResource(RESOURCE).build();
  }

  @BeforeEach
  void setUpOpenLineageService() {
    reset(OPEN_LINEAGE_SERVICE);
    when(OPEN_LINEAGE_SERVICE.createAsync(any(LineageEvent.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
  }

  @Test
  public void testCreateLineageEvent() throws IOException {
    Response response = postLineageEvent();

    assertEquals(201, response.getStatus());
  }

  @Test
  public void testCreateLineageEventBadRequest() throws IOException {
    when(OPEN_LINEAGE_SERVICE.createAsync(any(LineageEvent.class)))
        .thenReturn(CompletableFuture.failedFuture(new IllegalArgumentException("invalid")));

    assertEquals(400, postLineageEvent().getStatus());
  }

  @Test
  public void testCreateLineageEventInternalFailure() throws IOException {
    when(OPEN_LINEAGE_SERVICE.createAsync(any(LineageEvent.class)))
        .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("failed")));

    assertEquals(500, postLineageEvent().getStatus());
  }

  @Test
  public void testCreateLineageEventOverloaded() throws IOException {
    when(OPEN_LINEAGE_SERVICE.createAsync(any(LineageEvent.class)))
        .thenReturn(
            CompletableFuture.failedFuture(
                new IntakeOverloadedException(new RejectedExecutionException("full"))));

    Response response = postLineageEvent();

    assertEquals(503, response.getStatus());
    assertEquals("1", response.getHeaderString("Retry-After"));
  }

  @Test
  public void testGetLineage() {
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
  public void testGetLineageEventsBadSort() {
    final Response response =
        UNDER_TEST
            .target("/api/v1/events/lineage")
            .queryParam("sortDirection", "asdf")
            .request()
            .get();

    assertEquals(response.getStatus(), 400);
  }

  private Response postLineageEvent() throws IOException {
    String event =
        new String(
            OpenLineageResourceTest.class
                .getResourceAsStream("/open_lineage/event_required_only.json")
                .readAllBytes(),
            StandardCharsets.UTF_8);
    LineageEvent lineageEvent = Utils.newObjectMapper().readValue(event, LineageEvent.class);
    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    RESOURCE.create(lineageEvent, asyncResponse);

    ArgumentCaptor<Response> response = ArgumentCaptor.forClass(Response.class);
    verify(asyncResponse).resume(response.capture());
    return response.getValue();
  }
}
