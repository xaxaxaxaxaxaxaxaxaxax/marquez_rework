/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CREATED;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.jersey.jsr310.ZonedDateTimeParam;
import java.util.Collections;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import marquez.api.models.SortDirection;
import marquez.common.models.RunId;
import marquez.db.OpenLineageDao;
import marquez.db.OpenLineageQueueDao;
import marquez.db.OpenLineageQueueDao.PreparedAdmission;
import marquez.db.OpenLineageQueueDao.PreparedEvent;
import marquez.service.OpenLineageIntake;
import marquez.service.ServiceFactory;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.JobEvent;
import marquez.service.models.LineageEvent;
import marquez.service.models.NodeId;

@Slf4j
@Path("/api/v1")
public class OpenLineageResource extends BaseResource {
  private static final String DEFAULT_DEPTH = "20";
  static final int MAX_BATCH_SIZE = OpenLineageQueueDao.MAX_ADMISSION_EVENTS;

  private final OpenLineageDao openLineageDao;
  private final OpenLineageIntake openLineageIntake;

  public OpenLineageResource(
      @NonNull final ServiceFactory serviceFactory,
      @NonNull final OpenLineageDao openLineageDao,
      @NonNull final OpenLineageIntake openLineageIntake) {
    super(serviceFactory);
    this.openLineageDao = openLineageDao;
    this.openLineageIntake = openLineageIntake;
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @POST
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  @Path("/lineage")
  public Response create(@Valid @NotNull BaseEvent event) {
    if (!isSupported(event)) {
      log.warn("Unsupported event type {}. Skipping without error", event.getClass().getName());
      return Response.status(200).entity(event).build();
    }

    final PreparedEvent prepared;
    try {
      prepared = prepare(event);
    } catch (IllegalArgumentException invalidArgument) {
      log.warn("Invalid OpenLineage event: {}", invalidArgument.getMessage());
      return Response.status(BAD_REQUEST).build();
    }

    openLineageIntake.enqueue(prepared);
    return Response.status(CREATED).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @POST
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  @Path("/lineage/batch")
  public Response createBatch(
      @NotNull @Size(min = 1, max = MAX_BATCH_SIZE) List<@NotNull @Valid BaseEvent> events) {
    final PreparedAdmission preparedAdmission;
    try {
      for (BaseEvent event : events) {
        if (!isSupported(event)) {
          log.warn("Unsupported batch event type {}", event.getClass().getName());
          return Response.status(BAD_REQUEST).build();
        }
        validateInputDatasetIds(event);
      }
      preparedAdmission = OpenLineageQueueDao.prepareAll(events);
    } catch (IllegalArgumentException invalidArgument) {
      log.warn("Invalid OpenLineage batch: {}", invalidArgument.getMessage());
      return Response.status(BAD_REQUEST).build();
    }

    openLineageIntake.enqueueAll(preparedAdmission);
    return Response.noContent().build();
  }

  private static boolean isSupported(BaseEvent event) {
    return event instanceof LineageEvent
        || event instanceof DatasetEvent
        || event instanceof JobEvent;
  }

  private static PreparedEvent prepare(BaseEvent event) {
    PreparedEvent prepared = OpenLineageQueueDao.prepare(event);
    validateInputDatasetIds(event);
    return prepared;
  }

  private static void validateInputDatasetIds(BaseEvent event) {
    if (event instanceof LineageEvent lineageEvent) {
      OpenLineageDao.validateDatasetIds(lineageEvent.getInputs());
    } else if (event instanceof JobEvent jobEvent) {
      OpenLineageDao.validateDatasetIds(jobEvent.getInputs());
    }
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  @Path("/lineage")
  public Response getLineage(
      @QueryParam("nodeId") @NotNull NodeId nodeId,
      @QueryParam("depth") @DefaultValue(DEFAULT_DEPTH) int depth) {
    throwIfNotExists(nodeId);
    return Response.ok(lineageService.lineage(nodeId, depth)).build();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Path("/events/lineage")
  @Produces(APPLICATION_JSON)
  public Response getLineageEvents(
      @QueryParam("before") @DefaultValue("2030-01-01T00:00:00+00:00") ZonedDateTimeParam before,
      @QueryParam("after") @DefaultValue("1970-01-01T00:00:00+00:00") ZonedDateTimeParam after,
      @QueryParam("sortDirection") @DefaultValue("desc") SortDirection sortDirection,
      @QueryParam("limit") @DefaultValue("100") @Min(value = 0) int limit,
      @QueryParam("offset") @DefaultValue("0") @Min(value = 0) int offset) {
    List<LineageEvent> events = Collections.emptyList();
    switch (sortDirection) {
      case DESC ->
          events = openLineageDao.getAllLineageEventsDesc(before.get(), after.get(), limit, offset);
      case ASC ->
          events = openLineageDao.getAllLineageEventsAsc(before.get(), after.get(), limit, offset);
    }
    int totalCount = openLineageDao.getAllLineageTotalCount(before.get(), after.get());
    return Response.ok(new Events(events, totalCount)).build();
  }

  /**
   * Returns the upstream lineage for a given run. Recursively: run -> dataset version it read from
   * -> the run that produced it
   *
   * @param runId the run to get upstream lineage from
   * @param depth the maximum depth of the upstream lineage
   * @return the upstream lineage for that run up to `detph` levels
   */
  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Consumes(APPLICATION_JSON)
  @Produces(APPLICATION_JSON)
  @Path("/runlineage/upstream")
  public Response getRunLineageUpstream(
      @QueryParam("runId") @NotNull RunId runId,
      @QueryParam("depth") @DefaultValue(DEFAULT_DEPTH) int depth) {
    throwIfNotExists(runId);
    return Response.ok(lineageService.upstream(runId, depth)).build();
  }

  @Value
  static class Events {
    @NonNull
    @JsonProperty("events")
    List<LineageEvent> value;

    int totalCount;
  }
}
