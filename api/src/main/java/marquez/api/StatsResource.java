/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import io.dropwizard.jersey.errors.ErrorMessage;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.api.models.Period;
import marquez.service.InvalidStatsQueryException;
import marquez.service.ServiceFactory;
import marquez.service.StatsService;
import marquez.service.models.StatsQuery;
import marquez.service.models.StatsQuery.Metric;
import marquez.service.models.StatsQuery.Scope;

@Slf4j
@Path("/api/v1/stats")
public class StatsResource {

  private final StatsService statsService;

  public StatsResource(@NonNull final ServiceFactory serviceFactory) {
    this.statsService = serviceFactory.getStatsService();
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/query")
  public Response query(
      @QueryParam("metric") String metric,
      @QueryParam("scope") String scope,
      @QueryParam("namespace") String namespace,
      @QueryParam("jobName") String jobName,
      @QueryParam("runId") String runId,
      @QueryParam("startAt") String startAt,
      @QueryParam("endAt") String endAt,
      @QueryParam("rollup") String rollup) {
    try {
      StatsQuery query =
          new StatsQuery(
              parseRequiredMetric(metric),
              parseOptionalScope(scope),
              namespace,
              jobName,
              parseUuid(runId),
              parseInstant("startAt", startAt),
              parseInstant("endAt", endAt),
              parseDuration(rollup));
      return Response.ok(statsService.query(query)).build();
    } catch (InvalidStatsQueryException invalidQuery) {
      return Response.status(BAD_REQUEST)
          .type(APPLICATION_JSON_TYPE)
          .entity(new ErrorMessage(BAD_REQUEST.getStatusCode(), invalidQuery.getMessage()))
          .build();
    }
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/lineage-events")
  public Response getStats(
      @QueryParam("period") Period period, @QueryParam("timezone") String timezone) {

    // Check if the period is WEEK and timezone is missing
    if (Period.WEEK.equals(period) && (timezone == null || timezone.isEmpty())) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("Timezone must be specified for period 'WEEK'")
          .build();
    }

    return (Period.DAY.equals(period)
        ? Response.ok(statsService.getLastDayLineageMetrics()).build()
        : Period.WEEK.equals(period)
            ? Response.ok(statsService.getLastWeekLineageMetrics(timezone)).build()
            : Response.status(Response.Status.BAD_REQUEST).entity("Invalid period").build());
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/jobs")
  public Response getJobs(
      @QueryParam("period") Period period, @QueryParam("timezone") String timezone) {

    return (Period.DAY.equals(period)
        ? Response.ok(statsService.getLastDayJobs()).build()
        : Period.WEEK.equals(period)
            ? Response.ok(statsService.getLastWeekJobs(timezone)).build()
            : Response.status(Response.Status.BAD_REQUEST).entity("Invalid period").build());
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/datasets")
  public Response getDatasets(
      @QueryParam("period") Period period, @QueryParam("timezone") String timezone) {

    return (Period.DAY.equals(period)
        ? Response.ok(statsService.getLastDayDatasets()).build()
        : Period.WEEK.equals(period)
            ? Response.ok(statsService.getLastWeekDatasets(timezone)).build()
            : Response.status(Response.Status.BAD_REQUEST).entity("Invalid period").build());
  }

  @Timed
  @ResponseMetered
  @ExceptionMetered
  @GET
  @Produces(APPLICATION_JSON)
  @Path("/sources")
  public Response getSources(
      @QueryParam("period") Period period, @QueryParam("timezone") String timezone) {

    return (Period.DAY.equals(period)
        ? Response.ok(statsService.getLastDaySources()).build()
        : Period.WEEK.equals(period)
            ? Response.ok(statsService.getLastWeekSources(timezone)).build()
            : Response.status(Response.Status.BAD_REQUEST).entity("Invalid period").build());
  }

  private static Metric parseRequiredMetric(String value) {
    if (value == null || value.isEmpty()) {
      throw invalid("metric is required");
    }
    try {
      return Metric.valueOf(value);
    } catch (IllegalArgumentException error) {
      throw invalid("Invalid metric: " + value, error);
    }
  }

  private static Scope parseOptionalScope(String value) {
    if (value == null) {
      return null;
    }
    try {
      return Scope.valueOf(value);
    } catch (IllegalArgumentException error) {
      throw invalid("Invalid scope: " + value, error);
    }
  }

  private static UUID parseUuid(String value) {
    if (value == null) {
      return null;
    }
    try {
      UUID parsed = UUID.fromString(value);
      if (!parsed.toString().equalsIgnoreCase(value)) {
        throw new IllegalArgumentException();
      }
      return parsed;
    } catch (IllegalArgumentException error) {
      throw invalid("Invalid runId: " + value, error);
    }
  }

  private static Instant parseInstant(String name, String value) {
    if (value == null) {
      return null;
    }
    try {
      return OffsetDateTime.parse(value).toInstant();
    } catch (DateTimeParseException error) {
      throw invalid("Invalid " + name + ": " + value, error);
    }
  }

  private static Duration parseDuration(String value) {
    if (value == null) {
      return null;
    }
    try {
      return Duration.parse(value);
    } catch (DateTimeParseException error) {
      throw invalid("Invalid rollup: " + value, error);
    }
  }

  private static InvalidStatsQueryException invalid(String message) {
    return new InvalidStatsQueryException(message);
  }

  private static InvalidStatsQueryException invalid(String message, Throwable cause) {
    return new InvalidStatsQueryException(message, cause);
  }
}
