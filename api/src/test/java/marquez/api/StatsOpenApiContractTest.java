/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StatsOpenApiContractTest {
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  @Test
  void queryContractIsTypedBoundedAndExplicit() throws Exception {
    JsonNode spec = readRepoYaml("spec/openapi.yml");
    JsonNode get = spec.at("/paths/~1stats~1query/get");

    assertThat(get.path("operationId").asText()).isEqualTo("queryStats");
    assertThat(parameterNames(get.path("parameters")))
        .containsExactly(
            "metric", "scope", "namespace", "jobName", "runId", "startAt", "endAt", "rollup");
    assertThat(parameter(get, "metric").path("required").asBoolean()).isTrue();
    assertThat(parameter(get, "metric").at("/schema/$ref").asText())
        .isEqualTo("#/components/schemas/StatsMetric");
    assertThat(parameter(get, "scope").at("/schema/allOf/0/$ref").asText())
        .isEqualTo("#/components/schemas/StatsScope");
    assertThat(parameter(get, "scope").at("/schema/default").asText()).isEqualTo("GLOBAL");
    assertThat(parameter(get, "rollup").at("/schema/default").asText()).isEqualTo("PT1H");
    assertThat(parameter(get, "namespace").at("/schema/maxLength").asInt()).isEqualTo(1024);
    assertThat(parameter(get, "jobName").at("/schema/maxLength").asInt()).isEqualTo(1024);
    assertThat(parameter(get, "runId").at("/schema/format").asText()).isEqualTo("uuid");
    assertThat(parameter(get, "startAt").at("/schema/format").asText()).isEqualTo("date-time");
    assertThat(parameter(get, "endAt").at("/schema/format").asText()).isEqualTo("date-time");

    assertThat(textValues(schema(spec, "StatsMetric").path("enum")))
        .containsExactly(
            "LINEAGE_EVENTS_START",
            "LINEAGE_EVENTS_COMPLETE",
            "LINEAGE_EVENTS_FAIL",
            "LINEAGE_EVENTS_ABORT",
            "JOBS_TOTAL",
            "DATASETS_TOTAL",
            "SOURCES_TOTAL");
    assertThat(textValues(schema(spec, "StatsScope").path("enum")))
        .containsExactly("GLOBAL", "NAMESPACE", "JOB", "RUN");

    JsonNode result = schema(spec, "StatsQueryResult");
    assertThat(textValues(result.path("required")))
        .containsExactlyInAnyOrder("metric", "scope", "startAt", "endAt", "rollup", "points");
    assertThat(result.at("/properties/points/minItems").asInt()).isEqualTo(1);
    assertThat(result.at("/properties/points/maxItems").asInt()).isEqualTo(1000);
    assertThat(result.at("/properties/points/items/$ref").asText())
        .isEqualTo("#/components/schemas/StatsPoint");

    JsonNode point = schema(spec, "StatsPoint");
    assertThat(textValues(point.path("required")))
        .containsExactlyInAnyOrder("startInterval", "endInterval", "value");
    assertThat(point.at("/properties/value/format").asText()).isEqualTo("int64");
    assertThat(point.at("/properties/value/minimum").asLong()).isZero();

    assertThat(fieldNames(get.path("responses"))).containsExactlyInAnyOrder("200", "400", "500");
    assertThat(get.at("/responses/200/content/application~1json/schema/$ref").asText())
        .isEqualTo("#/components/schemas/StatsQueryResult");
    assertThat(get.at("/responses/400/content/application~1json/schema/$ref").asText())
        .isEqualTo("#/components/schemas/StatsError");

    assertThat(get.path("description").asText())
        .contains(
            "half-open `[startAt, endAt)`",
            "fixed elapsed-time rollups anchored at `startAt`",
            "between `PT1M` and `P30D`",
            "whole-millisecond precision",
            "whole-microsecond precision",
            "366 elapsed days or 1000 buckets",
            "source totals support only `GLOBAL`",
            "producer-reported `event_time`",
            "exact, case-sensitive reported identities",
            "stored `run_uuid`",
            "committed raw events are visible",
            "queue admission alone does not count",
            "Duplicate submissions count independently",
            "late arrivals can revise earlier buckets",
            "old ranges indistinguishable from zero",
            "cumulative count",
            "currently retained physical rows",
            "`created_at < bucket end`",
            "including hidden, deleted, or symlink rows",
            "deletion and visibility history is not reconstructed",
            "no timezone or calendar-day semantics");
  }

  private static JsonNode parameter(JsonNode operation, String name) {
    for (JsonNode parameter : operation.path("parameters")) {
      if (name.equals(parameter.path("name").asText())) {
        return parameter;
      }
    }
    throw new AssertionError("Missing parameter: " + name);
  }

  private static List<String> parameterNames(JsonNode parameters) {
    List<String> names = new ArrayList<>();
    for (JsonNode parameter : parameters) {
      names.add(parameter.path("name").asText());
    }
    return names;
  }

  private static JsonNode schema(JsonNode spec, String name) {
    JsonNode schema = spec.at("/components/schemas/" + name);
    assertThat(schema.isMissingNode()).as(name).isFalse();
    return schema;
  }

  private static Set<String> fieldNames(JsonNode object) {
    Set<String> names = new LinkedHashSet<>();
    object.fieldNames().forEachRemaining(names::add);
    return names;
  }

  private static List<String> textValues(JsonNode array) {
    List<String> values = new ArrayList<>();
    for (JsonNode value : array) {
      values.add(value.asText());
    }
    return values;
  }

  private static JsonNode readRepoYaml(String relativePath) throws IOException {
    try (java.io.Reader reader = Files.newBufferedReader(repoRoot().resolve(relativePath))) {
      return YAML.readTree(reader);
    }
  }

  private static Path repoRoot() throws IOException {
    Path root = Path.of("").toAbsolutePath();
    while (root != null && !Files.exists(root.resolve("settings.gradle"))) {
      root = root.getParent();
    }
    if (root == null) {
      throw new IOException("Could not locate repository root");
    }
    return root;
  }
}
