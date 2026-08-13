/*
 * Copyright 2018-2023 contributors to the Marquez project
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OpenLineageOpenApiContractTest {
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
  private static final List<String> REQUEST_SCHEMAS =
      List.of(
          "OpenLineageEventRequest",
          "OpenLineageRunEventRequest",
          "OpenLineageDatasetEventRequest",
          "OpenLineageJobEventRequest",
          "OpenLineageRunRequest",
          "OpenLineageRunFacetsRequest",
          "OpenLineageParentRunFacetRequest",
          "OpenLineageRunLinkRequest",
          "OpenLineageJobRequest",
          "OpenLineageDatasetRequest",
          "OpenLineageIdentityRequest");

  @Test
  void publicAndDocumentationContractsStaySynchronized() throws Exception {
    JsonNode publicSpec = readRepoYaml("spec/openapi.yml");
    JsonNode documentationSpec = readRepoYaml("docs/openapi.yml");

    assertThat(documentationSpec.at("/paths/~1lineage/post/requestBody"))
        .isEqualTo(publicSpec.at("/paths/~1lineage/post/requestBody"));
    assertThat(documentationSpec.at("/paths/~1lineage/post/description"))
        .isEqualTo(publicSpec.at("/paths/~1lineage/post/description"));
    assertThat(documentationSpec.at("/paths/~1lineage/post/responses"))
        .isEqualTo(publicSpec.at("/paths/~1lineage/post/responses"));
    for (String schemaName : REQUEST_SCHEMAS) {
      assertThat(documentationSpec.at("/components/schemas/" + schemaName))
          .as(schemaName)
          .isEqualTo(publicSpec.at("/components/schemas/" + schemaName));
    }
  }

  @Test
  void requestUnionPreservesSupportedVariantsAndHttpResponses() throws Exception {
    JsonNode spec = readRepoYaml("spec/openapi.yml");
    JsonNode post = spec.at("/paths/~1lineage/post");

    assertThat(post.at("/requestBody/required").asBoolean()).isTrue();
    assertThat(post.at("/requestBody/content/application~1json/schema/$ref").asText())
        .isEqualTo("#/components/schemas/OpenLineageEventRequest");
    assertThat(fieldNames(post.path("responses")))
        .containsExactlyInAnyOrder("201", "400", "422", "500");
    String operationDescription = post.path("description").asText();
    assertThat(operationDescription)
        .contains(
            "Acceptance confirms only that the event was committed to the intake queue",
            "Search indexing and run-transition listener delivery happen after the queue item is acknowledged",
            "best effort",
            "no durable delivery guarantee",
            "may be missing, concurrent, or out of order",
            "not retried by the intake queue",
            "Any 5xx response during queue admission is commit-indeterminate",
            "may already have committed even if Marquez did not observe the commit result",
            "Clients that retry must tolerate duplicate events");
    assertThat(post.at("/responses/201/description").asText())
        .isEqualTo(
            "Empty response confirming only that the lineage event was committed to the durable intake queue");
    assertThat(post.at("/responses/500/description").asText())
        .contains(
            "commit outcome is indeterminate",
            "may already have been committed",
            "a retry can submit a duplicate");

    JsonNode union = schema(spec, "OpenLineageEventRequest");
    assertThat(union.has("discriminator")).isFalse();
    assertThat(textValues(union.path("oneOf"), "$ref"))
        .containsExactly(
            "#/components/schemas/OpenLineageRunEventRequest",
            "#/components/schemas/OpenLineageDatasetEventRequest",
            "#/components/schemas/OpenLineageJobEventRequest");

    JsonNode runEvent = schema(spec, "OpenLineageRunEventRequest");
    assertThat(textValues(runEvent.path("required"), null))
        .containsExactlyInAnyOrder("eventTime", "run", "job", "producer")
        .doesNotContain("schemaURL");
    assertThat(runEvent.at("/properties/schemaURL/nullable").asBoolean()).isTrue();
    assertThat(runEvent.path("example").has("schemaURL")).isFalse();
    assertExampleContainsRequired(runEvent);

    JsonNode datasetEvent = schema(spec, "OpenLineageDatasetEventRequest");
    assertThat(textValues(datasetEvent.path("required"), null))
        .containsExactlyInAnyOrder("eventTime", "dataset", "producer", "schemaURL");
    assertThat(datasetEvent.at("/properties/schemaURL/pattern").asText())
        .isEqualTo("/DatasetEvent$");
    assertExampleContainsRequired(datasetEvent);

    JsonNode jobEvent = schema(spec, "OpenLineageJobEventRequest");
    assertThat(textValues(jobEvent.path("required"), null))
        .containsExactlyInAnyOrder("eventTime", "job", "producer", "schemaURL");
    assertThat(jobEvent.at("/properties/schemaURL/pattern").asText()).isEqualTo("/JobEvent$");
    assertExampleContainsRequired(jobEvent);

    JsonNode runFacets = schema(spec, "OpenLineageRunFacetsRequest");
    assertThat(runFacets.path("additionalProperties").asBoolean()).isTrue();
    assertThat(runFacets.at("/properties/parent/$ref").asText())
        .isEqualTo("#/components/schemas/OpenLineageParentRunFacetRequest");
    assertThat(runFacets.at("/properties/parentRun/$ref").asText())
        .isEqualTo("#/components/schemas/OpenLineageParentRunFacetRequest");
    JsonNode parentFacet = schema(spec, "OpenLineageParentRunFacetRequest");
    assertThat(textValues(parentFacet.path("required"), null))
        .containsExactlyInAnyOrder("_producer", "_schemaURL", "run", "job");
    assertThat(parentFacet.at("/properties/_producer/format").asText()).isEqualTo("uri");
    assertThat(parentFacet.at("/properties/_schemaURL/format").asText()).isEqualTo("uri");
    assertThat(parentFacet.at("/properties/run/$ref").asText())
        .isEqualTo("#/components/schemas/OpenLineageRunLinkRequest");
    assertThat(parentFacet.at("/properties/job/$ref").asText())
        .isEqualTo("#/components/schemas/OpenLineageJobRequest");
    JsonNode runLink = schema(spec, "OpenLineageRunLinkRequest");
    assertThat(textValues(runLink.path("required"), null)).containsExactly("runId");
    assertIdentityProperty(runLink, "runId");
    JsonNode job = schema(spec, "OpenLineageJobRequest");
    assertThat(job.path("additionalProperties").asBoolean()).isTrue();
    assertThat(textValues(job.path("required"), null))
        .containsExactlyInAnyOrder("namespace", "name");
    assertIdentityProperty(job, "namespace");
    assertIdentityProperty(job, "name");
    JsonNode dataset = schema(spec, "OpenLineageDatasetRequest");
    assertThat(dataset.path("additionalProperties").asBoolean()).isTrue();
    assertThat(textValues(dataset.path("required"), null))
        .containsExactlyInAnyOrder("namespace", "name");
    assertIdentityProperty(dataset, "namespace");
    assertIdentityProperty(dataset, "name");

    JsonNode identity = schema(spec, "OpenLineageIdentityRequest");
    assertThat(identity.path("minLength").asInt()).isEqualTo(1);
    assertThat(identity.has("pattern")).isFalse();
    assertThat(identity.path("description").asText()).contains("OpenAPI 3.0 cannot express");

    String generatedPage =
        Files.readString(repoRoot().resolve("docs/docs/api/record-lineage.api.mdx"));
    assertThat(generatedPage)
        .doesNotContain(">any</ul>")
        .contains(
            operationDescription,
            post.at("/responses/201/description").asText(),
            "OpenLineageRunEventRequest",
            "OpenLineageDatasetEventRequest",
            "OpenLineageJobEventRequest",
            "Acceptance confirms only that the event was committed to the intake queue",
            "no durable delivery guarantee",
            "may be missing, concurrent, or out of order",
            "not retried by the intake queue",
            "Any 5xx response during queue admission is commit-indeterminate",
            "Clients that retry must tolerate duplicate events",
            post.at("/responses/500/description").asText());
  }

  private static void assertExampleContainsRequired(JsonNode schema) {
    assertThat(fieldNames(schema.path("example")))
        .containsAll(textValues(schema.path("required"), null));
  }

  private static void assertIdentityProperty(JsonNode schema, String property) {
    assertThat(schema.at("/properties/" + property + "/$ref").asText())
        .isEqualTo("#/components/schemas/OpenLineageIdentityRequest");
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

  private static List<String> textValues(JsonNode array, String field) {
    java.util.ArrayList<String> values = new java.util.ArrayList<>();
    for (JsonNode value : array) {
      values.add(field == null ? value.asText() : value.path(field).asText());
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
