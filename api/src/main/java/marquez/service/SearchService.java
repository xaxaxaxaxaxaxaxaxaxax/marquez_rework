/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import marquez.search.SearchConfig;
import marquez.service.models.LineageEvent;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.query_dsl.Operator;
import org.opensearch.client.opensearch._types.query_dsl.TextQueryType;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.search.BuiltinHighlighterType;
import org.opensearch.client.opensearch.core.search.HighlighterType;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.client.transport.rest_client.RestClientTransport;

@Slf4j
public class SearchService {

  String[] DATASET_FIELDS = {
    "run_id",
    "name",
    "namespace",
    "facets.schema.fields.name",
    "facets.schema.fields.type",
    "facets.columnLineage.fields.*.inputFields.name",
    "facets.columnLineage.fields.*.inputFields.namespace",
    "facets.columnLineage.fields.*.inputFields.field",
    "facets.columnLineage.fields.*.transformationDescription",
    "facets.columnLineage.fields.*.transformationType"
  };

  String[] JOB_FIELDS = {
    "facets.sql.query",
    "facets.sourceCode.sourceCode",
    "facets.sourceCode.language",
    "runFacets.processing_engine.name",
    "run_id",
    "name",
    "namespace",
    "type"
  };

  private final OpenSearchClient openSearchClient;
  private final SearchConfig searchConfig;

  public SearchService(SearchConfig searchConfig) {
    this.searchConfig = searchConfig;
    if (!searchConfig.isEnabled()) {
      log.info("Search is disabled, skipping initialization");
      this.openSearchClient = null;
      return;
    }
    final HttpHost host =
        new HttpHost(searchConfig.getHost(), searchConfig.getPort(), searchConfig.getScheme());
    final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        new AuthScope(host),
        new UsernamePasswordCredentials(searchConfig.getUsername(), searchConfig.getPassword()));
    final RestClient restClient =
        RestClient.builder(host)
            .setHttpClientConfigCallback(
                httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
            .build();

    JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();
    // register JavaTimeModule to handle ZonedDateTime
    jsonpMapper.objectMapper().registerModule(new JavaTimeModule());
    final OpenSearchTransport transport = new RestClientTransport(restClient, jsonpMapper);
    this.openSearchClient = new OpenSearchClient(transport);
    BooleanResponse booleanResponse;
    try {
      booleanResponse = openSearchClient.ping();
      log.info("OpenSearch Active: {}", booleanResponse.value());
    } catch (IOException e) {
      log.warn("Search not configured");
    }
  }

  SearchService(SearchConfig searchConfig, OpenSearchClient openSearchClient) {
    this.searchConfig = searchConfig;
    this.openSearchClient = openSearchClient;
  }

  public OpenSearchClient getClient() {
    return this.openSearchClient;
  }

  public SearchResponse<ObjectNode> searchDatasets(String query) throws IOException {
    return this.openSearchClient.search(
        s ->
            s.index("datasets")
                .query(
                    q ->
                        q.multiMatch(
                            m ->
                                m.query(query)
                                    .type(TextQueryType.PhrasePrefix)
                                    .fields(Arrays.stream(DATASET_FIELDS).toList())
                                    .operator(Operator.Or)))
                .highlight(
                    hl -> {
                      for (String field : DATASET_FIELDS) {
                        hl.fields(
                            field,
                            f ->
                                f.type(
                                    HighlighterType.of(
                                        fn -> fn.builtin(BuiltinHighlighterType.Plain))));
                      }
                      return hl;
                    }),
        ObjectNode.class);
  }

  public SearchResponse<ObjectNode> searchJobs(String query) throws IOException {
    return this.openSearchClient.search(
        s -> {
          s.index("jobs")
              .query(
                  q ->
                      q.multiMatch(
                          m ->
                              m.query(query)
                                  .type(TextQueryType.PhrasePrefix)
                                  .fields(Arrays.stream(JOB_FIELDS).toList())
                                  .operator(Operator.Or)));
          s.highlight(
              hl -> {
                for (String field : JOB_FIELDS) {
                  hl.fields(
                      field,
                      f ->
                          f.type(
                              HighlighterType.of(fn -> fn.builtin(BuiltinHighlighterType.Plain))));
                }
                return hl;
              });
          return s;
        },
        ObjectNode.class);
  }

  public void indexEvent(@Valid @NotNull LineageEvent event) {
    if (!searchConfig.isEnabled()) {
      log.debug("Search is disabled, skipping indexing");
      return;
    }
    UUID runUuid = runUuidFromEvent(event.getRun());
    log.debug("Indexing event for run {}", runUuid);

    int operationCount = 1;
    if (event.getInputs() != null) {
      operationCount += event.getInputs().size();
    }
    if (event.getOutputs() != null) {
      operationCount += event.getOutputs().size();
    }

    List<BulkOperation> operations = new ArrayList<>(operationCount);
    addDatasetOperations(operations, event.getInputs(), runUuid, event);
    addDatasetOperations(operations, event.getOutputs(), runUuid, event);
    operations.add(buildJobIndexOperation(runUuid, event));

    try {
      BulkResponse response = openSearchClient.bulk(BulkRequest.of(b -> b.operations(operations)));
      throwIfBulkFailed(response);
    } catch (IOException e) {
      // Search is a best-effort side effect. Preserve intake when its transport is unavailable.
      log.error("Failed to index event; OpenSearch is not available.", e);
    }
  }

  private UUID runUuidFromEvent(LineageEvent.Run run) {
    UUID runUuid;
    try {
      runUuid = UUID.fromString(run.getRunId());
    } catch (Exception e) {
      runUuid = UUID.nameUUIDFromBytes(run.getRunId().getBytes(StandardCharsets.UTF_8));
    }
    return runUuid;
  }

  private Map<String, Object> buildJobIndexRequest(UUID runUuid, LineageEvent event) {
    Map<String, Object> jsonMap = new HashMap<>();

    jsonMap.put("run_id", runUuid.toString());
    jsonMap.put("eventType", event.getEventType());
    jsonMap.put("name", event.getJob().getName());
    jsonMap.put("type", event.getJob().isStreamingJob() ? "STREAM" : "BATCH");
    jsonMap.put("namespace", event.getJob().getNamespace());
    jsonMap.put("facets", event.getJob().getFacets());
    jsonMap.put("runFacets", event.getRun().getFacets());
    return jsonMap;
  }

  private Map<String, Object> buildDatasetIndexRequest(
      UUID runUuid, LineageEvent.Dataset dataset, LineageEvent event) {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("run_id", runUuid.toString());
    jsonMap.put("eventType", event.getEventType());
    jsonMap.put("name", dataset.getName());
    jsonMap.put("inputFacets", dataset.getInputFacets());
    jsonMap.put("outputFacets", dataset.getOutputFacets());
    jsonMap.put("namespace", dataset.getNamespace());
    jsonMap.put("facets", dataset.getFacets());
    return jsonMap;
  }

  private BulkOperation buildJobIndexOperation(UUID runUuid, LineageEvent event) {
    return BulkOperation.of(
        operation ->
            operation.index(
                index ->
                    index
                        .index("jobs")
                        .id(
                            String.format(
                                "JOB:%s:%s",
                                event.getJob().getNamespace(), event.getJob().getName()))
                        .document(buildJobIndexRequest(runUuid, event))));
  }

  private void addDatasetOperations(
      List<BulkOperation> operations,
      List<LineageEvent.Dataset> datasets,
      UUID runUuid,
      LineageEvent event) {
    if (datasets == null) {
      return;
    }
    for (LineageEvent.Dataset dataset : datasets) {
      Map<String, Object> document = buildDatasetIndexRequest(runUuid, dataset, event);
      operations.add(
          BulkOperation.of(
              operation ->
                  operation.index(
                      index ->
                          index
                              .index("datasets")
                              .id(
                                  String.format(
                                      "DATASET:%s:%s", dataset.getNamespace(), dataset.getName()))
                              .document(document))));
    }
  }

  private void throwIfBulkFailed(BulkResponse response) {
    if (!response.errors()) {
      return;
    }

    String failure =
        response.items().stream()
            .filter(item -> item.error() != null)
            .findFirst()
            .map(this::describeFailure)
            .orElse("unknown bulk item");
    throw new IllegalStateException("OpenSearch bulk indexing failed for " + failure);
  }

  private String describeFailure(BulkResponseItem item) {
    return String.format("%s/%s: %s", item.index(), item.id(), item.error().reason());
  }

  public boolean isEnabled() {
    return searchConfig.isEnabled();
  }
}
