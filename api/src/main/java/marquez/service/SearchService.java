/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.Utils;
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
import org.opensearch.client.opensearch.core.search.Highlight;
import org.opensearch.client.opensearch.core.search.HighlightField;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.client.transport.rest_client.RestClientTransport;

@Slf4j
public class SearchService {
  private static final Base64.Encoder ID_COMPONENT_ENCODER =
      Base64.getUrlEncoder().withoutPadding();

  private enum DocumentKind {
    DATASET(
        "datasets",
        "DATASET",
        List.of(
            "run_id",
            "name",
            "namespace",
            "facets.schema.fields.name",
            "facets.schema.fields.type",
            "facets.columnLineage.fields.*.inputFields.name",
            "facets.columnLineage.fields.*.inputFields.namespace",
            "facets.columnLineage.fields.*.inputFields.field",
            "facets.columnLineage.fields.*.transformationDescription",
            "facets.columnLineage.fields.*.transformationType")),
    JOB(
        "jobs",
        "JOB",
        List.of(
            "facets.sql.query",
            "facets.sourceCode.sourceCode",
            "facets.sourceCode.language",
            "runFacets.processing_engine.name",
            "run_id",
            "name",
            "namespace",
            "type"));

    private final String indexName;
    private final String idDomain;
    private final List<String> fields;
    private final Highlight highlight;

    DocumentKind(String indexName, String idDomain, List<String> fields) {
      this.indexName = indexName;
      this.idDomain = idDomain;
      this.fields = fields;
      HighlightField plainHighlight =
          HighlightField.of(
              field -> field.type(type -> type.builtin(BuiltinHighlighterType.Plain)));
      this.highlight =
          Highlight.of(
              builder -> {
                fields.forEach(field -> builder.fields(field, plainHighlight));
                return builder;
              });
    }
  }

  record IndexEntry(LineageEvent event, UUID effectiveRunUuid) {
    IndexEntry {
      Objects.requireNonNull(event, "event");
      Objects.requireNonNull(effectiveRunUuid, "effectiveRunUuid");
    }
  }

  @JsonInclude(JsonInclude.Include.ALWAYS)
  private record JobIndexDocument(
      @JsonProperty("run_id") String runId,
      String eventType,
      String name,
      String type,
      String namespace,
      LineageEvent.JobFacet facets,
      LineageEvent.RunFacet runFacets) {}

  @JsonInclude(JsonInclude.Include.ALWAYS)
  private record DatasetIndexDocument(
      @JsonProperty("run_id") String runId,
      String eventType,
      String name,
      LineageEvent.InputDatasetFacets inputFacets,
      LineageEvent.OutputDatasetFacets outputFacets,
      String namespace,
      LineageEvent.DatasetFacets facets) {}

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
    return search(DocumentKind.DATASET, query);
  }

  public SearchResponse<ObjectNode> searchJobs(String query) throws IOException {
    return search(DocumentKind.JOB, query);
  }

  private SearchResponse<ObjectNode> search(DocumentKind kind, String query) throws IOException {
    return this.openSearchClient.search(
        request ->
            request
                .index(kind.indexName)
                .query(
                    queryBuilder ->
                        queryBuilder.multiMatch(
                            multiMatch ->
                                multiMatch
                                    .query(query)
                                    .type(TextQueryType.PhrasePrefix)
                                    .fields(kind.fields)
                                    .operator(Operator.Or)))
                .highlight(kind.highlight),
        ObjectNode.class);
  }

  public boolean indexEvent(@Valid @NotNull LineageEvent event) {
    if (indexingDisabled()) {
      return true;
    }
    return indexEventWithRunUuid(event, runUuidFromEvent(event.getRun()));
  }

  /** Indexes a queued event with the run identity resolved by its committed projection. */
  boolean indexEvent(@Valid @NotNull LineageEvent event, @NotNull UUID effectiveRunUuid) {
    if (indexingDisabled()) {
      return true;
    }
    return indexEventWithRunUuid(event, effectiveRunUuid);
  }

  /** Best-effort indexing for an ordered batch of committed queued events. */
  int indexEventsBestEffort(@NotNull List<IndexEntry> orderedEntries) {
    Objects.requireNonNull(orderedEntries, "orderedEntries");
    if (indexingDisabled() || orderedEntries.isEmpty()) {
      return 0;
    }

    int entryCount = orderedEntries.size();
    List<BulkOperation> operations = new ArrayList<>();
    int[] operationEnds = new int[entryCount];
    int materializationFailures = 0;

    for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
      try {
        IndexEntry entry = orderedEntries.get(entryIndex);
        List<BulkOperation> entryOperations =
            buildIndexOperations(entry.event(), entry.effectiveRunUuid());
        operations.addAll(entryOperations);
      } catch (RuntimeException e) {
        materializationFailures++;
        log.error(
            "Failed to materialize OpenSearch operations for queued event at batch index {}",
            entryIndex,
            e);
      }
      operationEnds[entryIndex] = operations.size();
    }

    if (operations.isEmpty()) {
      return materializationFailures;
    }

    try {
      BulkResponse response = openSearchClient.bulk(BulkRequest.of(b -> b.operations(operations)));
      if (!response.errors()) {
        return materializationFailures;
      }

      List<BulkResponseItem> responseItems = response.items();
      if (responseItems == null || responseItems.size() != operations.size()) {
        log.error(
            "OpenSearch bulk response could not be aligned to queued events: expected {} items, "
                + "received {}",
            operations.size(),
            responseItems == null ? null : responseItems.size());
        return entryCount;
      }

      int itemFailures = 0;
      int entryIndex = 0;
      int lastFailedEntry = -1;
      for (int operationIndex = 0; operationIndex < responseItems.size(); operationIndex++) {
        while (operationIndex >= operationEnds[entryIndex]) {
          entryIndex++;
        }
        BulkResponseItem item = responseItems.get(operationIndex);
        if (item == null) {
          log.error("OpenSearch bulk response contained a null item");
          return entryCount;
        }
        if (entryIndex != lastFailedEntry && item.error() != null) {
          itemFailures++;
          lastFailedEntry = entryIndex;
          log.error(
              "OpenSearch bulk indexing failed for queued event at batch index {}: {}",
              entryIndex,
              describeFailure(item));
        }
      }

      if (itemFailures == 0) {
        log.error("OpenSearch bulk response reported errors without a failed item");
        return entryCount;
      }
      return materializationFailures + itemFailures;
    } catch (IOException | RuntimeException e) {
      log.error("Failed to bulk index queued OpenLineage events", e);
      return entryCount;
    }
  }

  private boolean indexingDisabled() {
    if (!searchConfig.isEnabled()) {
      log.debug("Search is disabled, skipping indexing");
      return true;
    }
    return false;
  }

  private boolean indexEventWithRunUuid(LineageEvent event, UUID runUuid) {
    log.debug("Indexing event for run {}", runUuid);

    List<BulkOperation> operations = buildIndexOperations(event, runUuid);

    try {
      BulkResponse response = openSearchClient.bulk(BulkRequest.of(b -> b.operations(operations)));
      throwIfBulkFailed(response);
      return true;
    } catch (IOException e) {
      // Search is a best-effort side effect. Preserve intake when its transport is unavailable.
      log.error("Failed to index event; OpenSearch is not available.", e);
      return false;
    }
  }

  private List<BulkOperation> buildIndexOperations(LineageEvent event, UUID runUuid) {
    List<LineageEvent.Dataset> inputs = Objects.requireNonNullElse(event.getInputs(), List.of());
    List<LineageEvent.Dataset> outputs = Objects.requireNonNullElse(event.getOutputs(), List.of());

    String runId = runUuid.toString();
    List<BulkOperation> operations = new ArrayList<>(1 + inputs.size() + outputs.size());
    addDatasetOperations(operations, inputs, runId, event);
    addDatasetOperations(operations, outputs, runId, event);
    operations.add(buildJobIndexOperation(runId, event));
    return operations;
  }

  private UUID runUuidFromEvent(LineageEvent.Run run) {
    return Utils.openLineageRunUuid(run.getRunId());
  }

  private JobIndexDocument buildJobIndexRequest(
      String runId, LineageEvent event, String canonicalNamespace) {
    LineageEvent.Job job = event.getJob();
    return new JobIndexDocument(
        runId,
        event.getEventType(),
        job.getName(),
        job.isStreamingJob() ? "STREAM" : "BATCH",
        canonicalNamespace,
        job.getFacets(),
        event.getRun().getFacets());
  }

  private DatasetIndexDocument buildDatasetIndexRequest(
      String runId, LineageEvent.Dataset dataset, LineageEvent event, String canonicalNamespace) {
    return new DatasetIndexDocument(
        runId,
        event.getEventType(),
        dataset.getName(),
        dataset.getInputFacets(),
        dataset.getOutputFacets(),
        canonicalNamespace,
        dataset.getFacets());
  }

  private BulkOperation buildJobIndexOperation(String runId, LineageEvent event) {
    String canonicalNamespace = Utils.sanitizeOpenLineageNamespace(event.getJob().getNamespace());
    return BulkOperation.of(
        operation ->
            operation.index(
                index ->
                    index
                        .index(DocumentKind.JOB.indexName)
                        .id(
                            indexDocumentId(
                                DocumentKind.JOB, canonicalNamespace, event.getJob().getName()))
                        .document(buildJobIndexRequest(runId, event, canonicalNamespace))));
  }

  private void addDatasetOperations(
      List<BulkOperation> operations,
      List<LineageEvent.Dataset> datasets,
      String runId,
      LineageEvent event) {
    for (LineageEvent.Dataset dataset : datasets) {
      String canonicalNamespace = Utils.sanitizeOpenLineageNamespace(dataset.getNamespace());
      DatasetIndexDocument document =
          buildDatasetIndexRequest(runId, dataset, event, canonicalNamespace);
      operations.add(
          BulkOperation.of(
              operation ->
                  operation.index(
                      index ->
                          index
                              .index(DocumentKind.DATASET.indexName)
                              .id(
                                  indexDocumentId(
                                      DocumentKind.DATASET, canonicalNamespace, dataset.getName()))
                              .document(document))));
    }
  }

  private static String indexDocumentId(DocumentKind kind, String canonicalNamespace, String name) {
    return kind.idDomain
        + "."
        + encodeIdComponent(canonicalNamespace)
        + "."
        + encodeIdComponent(name);
  }

  private static String encodeIdComponent(String value) {
    return ID_COMPONENT_ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
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
