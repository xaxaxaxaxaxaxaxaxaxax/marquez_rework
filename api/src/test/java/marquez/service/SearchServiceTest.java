/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import marquez.search.SearchConfig;
import marquez.service.models.LineageEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch._types.query_dsl.MultiMatchQuery;
import org.opensearch.client.opensearch._types.query_dsl.Operator;
import org.opensearch.client.opensearch._types.query_dsl.TextQueryType;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.search.BuiltinHighlighterType;
import org.opensearch.client.opensearch.core.search.HighlightField;
import org.opensearch.client.util.ObjectBuilder;

class SearchServiceTest {
  private static final UUID RUN_ID = UUID.fromString("de2d8a76-57b3-42f6-8d26-06d6179ac45c");
  private static final UUID EFFECTIVE_RUN_ID =
      UUID.fromString("ec0f5598-20ab-4d60-ab4d-6fc280748251");
  private static final ObjectMapper DOCUMENT_MAPPER =
      new JacksonJsonpMapper().objectMapper().registerModule(new JavaTimeModule());

  private SearchConfig searchConfig;
  private OpenSearchClient openSearchClient;
  private SearchService searchService;

  @BeforeEach
  void setUp() {
    searchConfig = mock(SearchConfig.class);
    openSearchClient = mock(OpenSearchClient.class);
    when(searchConfig.isEnabled()).thenReturn(true);
    searchService = new SearchService(searchConfig, openSearchClient);
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void searchRequestsPreserveTheirExactSharedQueryShape() throws IOException {
    searchService.searchDatasets("needle");
    searchService.searchJobs("needle");

    ArgumentCaptor<Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>>> requests =
        ArgumentCaptor.forClass(Function.class);
    verify(openSearchClient, times(2)).search(requests.capture(), eq(ObjectNode.class));
    SearchRequest datasetRequest =
        requests.getAllValues().get(0).apply(new SearchRequest.Builder()).build();
    SearchRequest jobRequest =
        requests.getAllValues().get(1).apply(new SearchRequest.Builder()).build();

    assertSearchRequest(
        datasetRequest,
        "datasets",
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
            "facets.columnLineage.fields.*.transformationType"));
    assertSearchRequest(
        jobRequest,
        "jobs",
        List.of(
            "facets.sql.query",
            "facets.sourceCode.sourceCode",
            "facets.sourceCode.language",
            "runFacets.processing_engine.name",
            "run_id",
            "name",
            "namespace",
            "type"));
  }

  @Test
  void indexesInputsOutputsAndJobInOneOrderedBulkRequest() throws IOException {
    stubSuccessfulBulk();

    assertThat(searchService.indexEvent(lineageEvent())).isTrue();

    ArgumentCaptor<BulkRequest> request = ArgumentCaptor.forClass(BulkRequest.class);
    verify(openSearchClient).bulk(request.capture());
    List<BulkOperation> operations = request.getValue().operations();
    assertThat(operations)
        .extracting(operation -> operation.index().id())
        .containsExactly(
            "DATASET.aW5wdXQtbmFtZXNwYWNl.aW5wdXQ",
            "DATASET.b3V0cHV0LW5hbWVzcGFjZQ.b3V0cHV0",
            "JOB.am9iLW5hbWVzcGFjZQ.am9i");

    Map<String, Object> inputDocument = indexDocument(operations.get(0));
    assertThat(inputDocument)
        .containsOnlyKeys(
            "run_id", "eventType", "name", "inputFacets", "outputFacets", "namespace", "facets")
        .containsEntry("run_id", RUN_ID.toString())
        .containsEntry("namespace", "input-namespace")
        .containsEntry("name", "input")
        .containsEntry("inputFacets", null)
        .containsEntry("outputFacets", null)
        .containsEntry("facets", null);
    assertThat(indexDocument(operations.get(2)))
        .containsOnlyKeys("run_id", "eventType", "name", "type", "namespace", "facets", "runFacets")
        .containsEntry("run_id", RUN_ID.toString())
        .containsEntry("type", "BATCH")
        .containsEntry("facets", null)
        .containsEntry("runFacets", null);
  }

  @Test
  void indexesQueuedEventWithEffectiveRunUuid() throws IOException {
    stubSuccessfulBulk();

    assertThat(searchService.indexEvent(lineageEvent(), EFFECTIVE_RUN_ID)).isTrue();

    assertThat(capturedOperations())
        .extracting(operation -> indexDocument(operation).get("run_id"))
        .containsOnly(EFFECTIVE_RUN_ID.toString())
        .doesNotContain(RUN_ID.toString());
  }

  @Test
  void indexesQueuedEventsInOneOrderedBulkRequestWithTheirEffectiveRunUuids() throws IOException {
    stubSuccessfulBulk();
    LineageEvent first = lineageEvent();
    LineageEvent second = lineageEvent();

    assertThat(
            searchService.indexEventsBestEffort(
                List.of(
                    new SearchService.IndexEntry(first, RUN_ID),
                    new SearchService.IndexEntry(second, EFFECTIVE_RUN_ID))))
        .isZero();

    ArgumentCaptor<BulkRequest> request = ArgumentCaptor.forClass(BulkRequest.class);
    verify(openSearchClient).bulk(request.capture());
    List<BulkOperation> operations = request.getValue().operations();
    assertThat(operations)
        .extracting(operation -> operation.index().id())
        .containsExactly(
            "DATASET.aW5wdXQtbmFtZXNwYWNl.aW5wdXQ",
            "DATASET.b3V0cHV0LW5hbWVzcGFjZQ.b3V0cHV0",
            "JOB.am9iLW5hbWVzcGFjZQ.am9i",
            "DATASET.aW5wdXQtbmFtZXNwYWNl.aW5wdXQ",
            "DATASET.b3V0cHV0LW5hbWVzcGFjZQ.b3V0cHV0",
            "JOB.am9iLW5hbWVzcGFjZQ.am9i");
    assertThat(operations.subList(0, 3))
        .extracting(operation -> indexDocument(operation).get("run_id"))
        .containsOnly(RUN_ID.toString());
    assertThat(operations.subList(3, 6))
        .extracting(operation -> indexDocument(operation).get("run_id"))
        .containsOnly(EFFECTIVE_RUN_ID.toString());
  }

  @Test
  void continuesWhenBulkTransportIsUnavailable() throws IOException {
    when(openSearchClient.bulk(any(BulkRequest.class)))
        .thenThrow(new IOException("OpenSearch unavailable"));

    assertThat(assertDoesNotThrow(() -> searchService.indexEvent(lineageEvent()))).isFalse();
    verify(openSearchClient).bulk(any(BulkRequest.class));
  }

  @Test
  void failsWhenBulkResponseContainsItemErrors() throws IOException {
    BulkResponse response = mock(BulkResponse.class);
    when(response.errors()).thenReturn(true);
    when(response.items()).thenReturn(List.of());
    when(openSearchClient.bulk(any(BulkRequest.class))).thenReturn(response);

    assertThrows(IllegalStateException.class, () -> searchService.indexEvent(lineageEvent()));
  }

  @Test
  void batchMapsUnevenOperationRangesAcrossMaterializationFailure() throws IOException {
    LineageEvent first = lineageEvent();
    first.setOutputs(List.of());
    LineageEvent malformed = mock(LineageEvent.class);
    when(malformed.getInputs()).thenReturn(List.of(dataset("partial", "input")));
    when(malformed.getOutputs()).thenReturn(List.of());
    when(malformed.getJob()).thenThrow(new IllegalStateException("missing job"));
    LineageEvent third = lineageEvent();
    third.setInputs(
        List.of(
            dataset("third-input-namespace", "third-input-one"),
            dataset("third-input-namespace", "third-input-two")));
    LineageEvent fourth = lineageEvent();
    fourth.setInputs(List.of());
    fourth.setOutputs(List.of());
    BulkResponse response = mock(BulkResponse.class);
    when(response.errors()).thenReturn(true);
    doReturn(
            List.of(
                failedItem("first input"),
                failedItem("first job"),
                failedItem("third first input"),
                failedItem("third second input"),
                successfulItem(),
                successfulItem(),
                successfulItem()))
        .when(response)
        .items();
    when(openSearchClient.bulk(any(BulkRequest.class))).thenReturn(response);

    assertThat(
            searchService.indexEventsBestEffort(
                List.of(
                    new SearchService.IndexEntry(first, RUN_ID),
                    new SearchService.IndexEntry(malformed, UUID.randomUUID()),
                    new SearchService.IndexEntry(third, EFFECTIVE_RUN_ID),
                    new SearchService.IndexEntry(fourth, UUID.randomUUID()))))
        .isEqualTo(3);

    assertThat(capturedOperations()).hasSize(7);
  }

  @Test
  void batchTransportFailureCountsEverySubmittedEntry() throws IOException {
    when(openSearchClient.bulk(any(BulkRequest.class)))
        .thenThrow(new IOException("OpenSearch unavailable"));

    assertThat(
            searchService.indexEventsBestEffort(
                List.of(
                    new SearchService.IndexEntry(lineageEvent(), RUN_ID),
                    new SearchService.IndexEntry(lineageEvent(), EFFECTIVE_RUN_ID))))
        .isEqualTo(2);
  }

  @Test
  void batchRuntimeFailureCountsEverySubmittedEntry() throws IOException {
    when(openSearchClient.bulk(any(BulkRequest.class)))
        .thenThrow(new IllegalStateException("serialization failed"));

    assertThat(
            searchService.indexEventsBestEffort(
                List.of(
                    new SearchService.IndexEntry(lineageEvent(), RUN_ID),
                    new SearchService.IndexEntry(lineageEvent(), EFFECTIVE_RUN_ID))))
        .isEqualTo(2);
  }

  @Test
  void batchMaterializationFailureDoesNotSubmitPartialOperationsOrSuppressLaterEntries()
      throws IOException {
    stubSuccessfulBulk();
    LineageEvent malformed = mock(LineageEvent.class);
    when(malformed.getInputs()).thenReturn(List.of(dataset("partial", "input")));
    when(malformed.getOutputs()).thenReturn(List.of(dataset("partial", "output")));
    when(malformed.getJob()).thenThrow(new IllegalStateException("missing job"));

    assertThat(
            searchService.indexEventsBestEffort(
                List.of(
                    new SearchService.IndexEntry(malformed, RUN_ID),
                    new SearchService.IndexEntry(lineageEvent(), EFFECTIVE_RUN_ID))))
        .isEqualTo(1);

    List<BulkOperation> operations = capturedOperations();
    assertThat(operations).hasSize(3);
    assertThat(operations)
        .extracting(operation -> indexDocument(operation).get("run_id"))
        .containsOnly(EFFECTIVE_RUN_ID.toString());
  }

  @Test
  void batchConservativelyFailsSubmittedEntriesWhenErrorResponseCannotBeAligned()
      throws IOException {
    BulkResponse response = mock(BulkResponse.class);
    when(response.errors()).thenReturn(true);
    when(response.items()).thenReturn(List.of());
    when(openSearchClient.bulk(any(BulkRequest.class))).thenReturn(response);

    assertThat(
            searchService.indexEventsBestEffort(
                List.of(
                    new SearchService.IndexEntry(lineageEvent(), RUN_ID),
                    new SearchService.IndexEntry(lineageEvent(), EFFECTIVE_RUN_ID))))
        .isEqualTo(2);
  }

  @Test
  void emptyOrDisabledBatchSkipsBulkRequest() {
    assertThat(searchService.indexEventsBestEffort(List.of())).isZero();
    when(searchConfig.isEnabled()).thenReturn(false);

    assertThat(
            searchService.indexEventsBestEffort(
                List.of(new SearchService.IndexEntry(lineageEvent(), EFFECTIVE_RUN_ID))))
        .isZero();

    verifyNoInteractions(openSearchClient);
  }

  @Test
  void skipsBulkRequestWhenSearchIsDisabled() {
    when(searchConfig.isEnabled()).thenReturn(false);

    assertThat(searchService.indexEvent(lineageEvent())).isTrue();

    verifyNoInteractions(openSearchClient);
  }

  @Test
  void delimiterBearingIdentityComponentsRemainInjective() throws IOException {
    stubSuccessfulBulk();
    LineageEvent event =
        lineageEvent(
            LineageEvent.Job.builder().namespace("job-namespace").name("job").build(),
            List.of(dataset("a:b", "c"), dataset("a", "b:c")),
            List.of());

    assertThat(searchService.indexEvent(event)).isTrue();

    List<BulkOperation> operations = capturedOperations();
    assertThat(operations)
        .extracting(operation -> operation.index().id())
        .startsWith("DATASET.YTpi.Yw", "DATASET.YQ.Yjpj");
    assertThat(operations.get(0).index().id()).isNotEqualTo(operations.get(1).index().id());
  }

  @Test
  void canonicalNamespaceDrivesBothIdentityAndSource() throws IOException {
    stubSuccessfulBulk();
    LineageEvent event =
        lineageEvent(
            LineageEvent.Job.builder().namespace("job namespace?#").name("job").build(),
            List.of(
                dataset("namespace with spaces?#", "shared"),
                dataset("namespace_with_spaces__", "shared")),
            List.of());

    assertThat(searchService.indexEvent(event)).isTrue();

    List<BulkOperation> operations = capturedOperations();
    assertThat(operations.get(0).index().id()).isEqualTo(operations.get(1).index().id());
    assertThat(indexDocument(operations.get(0)))
        .containsEntry("namespace", "namespace_with_spaces__");
    assertThat(indexDocument(operations.get(1)))
        .containsEntry("namespace", "namespace_with_spaces__");
    assertThat(indexDocument(operations.get(2))).containsEntry("namespace", "job_namespace__");
  }

  @Test
  void jobAndDatasetIdentityDomainsRemainDistinct() throws IOException {
    stubSuccessfulBulk();
    LineageEvent event =
        lineageEvent(
            LineageEvent.Job.builder().namespace("same").name("entity").build(),
            List.of(dataset("same", "entity")),
            List.of());

    assertThat(searchService.indexEvent(event)).isTrue();

    List<BulkOperation> operations = capturedOperations();
    assertThat(operations)
        .extracting(operation -> operation.index().id())
        .containsExactly("DATASET.c2FtZQ.ZW50aXR5", "JOB.c2FtZQ.ZW50aXR5");
  }

  private LineageEvent lineageEvent() {
    return lineageEvent(
        LineageEvent.Job.builder().namespace("job-namespace").name("job").build(),
        List.of(dataset("input-namespace", "input")),
        List.of(dataset("output-namespace", "output")));
  }

  private LineageEvent lineageEvent(
      LineageEvent.Job job, List<LineageEvent.Dataset> inputs, List<LineageEvent.Dataset> outputs) {
    return LineageEvent.builder()
        .eventType("COMPLETE")
        .eventTime(Instant.parse("2026-08-10T00:00:00Z").atZone(ZoneOffset.UTC))
        .run(new LineageEvent.Run(RUN_ID.toString(), null))
        .job(job)
        .inputs(inputs)
        .outputs(outputs)
        .producer("https://example.com/producer")
        .build();
  }

  private static LineageEvent.Dataset dataset(String namespace, String name) {
    return LineageEvent.Dataset.builder().namespace(namespace).name(name).build();
  }

  private static void assertSearchRequest(
      SearchRequest request, String index, List<String> fields) {
    assertThat(request.index()).containsExactly(index);
    MultiMatchQuery query = request.query().multiMatch();
    assertThat(query.query()).isEqualTo("needle");
    assertThat(query.type()).isEqualTo(TextQueryType.PhrasePrefix);
    assertThat(query.operator()).isEqualTo(Operator.Or);
    assertThat(query.fields()).containsExactlyElementsOf(fields);
    assertThat(request.highlight().fields().keySet()).containsExactlyInAnyOrderElementsOf(fields);
    for (HighlightField field : request.highlight().fields().values()) {
      assertThat(field.type().builtin()).isEqualTo(BuiltinHighlighterType.Plain);
    }
  }

  private List<BulkOperation> capturedOperations() throws IOException {
    ArgumentCaptor<BulkRequest> request = ArgumentCaptor.forClass(BulkRequest.class);
    verify(openSearchClient).bulk(request.capture());
    return request.getValue().operations();
  }

  private void stubSuccessfulBulk() throws IOException {
    BulkResponse response = mock(BulkResponse.class);
    when(response.errors()).thenReturn(false);
    when(openSearchClient.bulk(any(BulkRequest.class))).thenReturn(response);
  }

  private static BulkResponseItem successfulItem() {
    return mock(BulkResponseItem.class);
  }

  private static BulkResponseItem failedItem(String reason) {
    BulkResponseItem item = mock(BulkResponseItem.class);
    ErrorCause error = mock(ErrorCause.class);
    when(error.reason()).thenReturn(reason);
    when(item.error()).thenReturn(error);
    return item;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> indexDocument(BulkOperation operation) {
    return DOCUMENT_MAPPER.convertValue(operation.index().document(), Map.class);
  }
}
