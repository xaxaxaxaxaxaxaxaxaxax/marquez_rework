/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import marquez.search.SearchConfig;
import marquez.service.models.LineageEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;

class SearchServiceTest {
  private static final UUID RUN_ID = UUID.fromString("de2d8a76-57b3-42f6-8d26-06d6179ac45c");
  private static final UUID EFFECTIVE_RUN_ID =
      UUID.fromString("ec0f5598-20ab-4d60-ab4d-6fc280748251");

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

    @SuppressWarnings("unchecked")
    Map<String, Object> inputDocument = (Map<String, Object>) operations.get(0).index().document();
    assertThat(inputDocument)
        .containsEntry("run_id", RUN_ID.toString())
        .containsEntry("namespace", "input-namespace")
        .containsEntry("name", "input");
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
  void queuedSingletonStillThrowsWhenBulkResponseContainsItemErrors() throws IOException {
    BulkResponse response = mock(BulkResponse.class);
    when(response.errors()).thenReturn(true);
    when(response.items()).thenReturn(List.of());
    when(openSearchClient.bulk(any(BulkRequest.class))).thenReturn(response);

    assertThrows(
        IllegalStateException.class,
        () -> searchService.indexEvent(lineageEvent(), EFFECTIVE_RUN_ID));
  }

  @Test
  void batchCountsDistinctFailedEntriesRatherThanFailedOperations() throws IOException {
    BulkResponse response = mock(BulkResponse.class);
    when(response.errors()).thenReturn(true);
    doReturn(
            List.of(
                failedItem("first input"),
                successfulItem(),
                failedItem("first job"),
                successfulItem(),
                failedItem("second output"),
                successfulItem(),
                successfulItem(),
                successfulItem(),
                successfulItem()))
        .when(response)
        .items();
    when(openSearchClient.bulk(any(BulkRequest.class))).thenReturn(response);

    assertThat(
            searchService.indexEventsBestEffort(
                List.of(
                    new SearchService.IndexEntry(lineageEvent(), RUN_ID),
                    new SearchService.IndexEntry(lineageEvent(), EFFECTIVE_RUN_ID),
                    new SearchService.IndexEntry(lineageEvent(), UUID.randomUUID()))))
        .isEqualTo(2);
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
    return (Map<String, Object>) operation.index().document();
  }
}
