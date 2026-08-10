/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
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
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;

class SearchServiceTest {
  private static final UUID RUN_ID = UUID.fromString("de2d8a76-57b3-42f6-8d26-06d6179ac45c");

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
    BulkResponse response = mock(BulkResponse.class);
    when(response.errors()).thenReturn(false);
    when(openSearchClient.bulk(any(BulkRequest.class))).thenReturn(response);

    searchService.indexEvent(lineageEvent());

    ArgumentCaptor<BulkRequest> request = ArgumentCaptor.forClass(BulkRequest.class);
    verify(openSearchClient).bulk(request.capture());
    List<BulkOperation> operations = request.getValue().operations();
    assertThat(operations)
        .extracting(operation -> operation.index().id())
        .containsExactly(
            "DATASET:input-namespace:input",
            "DATASET:output-namespace:output",
            "JOB:job-namespace:job");

    @SuppressWarnings("unchecked")
    Map<String, Object> inputDocument = (Map<String, Object>) operations.get(0).index().document();
    assertThat(inputDocument)
        .containsEntry("run_id", RUN_ID.toString())
        .containsEntry("namespace", "input-namespace")
        .containsEntry("name", "input");
  }

  @Test
  void continuesWhenBulkTransportIsUnavailable() throws IOException {
    when(openSearchClient.bulk(any(BulkRequest.class)))
        .thenThrow(new IOException("OpenSearch unavailable"));

    assertDoesNotThrow(() -> searchService.indexEvent(lineageEvent()));
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
  void skipsBulkRequestWhenSearchIsDisabled() {
    when(searchConfig.isEnabled()).thenReturn(false);

    searchService.indexEvent(lineageEvent());

    verifyNoInteractions(openSearchClient);
  }

  private LineageEvent lineageEvent() {
    return LineageEvent.builder()
        .eventType("COMPLETE")
        .eventTime(Instant.parse("2026-08-10T00:00:00Z").atZone(ZoneOffset.UTC))
        .run(new LineageEvent.Run(RUN_ID.toString(), null))
        .job(LineageEvent.Job.builder().namespace("job-namespace").name("job").build())
        .inputs(
            List.of(
                LineageEvent.Dataset.builder().namespace("input-namespace").name("input").build()))
        .outputs(
            List.of(
                LineageEvent.Dataset.builder()
                    .namespace("output-namespace")
                    .name("output")
                    .build()))
        .producer("https://example.com/producer")
        .build();
  }
}
