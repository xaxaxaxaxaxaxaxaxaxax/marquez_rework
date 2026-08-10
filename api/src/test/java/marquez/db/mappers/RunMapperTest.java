/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.mappers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import marquez.db.Columns;
import marquez.service.models.Run;
import org.jdbi.v3.core.statement.StatementContext;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

class RunMapperTest {
  private static final UUID RUN_UUID = UUID.fromString("10000000-0000-0000-0000-000000000001");
  private static final UUID INPUT_VERSION = UUID.fromString("20000000-0000-0000-0000-000000000001");
  private static final UUID OUTPUT_VERSION =
      UUID.fromString("30000000-0000-0000-0000-000000000001");
  private static final String DATASET_VERSION_ROW_UUID = "40000000-0000-0000-0000-000000000001";
  private static final List<String> COLUMN_NAMES =
      List.of(
          Columns.ROW_UUID,
          Columns.CREATED_AT,
          Columns.UPDATED_AT,
          Columns.NAMESPACE_NAME,
          Columns.JOB_NAME,
          Columns.INPUT_VERSIONS,
          Columns.OUTPUT_VERSIONS,
          Columns.DATASET_FACETS);

  @Test
  void mapsDatasetFacetsOnceAndLatestDuplicateWins() throws SQLException {
    ResultSet resultSet =
        resultSetWithDatasetFacets(
            """
            [
              {
                "dataset_version_uuid": "%s",
                "name": "quality",
                "type": "INPUT",
                "facet": {"quality": "old"}
              },
              {
                "dataset_version_uuid": "%s",
                "name": "rows",
                "type": "input",
                "facet": {"rows": 10}
              },
              {
                "dataset_version_uuid": "%s",
                "name": "quality",
                "type": "OuTpUt",
                "facet": {"quality": "output"}
              },
              {
                "dataset_version_uuid": "%s",
                "name": "quality",
                "type": "input",
                "facet": {"quality": "new"}
              }
            ]
            """
                .formatted(
                    DATASET_VERSION_ROW_UUID,
                    DATASET_VERSION_ROW_UUID,
                    DATASET_VERSION_ROW_UUID,
                    DATASET_VERSION_ROW_UUID));

    Run actual = new RunMapper().map(resultSet, mock(StatementContext.class));

    assertThat(actual.getInputDatasetVersions()).hasSize(1);
    assertThat(actual.getInputDatasetVersions().get(0).getDatasetVersionId().getVersion())
        .isEqualTo(INPUT_VERSION);
    assertThat(actual.getInputDatasetVersions().get(0).getFacets())
        .containsEntry("quality", "new")
        .containsEntry("rows", 10);
    assertThat(actual.getOutputDatasetVersions()).hasSize(1);
    assertThat(actual.getOutputDatasetVersions().get(0).getDatasetVersionId().getVersion())
        .isEqualTo(OUTPUT_VERSION);
    assertThat(actual.getOutputDatasetVersions().get(0).getFacets())
        .containsExactlyEntriesOf(Map.of("quality", "output"));
    verify(resultSet, times(1)).getObject(Columns.DATASET_FACETS);
  }

  @Test
  void malformedDatasetFacetsFallBackToEmptyMaps() throws SQLException {
    ResultSet resultSet = resultSetWithDatasetFacets("not-json");

    Run actual = new RunMapper().map(resultSet, mock(StatementContext.class));

    assertThat(actual.getInputDatasetVersions()).hasSize(1);
    assertThat(actual.getInputDatasetVersions().get(0).getFacets()).isEmpty();
    assertThat(actual.getOutputDatasetVersions()).hasSize(1);
    assertThat(actual.getOutputDatasetVersions().get(0).getFacets()).isEmpty();
    verify(resultSet, times(1)).getObject(Columns.DATASET_FACETS);
  }

  private ResultSet resultSetWithDatasetFacets(String datasetFacets) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(COLUMN_NAMES.size());
    for (int index = 0; index < COLUMN_NAMES.size(); index++) {
      when(metaData.getColumnName(index + 1)).thenReturn(COLUMN_NAMES.get(index));
    }

    when(resultSet.getObject(Columns.ROW_UUID)).thenReturn(RUN_UUID);
    when(resultSet.getObject(Columns.ROW_UUID, UUID.class)).thenReturn(RUN_UUID);
    Timestamp createdAt = Timestamp.from(Instant.parse("2024-01-01T00:00:00Z"));
    Timestamp updatedAt = Timestamp.from(Instant.parse("2024-01-01T00:01:00Z"));
    when(resultSet.getObject(Columns.CREATED_AT)).thenReturn(createdAt);
    when(resultSet.getTimestamp(Columns.CREATED_AT)).thenReturn(createdAt);
    when(resultSet.getObject(Columns.UPDATED_AT)).thenReturn(updatedAt);
    when(resultSet.getTimestamp(Columns.UPDATED_AT)).thenReturn(updatedAt);
    when(resultSet.getObject(Columns.NAMESPACE_NAME)).thenReturn("namespace");
    when(resultSet.getString(Columns.NAMESPACE_NAME)).thenReturn("namespace");
    when(resultSet.getObject(Columns.JOB_NAME)).thenReturn("job");
    when(resultSet.getString(Columns.JOB_NAME)).thenReturn("job");
    when(resultSet.getString(Columns.INPUT_VERSIONS))
        .thenReturn(datasetVersionsJson("input", INPUT_VERSION, DATASET_VERSION_ROW_UUID));
    when(resultSet.getString(Columns.OUTPUT_VERSIONS))
        .thenReturn(datasetVersionsJson("output", OUTPUT_VERSION, DATASET_VERSION_ROW_UUID));

    PGobject datasetFacetsObject = new PGobject();
    datasetFacetsObject.setType("json");
    datasetFacetsObject.setValue(datasetFacets);
    when(resultSet.getObject(Columns.DATASET_FACETS)).thenReturn(datasetFacetsObject);
    return resultSet;
  }

  private String datasetVersionsJson(String name, UUID version, String datasetVersionRowUuid) {
    return """
        [{
          "namespace": "namespace",
          "name": "%s",
          "version": "%s",
          "dataset_version_uuid": "%s"
        }]
        """
        .formatted(name, version, datasetVersionRowUuid);
  }
}
