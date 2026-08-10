/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.mappers;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toList;
import static marquez.common.models.RunState.NEW;
import static marquez.db.Columns.stringOrNull;
import static marquez.db.Columns.stringOrThrow;
import static marquez.db.Columns.timestampOrNull;
import static marquez.db.Columns.timestampOrThrow;
import static marquez.db.Columns.uuidOrNull;
import static marquez.db.Columns.uuidOrThrow;
import static marquez.db.mappers.MapperUtils.toFacetsOrNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.Utils;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetVersionId;
import marquez.common.models.InputDatasetVersion;
import marquez.common.models.NamespaceName;
import marquez.common.models.OutputDatasetVersion;
import marquez.common.models.RunId;
import marquez.common.models.RunState;
import marquez.db.Columns;
import marquez.service.models.Run;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.postgresql.util.PGobject;

@Slf4j
public final class RunMapper implements RowMapper<Run> {
  private final String columnPrefix;

  private static final ObjectMapper MAPPER = Utils.getMapper();
  private static final TypeReference<List<QueryDatasetVersion>> DATASET_VERSIONS_TYPE =
      new TypeReference<>() {};
  private static final TypeReference<ImmutableList<QueryDatasetFacet>> DATASET_FACETS_TYPE =
      new TypeReference<>() {};
  private static final TypeReference<Map<String, String>> RUN_ARGS_TYPE = new TypeReference<>() {};

  public RunMapper() {
    this("");
  }

  public RunMapper(String columnPrefix) {
    this.columnPrefix = columnPrefix;
  }

  @Override
  public Run map(@NonNull ResultSet results, @NonNull StatementContext context)
      throws SQLException {
    Set<String> columnNames = MapperUtils.getColumnNames(results.getMetaData());
    Optional<Instant> startedAt =
        Optional.ofNullable(timestampOrNull(results, columnPrefix + Columns.STARTED_AT));
    Optional<Long> durationMs =
        Optional.ofNullable(timestampOrNull(results, columnPrefix + Columns.ENDED_AT))
            .flatMap(endedAt -> startedAt.map(s -> s.until(endedAt, MILLIS)));
    List<QueryDatasetVersion> inputDatasetVersions =
        columnNames.contains(columnPrefix + Columns.INPUT_VERSIONS)
            ? toQueryDatasetVersion(results, columnPrefix + Columns.INPUT_VERSIONS)
            : ImmutableList.of();
    List<QueryDatasetVersion> outputDatasetVersions =
        columnNames.contains(columnPrefix + Columns.OUTPUT_VERSIONS)
            ? toQueryDatasetVersion(results, columnPrefix + Columns.OUTPUT_VERSIONS)
            : ImmutableList.of();
    DatasetFacetIndex datasetFacetIndex =
        indexDatasetFacets(getQueryDatasetFacets(results, columnNames));
    return new Run(
        RunId.of(uuidOrThrow(results, columnPrefix + Columns.ROW_UUID)),
        timestampOrThrow(results, columnPrefix + Columns.CREATED_AT),
        timestampOrThrow(results, columnPrefix + Columns.UPDATED_AT),
        timestampOrNull(results, columnPrefix + Columns.NOMINAL_START_TIME),
        timestampOrNull(results, columnPrefix + Columns.NOMINAL_END_TIME),
        stringOrNull(results, columnPrefix + Columns.CURRENT_RUN_STATE) == null
            ? NEW
            : RunState.valueOf(stringOrNull(results, columnPrefix + Columns.CURRENT_RUN_STATE)),
        columnNames.contains(columnPrefix + Columns.STARTED_AT)
            ? timestampOrNull(results, columnPrefix + Columns.STARTED_AT)
            : null,
        columnNames.contains(columnPrefix + Columns.ENDED_AT)
            ? timestampOrNull(results, columnPrefix + Columns.ENDED_AT)
            : null,
        durationMs.orElse(null),
        toArgsOrNull(results, columnPrefix + Columns.ARGS),
        stringOrThrow(results, columnPrefix + Columns.NAMESPACE_NAME),
        stringOrThrow(results, columnPrefix + Columns.JOB_NAME),
        uuidOrNull(results, columnPrefix + Columns.JOB_VERSION),
        stringOrNull(results, columnPrefix + Columns.LOCATION),
        toInputDatasetVersions(inputDatasetVersions, datasetFacetIndex.input()),
        toOutputDatasetVersions(outputDatasetVersions, datasetFacetIndex.output()),
        toFacetsOrNull(results, columnPrefix + Columns.FACETS));
  }

  private List<QueryDatasetVersion> toQueryDatasetVersion(ResultSet rs, String column)
      throws SQLException {
    String dsString = rs.getString(column);
    if (dsString == null) {
      return Collections.emptyList();
    }
    return Utils.fromJson(dsString, DATASET_VERSIONS_TYPE);
  }

  private Map<String, String> toArgsOrNull(ResultSet results, String argsColumn)
      throws SQLException {
    if (!Columns.exists(results, argsColumn)) {
      return ImmutableMap.of();
    }
    String args = stringOrNull(results, argsColumn);
    if (args == null) {
      return null;
    }
    return Utils.fromJson(args, RUN_ARGS_TYPE);
  }

  private List<InputDatasetVersion> toInputDatasetVersions(
      List<QueryDatasetVersion> datasetVersionIds,
      ImmutableMap<String, ImmutableMap<String, Object>> facetsByDatasetVersion) {
    try {
      return datasetVersionIds.stream()
          .map(
              version ->
                  new InputDatasetVersion(
                      version.toDatasetVersionId(),
                      facetsByDatasetVersion.getOrDefault(
                          version.datasetVersionUUID(), ImmutableMap.of())))
          .collect(toList());
    } catch (IllegalStateException e) {
      return Collections.emptyList();
    }
  }

  private List<OutputDatasetVersion> toOutputDatasetVersions(
      List<QueryDatasetVersion> datasetVersionIds,
      ImmutableMap<String, ImmutableMap<String, Object>> facetsByDatasetVersion) {
    try {
      return datasetVersionIds.stream()
          .map(
              version ->
                  new OutputDatasetVersion(
                      version.toDatasetVersionId(),
                      facetsByDatasetVersion.getOrDefault(
                          version.datasetVersionUUID(), ImmutableMap.of())))
          .collect(toList());
    } catch (IllegalStateException e) {
      return Collections.emptyList();
    }
  }

  private DatasetFacetIndex indexDatasetFacets(
      ImmutableList<QueryDatasetFacet> queryDatasetFacets) {
    if (queryDatasetFacets.isEmpty()) {
      return DatasetFacetIndex.EMPTY;
    }

    Map<String, Map<String, Object>> inputFacets = new HashMap<>();
    Map<String, Map<String, Object>> outputFacets = new HashMap<>();
    for (QueryDatasetFacet queryFacet : queryDatasetFacets) {
      Map<String, Map<String, Object>> facetsByDatasetVersion;
      if ("input".equalsIgnoreCase(queryFacet.type())) {
        facetsByDatasetVersion = inputFacets;
      } else if ("output".equalsIgnoreCase(queryFacet.type())) {
        facetsByDatasetVersion = outputFacets;
      } else {
        continue;
      }

      facetsByDatasetVersion
          .computeIfAbsent(queryFacet.datasetVersionUUID(), ignored -> new HashMap<>())
          // Facets arrive in ascending creation order, so the latest duplicate must win.
          .put(queryFacet.name(), queryFacet.facet().get(queryFacet.name()));
    }

    return new DatasetFacetIndex(
        toImmutableFacetIndex(inputFacets), toImmutableFacetIndex(outputFacets));
  }

  private ImmutableMap<String, ImmutableMap<String, Object>> toImmutableFacetIndex(
      Map<String, Map<String, Object>> mutableFacetIndex) {
    ImmutableMap.Builder<String, ImmutableMap<String, Object>> immutableFacetIndex =
        ImmutableMap.builder();
    mutableFacetIndex.forEach(
        (datasetVersionUuid, facets) ->
            immutableFacetIndex.put(datasetVersionUuid, ImmutableMap.copyOf(facets)));
    return immutableFacetIndex.build();
  }

  private ImmutableList<QueryDatasetFacet> getQueryDatasetFacets(
      ResultSet resultSet, Set<String> columnNames) throws SQLException {
    String column = columnPrefix + Columns.DATASET_FACETS;
    if (!columnNames.contains(column)) {
      return ImmutableList.of();
    }

    Object datasetFacets = resultSet.getObject(column);
    if (datasetFacets == null) {
      return ImmutableList.of();
    }

    try {
      return MAPPER.readValue(((PGobject) datasetFacets).getValue(), DATASET_FACETS_TYPE);
    } catch (JsonProcessingException e) {
      log.error("Could not read dataset facets from run row column {}", column, e);
      return ImmutableList.of();
    }
  }

  private record DatasetFacetIndex(
      ImmutableMap<String, ImmutableMap<String, Object>> input,
      ImmutableMap<String, ImmutableMap<String, Object>> output) {
    private static final DatasetFacetIndex EMPTY =
        new DatasetFacetIndex(ImmutableMap.of(), ImmutableMap.of());
  }

  record QueryDatasetFacet(
      @JsonProperty("dataset_version_uuid") String datasetVersionUUID,
      String name,
      String type,
      Map<String, Object> facet) {}

  record QueryDatasetVersion(
      String namespace,
      String name,
      UUID version,
      // field required to merge input versions with input dataset facets
      @JsonProperty("dataset_version_uuid") String datasetVersionUUID) {
    public DatasetVersionId toDatasetVersionId() {
      return DatasetVersionId.builder()
          .name(DatasetName.of(name))
          .namespace(NamespaceName.of(namespace))
          .version(version)
          .build();
    }
  }
}
