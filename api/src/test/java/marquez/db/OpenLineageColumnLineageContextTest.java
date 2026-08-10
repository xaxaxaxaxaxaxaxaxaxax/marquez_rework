/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.LineageTestUtils.PRODUCER_URL;
import static marquez.db.LineageTestUtils.SCHEMA_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import marquez.common.models.RunState;
import marquez.db.ColumnLineageDao.ColumnLineageDatasetWrite;
import marquez.db.ColumnLineageDao.ColumnLineageWrite;
import marquez.db.OpenLineageDao.ColumnLineageContext;
import marquez.db.OpenLineageDao.LineageWriteContext;
import marquez.db.models.ColumnLineageRow;
import marquez.db.models.DatasetFieldRow;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetSymlinkRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.InputFieldData;
import marquez.db.models.ModelDaos;
import marquez.db.models.NamespaceRow;
import marquez.db.models.SourceRow;
import marquez.db.models.UpdateLineageRow;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.Dataset;
import marquez.service.models.LineageEvent.DatasetFacets;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class OpenLineageColumnLineageContextTest {
  private static final String INPUT_NAMESPACE = "input_namespace";
  private static final String INPUT_DATASET = "input_dataset";
  private static final String INPUT_FIELD = "input_field";
  private static final String OUTPUT_COLUMN = "output_column";

  @Test
  void preservesLegacyPublicDaoEntryPoints() throws NoSuchMethodException {
    assertThat(
            OpenLineageDao.class.getMethod(
                "updateMarquezOnComplete",
                LineageEvent.class,
                UpdateLineageRow.class,
                RunState.class))
        .isNotNull();
    assertThat(
            OpenLineageDao.class.getMethod(
                "updateMarquezOnStreamingJob",
                LineageEvent.class,
                UpdateLineageRow.class,
                RunState.class))
        .isNotNull();
    assertThat(
            ColumnLineageDao.class.getMethod(
                "upsertColumnLineageRowsForIntake", List.class, Instant.class))
        .isNotNull();
    assertThat(
            JobVersionDao.class.getMethod(
                "upsertCurrentInputOrOutputDatasetsFor",
                UUID.class,
                Iterable.class,
                UUID.class,
                UUID.class,
                JobVersionDao.IoType.class))
        .isNotNull();
    assertThat(
            RunArgsDao.class.getMethod(
                "doUpsertRunArgs", UUID.class, Instant.class, String.class, String.class))
        .isNotNull();
  }

  @Test
  void doesNotLoadInputFieldsForOutputsWithoutNonemptyColumnLineage() {
    ModelDaos daos = mock(ModelDaos.class);
    ColumnLineageContext context = new ColumnLineageContext(daos, UUID.randomUUID());
    DatasetVersionRow datasetVersion = mock(DatasetVersionRow.class);

    Dataset withoutColumnLineage =
        new Dataset("namespace", "without_lineage", DatasetFacets.builder().build());
    Dataset withEmptyColumnLineage =
        new Dataset(
            "namespace",
            "empty_lineage",
            DatasetFacets.builder()
                .columnLineage(
                    new LineageEvent.ColumnLineageDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        new LineageEvent.ColumnLineageDatasetFacetFields(Collections.emptyMap())))
                .build());

    context.upsertColumnLineage(
        withoutColumnLineage, Instant.now(), Collections.emptyList(), datasetVersion);
    context.upsertColumnLineage(
        withEmptyColumnLineage, Instant.now(), Collections.emptyList(), datasetVersion);

    verify(daos, never()).getDatasetFieldDao();
    verify(daos, never()).getColumnLineageDao();
  }

  @Test
  void resolvesRunFieldsOnceAndFlushesMultipleOutputDatasetsInOnePhysicalIntakeCall() {
    UUID runUuid = UUID.randomUUID();
    UUID inputDatasetUuid = UUID.randomUUID();
    UUID inputDatasetVersionUuid = UUID.randomUUID();
    UUID inputDatasetFieldUuid = UUID.randomUUID();
    UUID outputDatasetFieldUuid = UUID.randomUUID();

    ModelDaos daos = mock(ModelDaos.class);
    RunDao runDao = mock(RunDao.class);
    DatasetFieldDao datasetFieldDao = mock(DatasetFieldDao.class);
    ColumnLineageDao columnLineageDao = mock(ColumnLineageDao.class);
    when(daos.getRunDao()).thenReturn(runDao);
    when(daos.getDatasetFieldDao()).thenReturn(datasetFieldDao);
    when(daos.getColumnLineageDao()).thenReturn(columnLineageDao);
    when(datasetFieldDao.findInputFieldsDataAssociatedWithRun(runUuid))
        .thenReturn(
            Collections.singletonList(
                new InputFieldData(
                    INPUT_NAMESPACE,
                    INPUT_DATASET,
                    INPUT_FIELD,
                    inputDatasetUuid,
                    inputDatasetFieldUuid,
                    inputDatasetVersionUuid)));
    DatasetFieldRow outputField = mock(DatasetFieldRow.class);
    when(outputField.getName()).thenReturn(OUTPUT_COLUMN);
    when(outputField.getUuid()).thenReturn(outputDatasetFieldUuid);
    DatasetVersionRow firstVersion = mock(DatasetVersionRow.class);
    DatasetVersionRow secondVersion = mock(DatasetVersionRow.class);
    when(firstVersion.getUuid()).thenReturn(UUID.randomUUID());
    when(secondVersion.getUuid()).thenReturn(UUID.randomUUID());

    LineageWriteContext context = LineageWriteContext.forIntake(daos, runUuid);
    Instant now = Instant.now();
    context.queueInputMapping(inputDatasetVersionUuid);
    context.flushInputMappings();
    context.collectColumnLineage(
        outputWithColumnLineage("first_output"),
        now,
        Collections.singletonList(outputField),
        firstVersion);
    context.collectColumnLineage(
        outputWithColumnLineage("second_output"),
        now,
        Collections.singletonList(outputField),
        secondVersion);
    context.flushColumnLineage(now);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ColumnLineageDatasetWrite>> writes = ArgumentCaptor.forClass(List.class);
    InOrder flushOrder = inOrder(runDao, datasetFieldDao, columnLineageDao);
    flushOrder.verify(runDao).updateInputMappingsInTransaction(eq(runUuid), any());
    flushOrder.verify(datasetFieldDao).findInputFieldsDataAssociatedWithRun(runUuid);
    flushOrder
        .verify(columnLineageDao)
        .upsertColumnLineageRowsForIntakeInTransaction(writes.capture(), eq(now));
    verify(datasetFieldDao, times(1)).findInputFieldsDataAssociatedWithRun(runUuid);
    assertThat(writes.getValue()).hasSize(2);
    assertThat(writes.getValue())
        .extracting(ColumnLineageDatasetWrite::outputDatasetVersionUuid)
        .containsExactly(firstVersion.getUuid(), secondVersion.getUuid());
    for (ColumnLineageDatasetWrite datasetWrite : writes.getValue()) {
      assertThat(datasetWrite.writes()).hasSize(1);
      assertThat(datasetWrite.writes().get(0).outputDatasetFieldUuid())
          .isEqualTo(outputDatasetFieldUuid);
      assertThat(datasetWrite.writes().get(0).inputs())
          .containsExactly(Pair.of(inputDatasetVersionUuid, inputDatasetFieldUuid));
    }
    verify(columnLineageDao, never())
        .upsertColumnLineageRowsForIntake(anyList(), any(Instant.class));
    verify(columnLineageDao, never())
        .upsertColumnLineageRows(any(UUID.class), anyList(), any(Instant.class));
  }

  @Test
  void doesNotLoadInputFieldsForRunlessColumnLineage() {
    ModelDaos daos = mock(ModelDaos.class);
    DatasetFieldRow outputField = mock(DatasetFieldRow.class);
    DatasetVersionRow datasetVersion = mock(DatasetVersionRow.class);
    when(outputField.getName()).thenReturn(OUTPUT_COLUMN);
    when(outputField.getUuid()).thenReturn(UUID.randomUUID());
    when(datasetVersion.getUuid()).thenReturn(UUID.randomUUID());

    new ColumnLineageContext(daos, null)
        .upsertColumnLineage(
            outputWithColumnLineage("runless_output"),
            Instant.now(),
            Collections.singletonList(outputField),
            datasetVersion);

    verify(daos, never()).getDatasetFieldDao();
    verify(daos, never()).getColumnLineageDao();
  }

  @Test
  void intakeSkipsPhysicalColumnLineageWithoutRunUuid() {
    ModelDaos daos = mock(ModelDaos.class);
    DatasetFieldRow outputField = mock(DatasetFieldRow.class);
    DatasetVersionRow datasetVersion = mock(DatasetVersionRow.class);
    when(outputField.getName()).thenReturn(OUTPUT_COLUMN);
    when(outputField.getUuid()).thenReturn(UUID.randomUUID());
    when(datasetVersion.getUuid()).thenReturn(UUID.randomUUID());

    LineageWriteContext context = LineageWriteContext.forIntake(daos, null);
    Instant now = Instant.now();
    context.collectColumnLineage(
        outputWithColumnLineage("runless_output"),
        now,
        Collections.singletonList(outputField),
        datasetVersion);
    context.flushColumnLineage(now);

    verify(daos, never()).getDatasetFieldDao();
    verify(daos, never()).getColumnLineageDao();
  }

  @Test
  void deduplicatesAliasesAndWritesAllOutputColumnsInOneDaoCall() {
    String aliasNamespace = "alias_namespace";
    String aliasDataset = "alias_dataset";
    String secondOutputColumn = "second_output_column";
    UUID runUuid = UUID.randomUUID();
    UUID inputDatasetUuid = UUID.randomUUID();
    UUID inputDatasetVersionUuid = UUID.randomUUID();
    UUID inputDatasetFieldUuid = UUID.randomUUID();

    ModelDaos daos = mock(ModelDaos.class);
    DatasetFieldDao datasetFieldDao = mock(DatasetFieldDao.class);
    ColumnLineageDao columnLineageDao = mock(ColumnLineageDao.class);
    when(daos.getDatasetFieldDao()).thenReturn(datasetFieldDao);
    when(daos.getColumnLineageDao()).thenReturn(columnLineageDao);
    when(datasetFieldDao.findInputFieldsDataAssociatedWithRun(runUuid))
        .thenReturn(
            List.of(
                new InputFieldData(
                    INPUT_NAMESPACE,
                    INPUT_DATASET,
                    INPUT_FIELD,
                    inputDatasetUuid,
                    inputDatasetFieldUuid,
                    inputDatasetVersionUuid),
                new InputFieldData(
                    INPUT_NAMESPACE,
                    INPUT_DATASET,
                    INPUT_FIELD,
                    inputDatasetUuid,
                    inputDatasetFieldUuid,
                    inputDatasetVersionUuid),
                new InputFieldData(
                    aliasNamespace,
                    aliasDataset,
                    INPUT_FIELD,
                    inputDatasetUuid,
                    inputDatasetFieldUuid,
                    inputDatasetVersionUuid)));
    when(columnLineageDao.upsertColumnLineageRows(any(UUID.class), anyList(), any(Instant.class)))
        .thenReturn(Collections.emptyList());

    DatasetFieldRow firstOutputField = mock(DatasetFieldRow.class);
    DatasetFieldRow secondOutputField = mock(DatasetFieldRow.class);
    when(firstOutputField.getName()).thenReturn(OUTPUT_COLUMN);
    when(firstOutputField.getUuid()).thenReturn(UUID.randomUUID());
    when(secondOutputField.getName()).thenReturn(secondOutputColumn);
    when(secondOutputField.getUuid()).thenReturn(UUID.randomUUID());
    DatasetVersionRow datasetVersion = mock(DatasetVersionRow.class);
    when(datasetVersion.getUuid()).thenReturn(UUID.randomUUID());

    Map<String, LineageEvent.ColumnLineageOutputColumn> columns = new LinkedHashMap<>();
    columns.put(
        OUTPUT_COLUMN,
        outputColumn(
            List.of(
                new LineageEvent.ColumnLineageInputField(
                    INPUT_NAMESPACE, INPUT_DATASET, INPUT_FIELD),
                new LineageEvent.ColumnLineageInputField(
                    aliasNamespace, aliasDataset, INPUT_FIELD))));
    columns.put(
        secondOutputColumn,
        outputColumn(
            Collections.singletonList(
                new LineageEvent.ColumnLineageInputField(
                    INPUT_NAMESPACE, INPUT_DATASET, INPUT_FIELD))));

    new ColumnLineageContext(daos, runUuid)
        .upsertColumnLineage(
            outputWithColumnLineage("output", columns),
            Instant.now(),
            List.of(firstOutputField, secondOutputField),
            datasetVersion);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ColumnLineageWrite>> writes = ArgumentCaptor.forClass(List.class);
    verify(columnLineageDao, times(1))
        .upsertColumnLineageRows(
            eq(datasetVersion.getUuid()), writes.capture(), any(Instant.class));
    assertThat(writes.getValue()).hasSize(2);
    assertThat(writes.getValue().get(0).inputs())
        .containsExactly(Pair.of(inputDatasetVersionUuid, inputDatasetFieldUuid));
    assertThat(writes.getValue().get(1).inputs())
        .containsExactly(Pair.of(inputDatasetVersionUuid, inputDatasetFieldUuid));
  }

  @Test
  void chunksLargeColumnLineageWritesAcrossOutputFieldsAndReadsBackOnce() {
    ColumnLineageDao dao = mock(ColumnLineageDao.class, CALLS_REAL_METHODS);
    UUID outputDatasetVersionUuid = UUID.randomUUID();
    UUID firstOutputDatasetFieldUuid = UUID.randomUUID();
    UUID secondOutputDatasetFieldUuid = UUID.randomUUID();
    List<Pair<UUID, UUID>> firstInputs = new ArrayList<>();
    List<Pair<UUID, UUID>> secondInputs = new ArrayList<>();
    for (int i = 0; i < ColumnLineageDao.MAX_ROWS_PER_UPSERT + 1; i++) {
      firstInputs.add(Pair.of(UUID.randomUUID(), UUID.randomUUID()));
    }
    for (int i = 0; i < ColumnLineageDao.MAX_ROWS_PER_UPSERT; i++) {
      secondInputs.add(Pair.of(UUID.randomUUID(), UUID.randomUUID()));
    }
    when(dao.findColumnLineageByDatasetVersionAndOutputDatasetFields(
            eq(outputDatasetVersionUuid), anyList()))
        .thenReturn(Collections.emptyList());

    dao.upsertColumnLineageRows(
        outputDatasetVersionUuid,
        List.of(
            new ColumnLineageWrite(firstOutputDatasetFieldUuid, firstInputs, "description", "type"),
            new ColumnLineageWrite(
                secondOutputDatasetFieldUuid, secondInputs, "description", "type")),
        Instant.now());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ColumnLineageRow>> rows = ArgumentCaptor.forClass(List.class);
    verify(dao, times(3)).doUpsertColumnLineageRow(rows.capture());
    verify(dao, times(1))
        .findColumnLineageByDatasetVersionAndOutputDatasetFields(
            eq(outputDatasetVersionUuid),
            eq(List.of(firstOutputDatasetFieldUuid, secondOutputDatasetFieldUuid)));
    assertThat(rows.getAllValues())
        .extracting(List::size)
        .containsExactly(
            ColumnLineageDao.MAX_ROWS_PER_UPSERT, ColumnLineageDao.MAX_ROWS_PER_UPSERT, 1);
  }

  @Test
  void chunksLargeColumnLineageReadbacksByOutputField() {
    ColumnLineageDao dao = mock(ColumnLineageDao.class, CALLS_REAL_METHODS);
    UUID outputDatasetVersionUuid = UUID.randomUUID();
    List<ColumnLineageWrite> writes = new ArrayList<>(ColumnLineageDao.MAX_FIELDS_PER_READ + 1);
    for (int i = 0; i < ColumnLineageDao.MAX_FIELDS_PER_READ + 1; i++) {
      writes.add(
          new ColumnLineageWrite(
              UUID.randomUUID(),
              Collections.singletonList(Pair.of(UUID.randomUUID(), UUID.randomUUID())),
              "description",
              "type"));
    }
    when(dao.findColumnLineageByDatasetVersionAndOutputDatasetFields(
            eq(outputDatasetVersionUuid), anyList()))
        .thenReturn(Collections.emptyList());

    dao.upsertColumnLineageRows(outputDatasetVersionUuid, writes, Instant.now());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<UUID>> outputFields = ArgumentCaptor.forClass(List.class);
    verify(dao, times(2))
        .findColumnLineageByDatasetVersionAndOutputDatasetFields(
            eq(outputDatasetVersionUuid), outputFields.capture());
    assertThat(outputFields.getAllValues())
        .extracting(List::size)
        .containsExactly(ColumnLineageDao.MAX_FIELDS_PER_READ, 1);
  }

  @Test
  void emptyBulkWriteDoesNotEnterTransaction() {
    ColumnLineageDao dao = mock(ColumnLineageDao.class, CALLS_REAL_METHODS);
    UUID outputDatasetVersionUuid = UUID.randomUUID();

    assertThat(
            dao.upsertColumnLineageRows(
                outputDatasetVersionUuid, Collections.emptyList(), Instant.now()))
        .isEmpty();
    assertThat(
            dao.upsertColumnLineageRows(
                outputDatasetVersionUuid,
                Collections.singletonList(
                    new ColumnLineageWrite(
                        UUID.randomUUID(), Collections.emptyList(), "description", "type")),
                Instant.now()))
        .isEmpty();

    verify(dao, never())
        .upsertColumnLineageRowsInTransaction(any(UUID.class), anyList(), any(Instant.class));
  }

  @Test
  void reusesNamespaceRowWhenFormattingDoesNotChangeName() {
    String namespace = "namespace";
    Instant now = Instant.now();
    UUID datasetVersionUuid = UUID.randomUUID();
    OpenLineageDao dao = mock(OpenLineageDao.class, CALLS_REAL_METHODS);
    ModelDaos daos = mock(ModelDaos.class);
    NamespaceDao namespaceDao = mock(NamespaceDao.class);
    SourceDao sourceDao = mock(SourceDao.class);
    DatasetSymlinkDao datasetSymlinkDao = mock(DatasetSymlinkDao.class);
    DatasetDao datasetDao = mock(DatasetDao.class);
    DatasetVersionDao datasetVersionDao = mock(DatasetVersionDao.class);
    DatasetFieldDao datasetFieldDao = mock(DatasetFieldDao.class);
    NamespaceRow namespaceRow = mock(NamespaceRow.class);
    SourceRow sourceRow = mock(SourceRow.class);
    DatasetSymlinkRow symlinkRow = mock(DatasetSymlinkRow.class);
    DatasetRow datasetRow = mock(DatasetRow.class);
    DatasetVersionRow datasetVersionRow = mock(DatasetVersionRow.class);

    when(daos.getNamespaceDao()).thenReturn(namespaceDao);
    when(daos.getSourceDao()).thenReturn(sourceDao);
    when(daos.getDatasetSymlinkDao()).thenReturn(datasetSymlinkDao);
    when(daos.getDatasetDao()).thenReturn(datasetDao);
    when(daos.getDatasetVersionDao()).thenReturn(datasetVersionDao);
    when(daos.getDatasetFieldDao()).thenReturn(datasetFieldDao);
    when(namespaceDao.upsertNamespaceRow(
            any(UUID.class), eq(now), eq(namespace), eq(OpenLineageDao.DEFAULT_NAMESPACE_OWNER)))
        .thenReturn(namespaceRow);
    when(namespaceRow.getUuid()).thenReturn(UUID.randomUUID());
    when(namespaceRow.getName()).thenReturn(namespace);
    when(sourceDao.upsertOrDefault(
            any(UUID.class), any(), eq(now), eq(OpenLineageDao.DEFAULT_SOURCE_NAME), eq("")))
        .thenReturn(sourceRow);
    when(sourceRow.getUuid()).thenReturn(UUID.randomUUID());
    when(sourceRow.getName()).thenReturn(OpenLineageDao.DEFAULT_SOURCE_NAME);
    when(datasetSymlinkDao.upsertDatasetSymlinkRow(
            any(UUID.class), eq("dataset"), any(UUID.class), eq(true), any(), eq(now)))
        .thenReturn(symlinkRow);
    when(symlinkRow.getUuid()).thenReturn(UUID.randomUUID());
    when(symlinkRow.getName()).thenReturn("dataset");
    when(datasetDao.upsert(
            any(UUID.class),
            any(),
            eq(now),
            any(UUID.class),
            eq(namespace),
            any(UUID.class),
            eq(OpenLineageDao.DEFAULT_SOURCE_NAME),
            eq("dataset"),
            eq("dataset"),
            any(),
            anyBoolean()))
        .thenReturn(datasetRow);
    when(datasetRow.getCurrentVersionUuid()).thenReturn(Optional.of(datasetVersionUuid));
    when(datasetVersionDao.findRowByUuid(datasetVersionUuid))
        .thenReturn(Optional.of(datasetVersionRow));

    UpdateLineageRow.DatasetRecord record =
        dao.upsertLineageDataset(
            daos,
            new Dataset(namespace, "dataset", DatasetFacets.builder().build()),
            now,
            null,
            true);

    verify(namespaceDao, times(1))
        .upsertNamespaceRow(
            any(UUID.class), eq(now), eq(namespace), eq(OpenLineageDao.DEFAULT_NAMESPACE_OWNER));
    assertThat(record.getNamespaceRow()).isSameAs(namespaceRow);
  }

  @Test
  void eventContextCachesNamespacesByExactName() {
    Instant now = Instant.now();
    ModelDaos daos = mock(ModelDaos.class);
    NamespaceDao namespaceDao = mock(NamespaceDao.class);
    NamespaceRow namespaceRow = mock(NamespaceRow.class);
    when(daos.getNamespaceDao()).thenReturn(namespaceDao);
    when(namespaceDao.upsertNamespaceRow(
            any(UUID.class), eq(now), eq("namespace"), eq(OpenLineageDao.DEFAULT_NAMESPACE_OWNER)))
        .thenReturn(namespaceRow);

    LineageWriteContext context = LineageWriteContext.forIntake(daos, null);
    assertThat(context.upsertNamespace("namespace", now)).isSameAs(namespaceRow);
    assertThat(context.upsertNamespace("namespace", now)).isSameAs(namespaceRow);

    verify(namespaceDao, times(1))
        .upsertNamespaceRow(
            any(UUID.class), eq(now), eq("namespace"), eq(OpenLineageDao.DEFAULT_NAMESPACE_OWNER));
  }

  @Test
  void sourceCacheReappliesNonconsecutiveSpecifications() {
    Instant now = Instant.now();
    ModelDaos daos = mock(ModelDaos.class);
    SourceDao sourceDao = mock(SourceDao.class);
    SourceRow firstA = mock(SourceRow.class);
    SourceRow b = mock(SourceRow.class);
    SourceRow secondA = mock(SourceRow.class);
    when(daos.getSourceDao()).thenReturn(sourceDao);
    when(sourceDao.upsert(any(UUID.class), eq("type"), eq(now), eq("source"), any()))
        .thenReturn(firstA, b, secondA);

    LineageWriteContext context = LineageWriteContext.forIntake(daos, null);
    assertThat(context.upsertSource("source", "type", "url-a", true, now)).isSameAs(firstA);
    assertThat(context.upsertSource("source", "type", "url-a", true, now)).isSameAs(firstA);
    assertThat(context.upsertSource("source", "type", "url-b", true, now)).isSameAs(b);
    assertThat(context.upsertSource("source", "type", "url-a", true, now)).isSameAs(secondA);

    ArgumentCaptor<String> urls = ArgumentCaptor.forClass(String.class);
    verify(sourceDao, times(3))
        .upsert(any(UUID.class), eq("type"), eq(now), eq("source"), urls.capture());
    assertThat(urls.getAllValues()).containsExactly("url-a", "url-b", "url-a");
  }

  private Dataset outputWithColumnLineage(String name) {
    return outputWithColumnLineage(
        name,
        Collections.singletonMap(
            OUTPUT_COLUMN,
            outputColumn(
                Collections.singletonList(
                    new LineageEvent.ColumnLineageInputField(
                        INPUT_NAMESPACE, INPUT_DATASET, INPUT_FIELD)))));
  }

  private Dataset outputWithColumnLineage(
      String name, Map<String, LineageEvent.ColumnLineageOutputColumn> columns) {
    return new Dataset(
        "output_namespace",
        name,
        DatasetFacets.builder()
            .columnLineage(
                new LineageEvent.ColumnLineageDatasetFacet(
                    PRODUCER_URL,
                    SCHEMA_URL,
                    new LineageEvent.ColumnLineageDatasetFacetFields(columns)))
            .build());
  }

  private LineageEvent.ColumnLineageOutputColumn outputColumn(
      List<LineageEvent.ColumnLineageInputField> inputFields) {
    return new LineageEvent.ColumnLineageOutputColumn(inputFields, "description", "type");
  }
}
