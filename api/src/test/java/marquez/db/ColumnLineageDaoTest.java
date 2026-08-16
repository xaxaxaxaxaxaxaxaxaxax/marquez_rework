/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.ColumnLineageTestUtils.createLineage;
import static marquez.db.ColumnLineageTestUtils.getDatasetA;
import static marquez.db.ColumnLineageTestUtils.getDatasetB;
import static marquez.db.ColumnLineageTestUtils.getDatasetC;
import static marquez.db.LineageTestUtils.PRODUCER_URL;
import static marquez.db.LineageTestUtils.SCHEMA_URL;
import static marquez.db.OpenLineageDefaults.DEFAULT_NAMESPACE_OWNER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import marquez.common.models.DatasetType;
import marquez.db.ColumnLineageDao.ColumnLineageDatasetWrite;
import marquez.db.ColumnLineageDao.ColumnLineageWrite;
import marquez.db.models.ColumnLineageNodeData;
import marquez.db.models.ColumnLineageRow;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.InputFieldNodeData;
import marquez.db.models.NamespaceRow;
import marquez.db.models.SourceRow;
import marquez.db.models.UpdateLineageRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.Dataset;
import org.apache.commons.lang3.tuple.Pair;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
public class ColumnLineageDaoTest {

  private static OpenLineageDao openLineageDao;
  private static ColumnLineageDao dao;
  private static DatasetFieldDao fieldDao;
  private static DatasetDao datasetDao;
  private static NamespaceDao namespaceDao;
  private static SourceDao sourceDao;
  private static DatasetVersionDao datasetVersionDao;

  private UUID outputDatasetFieldUuid = UUID.randomUUID();
  private String transformationDescription = "some-description";
  private String transformationType = "some-type";
  private Instant now = Instant.now();
  private DatasetRow inputDatasetRow;
  private DatasetRow outputDatasetRow;
  private DatasetVersionRow inputDatasetVersionRow;
  private DatasetVersionRow outputDatasetVersionRow;
  private LineageEvent.JobFacet jobFacet;

  private Dataset dataset_A = getDatasetA(); // dataset_A (col_a, col_b)
  private Dataset dataset_B = getDatasetB(); // dataset_B (col_c) depends on (col_a, col_b)
  private Dataset dataset_C = getDatasetC(); // dataset_C (col_d) depends on col_c

  @BeforeAll
  public static void setUpOnce(Jdbi jdbi) {
    openLineageDao = jdbi.onDemand(OpenLineageDao.class);
    dao = jdbi.onDemand(ColumnLineageDao.class);
    fieldDao = jdbi.onDemand(DatasetFieldDao.class);
    datasetDao = jdbi.onDemand(DatasetDao.class);
    namespaceDao = jdbi.onDemand(NamespaceDao.class);
    sourceDao = jdbi.onDemand(SourceDao.class);
    datasetVersionDao = jdbi.onDemand(DatasetVersionDao.class);
  }

  @BeforeEach
  public void setup() {
    // setup some dataset
    NamespaceRow namespaceRow =
        namespaceDao.upsertNamespaceRow(UUID.randomUUID(), now, "", DEFAULT_NAMESPACE_OWNER);
    SourceRow sourceRow = sourceDao.upsertOrDefault(UUID.randomUUID(), "", now, "", "");
    inputDatasetRow =
        datasetDao.upsert(
            UUID.randomUUID(),
            DatasetType.DB_TABLE,
            now,
            namespaceRow.getUuid(),
            "",
            sourceRow.getUuid(),
            "",
            "inputDataset",
            "",
            "",
            false);
    outputDatasetRow =
        datasetDao.upsert(
            UUID.randomUUID(),
            DatasetType.DB_TABLE,
            now,
            namespaceRow.getUuid(),
            "",
            sourceRow.getUuid(),
            "",
            "outputDataset",
            "",
            "",
            false);

    inputDatasetVersionRow =
        datasetVersionDao.upsert(
            UUID.randomUUID(),
            now,
            inputDatasetRow.getUuid(),
            UUID.randomUUID(),
            null,
            UUID.randomUUID(),
            null,
            "",
            "",
            "");
    outputDatasetVersionRow =
        datasetVersionDao.upsert(
            UUID.randomUUID(),
            now,
            outputDatasetRow.getUuid(),
            UUID.randomUUID(),
            null,
            UUID.randomUUID(),
            null,
            "",
            "",
            "");

    inputDatasetVersionRow =
        datasetVersionDao.upsert(
            UUID.randomUUID(),
            now,
            inputDatasetRow.getUuid(),
            UUID.randomUUID(),
            null,
            UUID.randomUUID(),
            null,
            "",
            "",
            "");

    // insert output dataset field
    fieldDao.upsert(
        outputDatasetFieldUuid, now, "output-field", "string", "desc", outputDatasetRow.getUuid());
  }

  @AfterEach
  public void tearDown(Jdbi jdbi) {
    ColumnLineageTestUtils.tearDown(jdbi);
  }

  @Test
  void testUpsertMultipleColumns() {
    UUID inputFieldUuid1 = UUID.randomUUID();
    UUID inputFieldUuid2 = UUID.randomUUID();

    // insert input dataset fields
    fieldDao.upsert(inputFieldUuid1, now, "a", "string", "desc", inputDatasetRow.getUuid());
    fieldDao.upsert(inputFieldUuid2, now, "b", "string", "desc", inputDatasetRow.getUuid());

    List<ColumnLineageRow> rows =
        dao.upsertColumnLineageRow(
            outputDatasetVersionRow.getUuid(),
            outputDatasetFieldUuid,
            Arrays.asList(
                Pair.of(inputDatasetVersionRow.getUuid(), inputFieldUuid1),
                Pair.of(inputDatasetVersionRow.getUuid(), inputFieldUuid2)),
            transformationDescription,
            transformationType,
            now);

    assertEquals(2, rows.size());
    assertEquals(inputDatasetVersionRow.getUuid(), rows.get(0).getInputDatasetVersionUuid());
    assertEquals(outputDatasetVersionRow.getUuid(), rows.get(0).getOutputDatasetVersionUuid());
    assertEquals(outputDatasetFieldUuid, rows.get(0).getOutputDatasetFieldUuid());
    assertTrue(
        Arrays.asList(inputFieldUuid1, inputFieldUuid2)
            .contains(rows.get(0).getInputDatasetFieldUuid())); // ordering may differ per run
    assertEquals(transformationDescription, rows.get(0).getTransformationDescription().get());
    assertEquals(transformationType, rows.get(0).getTransformationType().get());
    assertEquals(now.getEpochSecond(), rows.get(0).getCreatedAt().getEpochSecond());
    assertEquals(now.getEpochSecond(), rows.get(0).getUpdatedAt().getEpochSecond());
  }

  @Test
  void testUpsertEmptyList() {
    List<ColumnLineageRow> rows =
        dao.upsertColumnLineageRow(
            UUID.randomUUID(),
            outputDatasetFieldUuid,
            Collections.emptyList(), // provide empty list
            transformationDescription,
            transformationType,
            now);

    assertEquals(0, rows.size());
  }

  @Test
  void testUpsertOnUpdatePreventsDuplicates() {
    // insert input dataset fields
    UUID inputFieldUuid = UUID.randomUUID();
    fieldDao.upsert(inputFieldUuid, now, "a", "string", "desc", inputDatasetRow.getUuid());

    dao.upsertColumnLineageRow(
        inputDatasetVersionRow.getUuid(),
        outputDatasetFieldUuid,
        Arrays.asList(Pair.of(inputDatasetVersionRow.getUuid(), inputFieldUuid)),
        transformationDescription,
        transformationType,
        now);
    List<ColumnLineageRow> rows =
        dao.upsertColumnLineageRow(
            inputDatasetVersionRow.getUuid(),
            outputDatasetFieldUuid,
            Arrays.asList(Pair.of(inputDatasetVersionRow.getUuid(), inputFieldUuid)),
            transformationDescription,
            transformationType,
            now.plusSeconds(1000));

    // make sure there is one row with updatedAt modified
    assertEquals(1, rows.size());
    assertEquals(
        now.plusSeconds(1000).getEpochSecond(), rows.get(0).getUpdatedAt().getEpochSecond());
  }

  @Test
  void testBulkUpsertReadsBackExistingEdgesWithOriginalCreatedAt() {
    UUID existingInputFieldUuid = UUID.randomUUID();
    UUID newInputFieldUuid = UUID.randomUUID();
    fieldDao.upsert(
        existingInputFieldUuid, now, "existing", "string", "desc", inputDatasetRow.getUuid());
    fieldDao.upsert(newInputFieldUuid, now, "new", "string", "desc", inputDatasetRow.getUuid());

    dao.upsertColumnLineageRow(
        outputDatasetVersionRow.getUuid(),
        outputDatasetFieldUuid,
        Collections.singletonList(
            Pair.of(inputDatasetVersionRow.getUuid(), existingInputFieldUuid)),
        transformationDescription,
        transformationType,
        now);

    List<ColumnLineageRow> rows =
        dao.upsertColumnLineageRows(
            outputDatasetVersionRow.getUuid(),
            Collections.singletonList(
                new ColumnLineageWrite(
                    outputDatasetFieldUuid,
                    Collections.singletonList(
                        Pair.of(inputDatasetVersionRow.getUuid(), newInputFieldUuid)),
                    "new-description",
                    "new-type")),
            now.plusSeconds(1000));

    assertThat(rows)
        .extracting(ColumnLineageRow::getInputDatasetFieldUuid)
        .containsExactlyInAnyOrder(existingInputFieldUuid, newInputFieldUuid);
    ColumnLineageRow existingRow =
        rows.stream()
            .filter(row -> row.getInputDatasetFieldUuid().equals(existingInputFieldUuid))
            .findFirst()
            .orElseThrow();
    assertThat(existingRow.getCreatedAt().getEpochSecond()).isEqualTo(now.getEpochSecond());
  }

  @Test
  void testIntakePublicWrapperWritesPhysicalEdges() {
    UUID inputFieldUuid = UUID.randomUUID();
    fieldDao.upsert(
        inputFieldUuid, now, "intake-wrapper", "string", "desc", inputDatasetRow.getUuid());

    dao.upsertColumnLineageRowsForIntake(
        List.of(
            new ColumnLineageDatasetWrite(
                outputDatasetVersionRow.getUuid(),
                List.of(
                    new ColumnLineageWrite(
                        outputDatasetFieldUuid,
                        List.of(Pair.of(inputDatasetVersionRow.getUuid(), inputFieldUuid)),
                        transformationDescription,
                        transformationType)))),
        now);

    assertThat(
            dao.findColumnLineageByDatasetVersionColumnAndOutputDatasetField(
                outputDatasetVersionRow.getUuid(), outputDatasetFieldUuid))
        .singleElement()
        .satisfies(
            row -> {
              assertThat(row.getInputDatasetFieldUuid()).isEqualTo(inputFieldUuid);
              assertThat(row.getTransformationDescription()).contains(transformationDescription);
              assertThat(row.getTransformationType()).contains(transformationType);
            });
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void testIntakeCoreSkipsReadbackAndDeduplicatesPhysicalEdges() {
    ColumnLineageDao intakeDao = mock(ColumnLineageDao.class, CALLS_REAL_METHODS);
    UUID outputDatasetVersionUuid = UUID.randomUUID();
    UUID outputFieldUuid = UUID.randomUUID();
    Pair<UUID, UUID> input = Pair.of(UUID.randomUUID(), UUID.randomUUID());

    intakeDao.upsertColumnLineageRowsForIntakeInTransaction(
        List.of(
            new ColumnLineageDatasetWrite(
                outputDatasetVersionUuid,
                List.of(
                    new ColumnLineageWrite(
                        outputFieldUuid, List.of(input), "first-description", "first-type"),
                    new ColumnLineageWrite(
                        outputFieldUuid, List.of(input), "last-description", "last-type")))),
        now);

    ArgumentCaptor<List<ColumnLineageRow>> rows = ArgumentCaptor.forClass(List.class);
    verify(intakeDao).doUpsertColumnLineageRow(rows.capture());
    assertThat(rows.getValue()).hasSize(1);
    assertThat(rows.getValue().get(0).getTransformationDescription()).contains("last-description");
    assertThat(rows.getValue().get(0).getTransformationType()).contains("last-type");
    verify(intakeDao, never())
        .findColumnLineageByDatasetVersionAndOutputDatasetFields(any(UUID.class), anyList());
    verify(intakeDao, never())
        .findColumnLineageByDatasetVersionColumnAndOutputDatasetField(
            any(UUID.class), any(UUID.class));
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void testIntakeCoreDeduplicatesBeforeApplyingExactBatchBoundary() {
    ColumnLineageDao intakeDao = mock(ColumnLineageDao.class, CALLS_REAL_METHODS);
    UUID outputDatasetVersionUuid = UUID.randomUUID();
    UUID outputFieldUuid = UUID.randomUUID();
    List<Pair<UUID, UUID>> inputs = new ArrayList<>(ColumnLineageDao.MAX_ROWS_PER_UPSERT);
    for (int index = 0; index < ColumnLineageDao.MAX_ROWS_PER_UPSERT; index++) {
      inputs.add(Pair.of(new UUID(0, 100), new UUID(0, index + 1)));
    }
    Pair<UUID, UUID> repeatedInput = inputs.get(0);

    intakeDao.upsertColumnLineageRowsForIntakeInTransaction(
        List.of(
            new ColumnLineageDatasetWrite(
                outputDatasetVersionUuid,
                List.of(
                    new ColumnLineageWrite(
                        outputFieldUuid, inputs, "first-description", "first-type"))),
            new ColumnLineageDatasetWrite(
                outputDatasetVersionUuid,
                List.of(
                    new ColumnLineageWrite(
                        outputFieldUuid,
                        List.of(repeatedInput),
                        "last-description",
                        "last-type")))),
        now);

    ArgumentCaptor<List<ColumnLineageRow>> rows = ArgumentCaptor.forClass(List.class);
    verify(intakeDao).doUpsertColumnLineageRow(rows.capture());
    assertThat(rows.getValue()).hasSize(ColumnLineageDao.MAX_ROWS_PER_UPSERT);
    assertThat(rows.getValue())
        .filteredOn(row -> row.getInputDatasetFieldUuid().equals(repeatedInput.getRight()))
        .singleElement()
        .satisfies(
            row -> {
              assertThat(row.getTransformationDescription()).contains("last-description");
              assertThat(row.getTransformationType()).contains("last-type");
            });
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void testCompatibilityBulkPathDeduplicatesPhysicalEdgesWithLastOccurrenceWinning() {
    ColumnLineageDao bulkDao = mock(ColumnLineageDao.class, CALLS_REAL_METHODS);
    UUID outputDatasetVersionUuid = UUID.randomUUID();
    UUID outputFieldUuid = UUID.randomUUID();
    Pair<UUID, UUID> input = Pair.of(UUID.randomUUID(), UUID.randomUUID());

    bulkDao.upsertColumnLineageRowsInTransaction(
        outputDatasetVersionUuid,
        List.of(
            new ColumnLineageWrite(
                outputFieldUuid, List.of(input), "first-description", "first-type"),
            new ColumnLineageWrite(
                outputFieldUuid, List.of(input), "last-description", "last-type")),
        now);

    ArgumentCaptor<List<ColumnLineageRow>> rows = ArgumentCaptor.forClass(List.class);
    verify(bulkDao).doUpsertColumnLineageRow(rows.capture());
    assertThat(rows.getValue()).hasSize(1);
    assertThat(rows.getValue().get(0).getTransformationDescription()).contains("last-description");
    assertThat(rows.getValue().get(0).getTransformationType()).contains("last-type");
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void testIntakeCoreUsesBoundedDeterministicBatchesAcrossOutputDatasets() {
    ColumnLineageDao intakeDao = mock(ColumnLineageDao.class, CALLS_REAL_METHODS);
    UUID firstOutputVersionUuid = new UUID(Long.MIN_VALUE, 2);
    UUID secondOutputVersionUuid = new UUID(Long.MAX_VALUE, 1);
    List<Pair<UUID, UUID>> firstInputs = new ArrayList<>();
    for (int i = ColumnLineageDao.MAX_ROWS_PER_UPSERT - 1; i > 0; i--) {
      firstInputs.add(Pair.of(new UUID(0, 100), new UUID(0, i)));
    }
    List<Pair<UUID, UUID>> secondInputs =
        List.of(
            Pair.of(new UUID(0, 200), new UUID(0, 2)), Pair.of(new UUID(0, 200), new UUID(0, 1)));

    intakeDao.upsertColumnLineageRowsForIntakeInTransaction(
        List.of(
            new ColumnLineageDatasetWrite(
                firstOutputVersionUuid,
                List.of(
                    new ColumnLineageWrite(new UUID(0, 20), firstInputs, "description", "type"))),
            new ColumnLineageDatasetWrite(
                secondOutputVersionUuid,
                List.of(
                    new ColumnLineageWrite(new UUID(0, 10), secondInputs, "description", "type")))),
        now);

    ArgumentCaptor<List<ColumnLineageRow>> batches = ArgumentCaptor.forClass(List.class);
    verify(intakeDao, times(2)).doUpsertColumnLineageRow(batches.capture());
    assertThat(batches.getAllValues())
        .extracting(List::size)
        .containsExactly(ColumnLineageDao.MAX_ROWS_PER_UPSERT, 1);
    Comparator<ColumnLineageRow> physicalEdgeOrder =
        ColumnLineageDaoTest::comparePhysicalEdgesInPostgresOrder;
    List<ColumnLineageRow> allRows =
        batches.getAllValues().stream().flatMap(List::stream).collect(Collectors.toList());
    assertThat(allRows).isSortedAccordingTo(physicalEdgeOrder);
    assertThat(allRows.get(0).getOutputDatasetVersionUuid()).isEqualTo(secondOutputVersionUuid);
    assertThat(allRows.get(allRows.size() - 1).getOutputDatasetVersionUuid())
        .isEqualTo(firstOutputVersionUuid);
    assertThat(batches.getAllValues().get(0))
        .extracting(ColumnLineageRow::getOutputDatasetVersionUuid)
        .contains(firstOutputVersionUuid, secondOutputVersionUuid);
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void testIntakeCoreUsesUnsignedUuidLeastSignificantBitsOrder() {
    ColumnLineageDao intakeDao = mock(ColumnLineageDao.class, CALLS_REAL_METHODS);
    UUID firstInPostgresOrder = new UUID(0, Long.MAX_VALUE);
    UUID secondInPostgresOrder = new UUID(0, Long.MIN_VALUE);

    intakeDao.upsertColumnLineageRowsForIntakeInTransaction(
        List.of(
            new ColumnLineageDatasetWrite(
                UUID.randomUUID(),
                List.of(
                    new ColumnLineageWrite(
                        UUID.randomUUID(),
                        List.of(
                            Pair.of(new UUID(0, 1), secondInPostgresOrder),
                            Pair.of(new UUID(0, 1), firstInPostgresOrder)),
                        "description",
                        "type")))),
        now);

    ArgumentCaptor<List<ColumnLineageRow>> rows = ArgumentCaptor.forClass(List.class);
    verify(intakeDao).doUpsertColumnLineageRow(rows.capture());
    assertThat(rows.getValue())
        .extracting(ColumnLineageRow::getInputDatasetFieldUuid)
        .containsExactly(firstInPostgresOrder, secondInPostgresOrder);
  }

  @Test
  void testIntakeCoreKeepsLastOccurrenceAcrossExactBatchBoundary(Jdbi jdbi) {
    List<Pair<UUID, UUID>> inputs =
        insertPhysicalInputs(jdbi, ColumnLineageDao.MAX_ROWS_PER_UPSERT, "boundary");
    Pair<UUID, UUID> repeatedInput = inputs.get(0);

    writeIntakeInTransaction(
        jdbi,
        List.of(
            new ColumnLineageDatasetWrite(
                outputDatasetVersionRow.getUuid(),
                List.of(
                    new ColumnLineageWrite(
                        outputDatasetFieldUuid, inputs, "first-description", "first-type"),
                    new ColumnLineageWrite(
                        outputDatasetFieldUuid,
                        List.of(repeatedInput),
                        "last-description",
                        "last-type")))));

    List<ColumnLineageRow> rows =
        dao.findColumnLineageByDatasetVersionColumnAndOutputDatasetField(
            outputDatasetVersionRow.getUuid(), outputDatasetFieldUuid);
    assertThat(rows).hasSize(ColumnLineageDao.MAX_ROWS_PER_UPSERT);
    assertThat(rows)
        .filteredOn(row -> row.getInputDatasetFieldUuid().equals(repeatedInput.getRight()))
        .singleElement()
        .satisfies(
            row -> {
              assertThat(row.getTransformationDescription()).contains("last-description");
              assertThat(row.getTransformationType()).contains("last-type");
            });
  }

  @Test
  void testIntakeCoreReliesOnOuterTransactionForChunkRollback(Jdbi jdbi) {
    List<Pair<UUID, UUID>> inputs =
        new ArrayList<>(
            insertPhysicalInputs(jdbi, ColumnLineageDao.MAX_ROWS_PER_UPSERT, "intake-rollback"));
    inputs.add(Pair.of(UUID.randomUUID(), UUID.randomUUID()));

    assertThatThrownBy(
            () ->
                writeIntakeInTransaction(
                    jdbi,
                    List.of(
                        new ColumnLineageDatasetWrite(
                            outputDatasetVersionRow.getUuid(),
                            List.of(
                                new ColumnLineageWrite(
                                    outputDatasetFieldUuid,
                                    inputs,
                                    transformationDescription,
                                    transformationType))))))
        .isInstanceOf(RuntimeException.class);

    assertThat(
            dao.findColumnLineageByDatasetVersionColumnAndOutputDatasetField(
                outputDatasetVersionRow.getUuid(), outputDatasetFieldUuid))
        .isEmpty();
  }

  @Test
  void testBulkUpsertRollsBackEarlierChunksWhenLaterChunkFails(Jdbi jdbi) {
    List<UUID> validInputFieldUuids = new ArrayList<>(ColumnLineageDao.MAX_ROWS_PER_UPSERT);
    jdbi.useHandle(
        handle -> {
          PreparedBatch fields =
              handle.prepareBatch(
                  """
                  INSERT INTO dataset_fields (
                    uuid, type, created_at, updated_at, dataset_uuid, name, description
                  ) VALUES (
                    :uuid, :type, :now, :now, :datasetUuid, :name, :description
                  )
                  """);
          for (int i = 0; i < ColumnLineageDao.MAX_ROWS_PER_UPSERT; i++) {
            UUID fieldUuid = UUID.randomUUID();
            validInputFieldUuids.add(fieldUuid);
            fields
                .bind("uuid", fieldUuid)
                .bind("type", "string")
                .bind("now", now)
                .bind("datasetUuid", inputDatasetRow.getUuid())
                .bind("name", "rollback-field-" + i)
                .bind("description", "desc")
                .add();
          }
          fields.execute();
        });

    List<Pair<UUID, UUID>> inputs = new ArrayList<>(ColumnLineageDao.MAX_ROWS_PER_UPSERT + 1);
    for (UUID fieldUuid : validInputFieldUuids) {
      inputs.add(Pair.of(inputDatasetVersionRow.getUuid(), fieldUuid));
    }
    inputs.add(Pair.of(UUID.randomUUID(), UUID.randomUUID()));

    assertThatThrownBy(
            () ->
                dao.upsertColumnLineageRows(
                    outputDatasetVersionRow.getUuid(),
                    Collections.singletonList(
                        new ColumnLineageWrite(
                            outputDatasetFieldUuid,
                            inputs,
                            transformationDescription,
                            transformationType)),
                    now))
        .isInstanceOf(RuntimeException.class);
    assertThat(
            dao.findColumnLineageByDatasetVersionColumnAndOutputDatasetField(
                outputDatasetVersionRow.getUuid(), outputDatasetFieldUuid))
        .isEmpty();
  }

  @Test
  void testGetLineage() {
    createLineage(openLineageDao, dataset_A, dataset_B);
    UpdateLineageRow lineageRow = createLineage(openLineageDao, dataset_B, dataset_C);
    Set<ColumnLineageNodeData> lineage = getColumnLineage(lineageRow, "col_d");

    assertEquals(2, lineage.size());

    ColumnLineageNodeData dataset_b =
        lineage.stream().filter(cd -> cd.getDataset().equals("dataset_b")).findAny().get();
    ColumnLineageNodeData dataset_c =
        lineage.stream().filter(cd -> cd.getDataset().equals("dataset_c")).findAny().get();

    // test dataset_c
    assertThat(dataset_c.getInputFields()).hasSize(1);
    assertEquals("col_d", dataset_c.getField());
    assertEquals("namespace", dataset_c.getInputFields().get(0).getNamespace());
    assertEquals("dataset_b", dataset_c.getInputFields().get(0).getDataset());
    assertEquals("col_c", dataset_c.getInputFields().get(0).getField());
    assertEquals("type2", dataset_c.getInputFields().get(0).getTransformationType());
    assertEquals("description2", dataset_c.getInputFields().get(0).getTransformationDescription());

    // test dataset_b
    assertThat(dataset_b.getInputFields()).hasSize(2);
    assertEquals("col_c", dataset_b.getField());
    assertEquals(
        "col_b",
        dataset_b.getInputFields().stream()
            .filter(f -> f.getField().equals("col_b"))
            .findAny()
            .get()
            .getField());
    assertEquals(
        "col_a",
        dataset_b.getInputFields().stream()
            .filter(f -> f.getField().equals("col_a"))
            .findAny()
            .get()
            .getField());

    assertEquals("namespace", dataset_b.getInputFields().get(0).getNamespace());
    assertEquals("dataset_a", dataset_b.getInputFields().get(0).getDataset());
    assertEquals("type1", dataset_b.getInputFields().get(0).getTransformationType());
    assertEquals("description1", dataset_b.getInputFields().get(0).getTransformationDescription());
  }

  @Test
  void testGetLineageWhenNoLineageForColumn() {
    UpdateLineageRow lineageRow = createLineage(openLineageDao, dataset_A, dataset_B);
    createLineage(openLineageDao, dataset_B, dataset_C);

    UpdateLineageRow.DatasetRecord datasetRecord_a = lineageRow.getInputs().get().get(0);
    UUID field_col_a = fieldDao.findUuid(datasetRecord_a.getDatasetRow().getUuid(), "col_a").get();

    // assert lineage is empty
    assertThat(dao.getLineage(20, Collections.singletonList(field_col_a), false, Instant.now()))
        .isEmpty();
  }

  /**
   * Create dataset_d build on the topi of dataset_c. Lineage of depth 1 of dataset_d should be of
   * size 2 (instead of 3)
   */
  @Test
  void testGetLineageWithLimitedDepth() {
    Dataset dataset_D =
        new Dataset(
            "namespace",
            "dataset_d",
            LineageEvent.DatasetFacets.builder()
                .schema(
                    new LineageEvent.SchemaDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        Arrays.asList(new LineageEvent.SchemaField("col_e", "STRING", ""))))
                .columnLineage(
                    new LineageEvent.ColumnLineageDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        new LineageEvent.ColumnLineageDatasetFacetFields(
                            Collections.singletonMap(
                                "col_e",
                                new LineageEvent.ColumnLineageOutputColumn(
                                    Arrays.asList(
                                        new LineageEvent.ColumnLineageInputField(
                                            "namespace", "dataset_c", "col_d")),
                                    "",
                                    "")))))
                .build());

    createLineage(openLineageDao, dataset_A, dataset_B);
    createLineage(openLineageDao, dataset_B, dataset_C);
    UpdateLineageRow lineageRow = createLineage(openLineageDao, dataset_C, dataset_D);

    UpdateLineageRow.DatasetRecord datasetRecord_d = lineageRow.getOutputs().get().get(0);
    UUID field_col_e = fieldDao.findUuid(datasetRecord_d.getDatasetRow().getUuid(), "col_e").get();

    // make sure dataset are constructed properly
    assertThat(dao.getLineage(20, Collections.singletonList(field_col_e), false, Instant.now()))
        .hasSize(3);

    // depth 1 corresponds to single ColumnLineageData with other nodes as node inputFields
    assertThat(dao.getLineage(1, Collections.singletonList(field_col_e), false, Instant.now()))
        .hasSize(1);
  }

  @Test
  void testGetLineageWhenCycleExists() {
    Dataset dataset_A =
        new Dataset(
            "namespace",
            "dataset_a",
            LineageEvent.DatasetFacets.builder()
                .schema(
                    new LineageEvent.SchemaDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        Arrays.asList(
                            new LineageEvent.SchemaField("col_a", "STRING", ""),
                            new LineageEvent.SchemaField("col_b", "STRING", ""))))
                .columnLineage(
                    new LineageEvent.ColumnLineageDatasetFacet(
                        PRODUCER_URL,
                        SCHEMA_URL,
                        new LineageEvent.ColumnLineageDatasetFacetFields(
                            Collections.singletonMap(
                                "col_a",
                                new LineageEvent.ColumnLineageOutputColumn(
                                    Arrays.asList(
                                        new LineageEvent.ColumnLineageInputField(
                                            "namespace", "dataset_c", "col_d")),
                                    "description3",
                                    "type3")))))
                .build());

    createLineage(openLineageDao, dataset_A, dataset_B);
    createLineage(openLineageDao, dataset_B, dataset_C);
    UpdateLineageRow lineageRow = createLineage(openLineageDao, dataset_C, dataset_A);

    UpdateLineageRow.DatasetRecord datasetRecord_a = lineageRow.getOutputs().get().get(0);
    UpdateLineageRow.DatasetRecord datasetRecord_c = lineageRow.getInputs().get().get(0);

    UUID field_col_a = fieldDao.findUuid(datasetRecord_a.getDatasetRow().getUuid(), "col_a").get();
    UUID field_col_d = fieldDao.findUuid(datasetRecord_c.getDatasetRow().getUuid(), "col_d").get();

    // column lineages for col_a and col_e should be of size 3
    assertThat(dao.getLineage(20, Collections.singletonList(field_col_a), false, Instant.now()))
        .hasSize(3);
    assertThat(dao.getLineage(20, Collections.singletonList(field_col_d), false, Instant.now()))
        .hasSize(3);
  }

  /**
   * Run two jobs that write to dataset_b using dataset_a and dataset_c. Both input fields should be
   * returned
   */
  @Test
  void testGetLineageWhenTwoJobsWriteToSameDataset() {
    List<LineageEvent.ColumnLineageInputField> fields =
        getDatasetB()
            .getFacets()
            .getColumnLineage()
            .getFields()
            .getAdditionalFacets()
            .get("col_c")
            .getInputFields();

    Dataset datasetWithColAAsInputField = getDatasetB();
    datasetWithColAAsInputField
        .getFacets()
        .getColumnLineage()
        .getFields()
        .getAdditionalFacets()
        .get("col_c")
        .setInputFields(Collections.singletonList(fields.get(0)));
    createLineage(openLineageDao, getDatasetA(), datasetWithColAAsInputField);

    Dataset datasetWithColBAsInputField = getDatasetB();
    datasetWithColBAsInputField
        .getFacets()
        .getColumnLineage()
        .getFields()
        .getAdditionalFacets()
        .get("col_c")
        .setInputFields(Collections.singletonList(fields.get(1)));
    UpdateLineageRow lineageRow =
        createLineage(openLineageDao, getDatasetA(), datasetWithColBAsInputField);

    // assert input fields for col_c contain col_a and col_b
    List<String> inputFields =
        getColumnLineage(lineageRow, "col_c").stream()
            .filter(node -> node.getDataset().equals("dataset_b"))
            .flatMap(node -> node.getInputFields().stream())
            .map(input -> input.getField())
            .collect(Collectors.toList());

    assertThat(inputFields).hasSize(2).contains("col_a", "col_b");
  }

  @Test
  void testGetLineagePointInTime() {
    UpdateLineageRow lineageRow = createLineage(openLineageDao, dataset_A, dataset_B);

    UpdateLineageRow.DatasetRecord datasetRecord_b = lineageRow.getOutputs().get().get(0);
    UUID field_col_b = fieldDao.findUuid(datasetRecord_b.getDatasetRow().getUuid(), "col_c").get();
    Instant columnLineageCreatedAt =
        dao.findColumnLineageByDatasetVersionColumnAndOutputDatasetField(
                datasetRecord_b.getDatasetVersionRow().getUuid(), field_col_b)
            .get(0)
            .getCreatedAt();

    // assert lineage is empty before and present after
    assertThat(
            dao.getLineage(
                20,
                Collections.singletonList(field_col_b),
                false,
                columnLineageCreatedAt.minusSeconds(1)))
        .isEmpty();
    assertThat(
            dao.getLineage(
                20,
                Collections.singletonList(field_col_b),
                false,
                columnLineageCreatedAt.plusSeconds(1)))
        .hasSize(1);
  }

  @Test
  void testGetLineageWhenJobRunMultipleTimes() {
    createLineage(openLineageDao, dataset_A, dataset_B);
    createLineage(openLineageDao, dataset_A, dataset_B);
    UpdateLineageRow lineageRow = createLineage(openLineageDao, dataset_A, dataset_B);

    Set<ColumnLineageNodeData> columnLineage = getColumnLineage(lineageRow, "col_c");
    assertThat(columnLineage).hasSize(1);

    ColumnLineageNodeData node = columnLineage.stream().findAny().get();

    assertThat(node.getDatasetVersion())
        .isEqualTo(lineageRow.getOutputs().get().get(0).getDatasetVersionRow().getUuid());
    assertThat(node.getInputFields().get(0).getDatasetVersion())
        .isEqualTo(lineageRow.getInputs().get().get(0).getDatasetVersionRow().getUuid());
  }

  @Test
  void testGetLineageWhenDataTypeIsEmpty() {
    Dataset datasetWithNullDataType = getDatasetB();
    datasetWithNullDataType.getFacets().getSchema().getFields().get(0).setType(null);

    UpdateLineageRow lineageRow = createLineage(openLineageDao, dataset_A, datasetWithNullDataType);
    getColumnLineage(lineageRow, "col_c");
  }

  @Test
  void testGetLineageRowsForDatasetsWhenMultipleJobsWriteToADataset() {
    List<LineageEvent.ColumnLineageInputField> fields =
        getDatasetB()
            .getFacets()
            .getColumnLineage()
            .getFields()
            .getAdditionalFacets()
            .get("col_c")
            .getInputFields();

    Dataset datasetWithColAAsInputField = getDatasetB();
    datasetWithColAAsInputField
        .getFacets()
        .getColumnLineage()
        .getFields()
        .getAdditionalFacets()
        .get("col_c")
        .setInputFields(Collections.singletonList(fields.get(0)));
    createLineage(openLineageDao, getDatasetA(), datasetWithColAAsInputField);

    Dataset datasetWithColBAsInputField = getDatasetB();
    datasetWithColBAsInputField
        .getFacets()
        .getColumnLineage()
        .getFields()
        .getAdditionalFacets()
        .get("col_c")
        .setInputFields(Collections.singletonList(fields.get(1)));
    createLineage(openLineageDao, getDatasetA(), datasetWithColBAsInputField);

    List<InputFieldNodeData> inputFields =
        dao
            .getLineageRowsForDatasets(Collections.singletonList(Pair.of("namespace", "dataset_b")))
            .stream()
            .findAny()
            .get()
            .getInputFields();
    assertThat(inputFields).hasSize(2); // should contain col_a and col_b
  }

  private List<Pair<UUID, UUID>> insertPhysicalInputs(
      Jdbi jdbi, int count, String fieldNamePrefix) {
    List<Pair<UUID, UUID>> inputs = new ArrayList<>(count);
    String uniquePrefix = fieldNamePrefix + "-" + UUID.randomUUID() + "-";
    jdbi.useHandle(
        handle -> {
          PreparedBatch fields =
              handle.prepareBatch(
                  """
                  INSERT INTO dataset_fields (
                    uuid, type, created_at, updated_at, dataset_uuid, name, description
                  ) VALUES (
                    :uuid, :type, :now, :now, :datasetUuid, :name, :description
                  )
                  """);
          for (int index = 0; index < count; index++) {
            UUID fieldUuid = UUID.randomUUID();
            inputs.add(Pair.of(inputDatasetVersionRow.getUuid(), fieldUuid));
            fields
                .bind("uuid", fieldUuid)
                .bind("type", "string")
                .bind("now", now)
                .bind("datasetUuid", inputDatasetRow.getUuid())
                .bind("name", uniquePrefix + index)
                .bind("description", "desc")
                .add();
          }
          fields.execute();
        });
    return inputs;
  }

  private void writeIntakeInTransaction(Jdbi jdbi, List<ColumnLineageDatasetWrite> writes) {
    jdbi.useTransaction(
        handle ->
            handle
                .attach(ColumnLineageDao.class)
                .upsertColumnLineageRowsForIntakeInTransaction(writes, now));
  }

  private static int comparePhysicalEdgesInPostgresOrder(
      ColumnLineageRow left, ColumnLineageRow right) {
    int compared =
        compareUuidsInPostgresOrder(
            left.getOutputDatasetVersionUuid(), right.getOutputDatasetVersionUuid());
    if (compared != 0) {
      return compared;
    }
    compared =
        compareUuidsInPostgresOrder(
            left.getOutputDatasetFieldUuid(), right.getOutputDatasetFieldUuid());
    if (compared != 0) {
      return compared;
    }
    compared =
        compareUuidsInPostgresOrder(
            left.getInputDatasetVersionUuid(), right.getInputDatasetVersionUuid());
    return compared != 0
        ? compared
        : compareUuidsInPostgresOrder(
            left.getInputDatasetFieldUuid(), right.getInputDatasetFieldUuid());
  }

  private static int compareUuidsInPostgresOrder(UUID left, UUID right) {
    int compared =
        Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
    return compared != 0
        ? compared
        : Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
  }

  private Set<ColumnLineageNodeData> getColumnLineage(UpdateLineageRow lineageRow, String field) {
    UpdateLineageRow.DatasetRecord datasetRecord = lineageRow.getOutputs().get().get(0);
    UUID field_UUID = fieldDao.findUuid(datasetRecord.getDatasetRow().getUuid(), field).get();

    return dao.getLineage(20, Collections.singletonList(field_UUID), false, Instant.now());
  }
}
