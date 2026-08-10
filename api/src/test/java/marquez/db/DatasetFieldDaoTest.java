/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.OpenLineageDao.DEFAULT_NAMESPACE_OWNER;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import marquez.api.JdbiUtils;
import marquez.common.models.DatasetType;
import marquez.db.DatasetFieldDao.DatasetFieldUpsert;
import marquez.db.models.DatasetFieldRow;
import marquez.db.models.DatasetRow;
import marquez.db.models.NamespaceRow;
import marquez.db.models.SourceRow;
import marquez.jdbi.MarquezJdbiExternalPostgresExtension;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class DatasetFieldDaoTest {
  private static final Instant CREATED_AT = Instant.parse("2024-01-01T00:00:00Z");
  private static final Instant LATER = CREATED_AT.plusSeconds(60);

  private static Jdbi jdbi;
  private static DatasetFieldDao fieldDao;
  private static DatasetDao datasetDao;
  private static NamespaceDao namespaceDao;
  private static SourceDao sourceDao;

  private DatasetRow dataset;

  @BeforeAll
  static void setUpOnce(Jdbi jdbi) {
    DatasetFieldDaoTest.jdbi = jdbi;
    fieldDao = jdbi.onDemand(DatasetFieldDao.class);
    datasetDao = jdbi.onDemand(DatasetDao.class);
    namespaceDao = jdbi.onDemand(NamespaceDao.class);
    sourceDao = jdbi.onDemand(SourceDao.class);
  }

  @BeforeEach
  void setUp() {
    NamespaceRow namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), CREATED_AT, "field-test", DEFAULT_NAMESPACE_OWNER);
    SourceRow source =
        sourceDao.upsertOrDefault(
            UUID.randomUUID(), "POSTGRES", CREATED_AT, "field-test-source", "");
    dataset =
        datasetDao.upsert(
            UUID.randomUUID(),
            DatasetType.DB_TABLE,
            CREATED_AT,
            namespace.getUuid(),
            namespace.getName(),
            source.getUuid(),
            source.getName(),
            "field-test-dataset",
            "field-test-dataset");
  }

  @AfterEach
  void tearDown() {
    JdbiUtils.cleanDatabase(jdbi);
  }

  @Test
  void upsertAllPreservesOccurrenceOrderAndPostgresNullSemantics() {
    UUID zetaUuid = UUID.randomUUID();
    UUID alphaFirstUuid = UUID.randomUUID();
    UUID alphaDiscardedUuid = UUID.randomUUID();
    UUID firstNullUuid = UUID.randomUUID();
    UUID secondNullUuid = UUID.randomUUID();
    List<DatasetFieldUpsert> inputs =
        List.of(
            field(zetaUuid, CREATED_AT, "zeta", "STRING", "zeta"),
            field(alphaFirstUuid, CREATED_AT, "alpha", "STRING", "old"),
            field(firstNullUuid, CREATED_AT, "nullable", null, "first"),
            field(alphaDiscardedUuid, LATER, "alpha", "STRING", "new"),
            field(secondNullUuid, LATER, "nullable", null, "second"));

    List<DatasetFieldRow> rows = fieldDao.upsertAll(inputs);

    assertThat(rows)
        .extracting(DatasetFieldRow::getUuid)
        .containsExactly(zetaUuid, alphaFirstUuid, firstNullUuid, alphaFirstUuid, secondNullUuid);
    assertThat(rows.get(1).getCreatedAt()).isEqualTo(CREATED_AT);
    assertThat(rows.get(1).getUpdatedAt()).isEqualTo(LATER);
    assertThat(rows.get(1).getDescription()).contains("new");
    assertThat(rows.get(3)).isEqualTo(rows.get(1));
    assertThat(countFields()).isEqualTo(4);
  }

  @Test
  void upsertAllReturnsStoredUuidForAnExistingNonNullIdentity() {
    UUID storedUuid = UUID.randomUUID();
    DatasetFieldRow stored =
        fieldDao.upsert(storedUuid, CREATED_AT, "existing", "STRING", "old", dataset.getUuid());

    DatasetFieldRow updated =
        fieldDao
            .upsertAll(
                List.of(field(UUID.randomUUID(), LATER, "existing", "STRING", "new description")))
            .get(0);

    assertThat(updated.getUuid()).isEqualTo(storedUuid);
    assertThat(updated.getCreatedAt()).isEqualTo(stored.getCreatedAt());
    assertThat(updated.getUpdatedAt()).isEqualTo(LATER);
    assertThat(updated.getDescription()).contains("new description");
    assertThat(countFields()).isEqualTo(1);
  }

  @Test
  void upsertAllChunksAfterGlobalDeduplicationAndRestoresInputOrder() {
    UUID sharedUuid = UUID.randomUUID();
    List<DatasetFieldUpsert> inputs = new ArrayList<>();
    inputs.add(field(sharedUuid, CREATED_AT, "shared", "STRING", "old"));
    for (int index = 1000; index >= 1; index--) {
      inputs.add(
          field(
              UUID.randomUUID(),
              CREATED_AT,
              String.format("field-%04d", index),
              "STRING",
              "description"));
    }
    inputs.add(field(UUID.randomUUID(), LATER, "shared", "STRING", "new"));

    List<DatasetFieldRow> rows = fieldDao.upsertAll(inputs);

    assertThat(rows).hasSize(inputs.size());
    assertThat(rows.get(0).getUuid()).isEqualTo(sharedUuid);
    assertThat(rows.get(rows.size() - 1).getUuid()).isEqualTo(sharedUuid);
    assertThat(rows.get(rows.size() - 1).getUpdatedAt()).isEqualTo(LATER);
    assertThat(rows.get(rows.size() - 1).getDescription()).contains("new");
    assertThat(rows.subList(1, rows.size() - 1))
        .extracting(DatasetFieldRow::getUuid)
        .containsExactlyElementsOf(
            inputs.subList(1, inputs.size() - 1).stream()
                .map(DatasetFieldUpsert::getUuid)
                .collect(Collectors.toList()));
    assertThat(countFields()).isEqualTo(1001);
  }

  @Test
  void upsertAllAcceptsAnEmptyList() {
    assertThat(fieldDao.upsertAll(List.of())).isEmpty();
    assertThat(countFields()).isZero();
  }

  private DatasetFieldUpsert field(
      UUID uuid, Instant updatedAt, String name, String type, String description) {
    return new DatasetFieldUpsert(
        uuid, CREATED_AT, updatedAt, dataset.getUuid(), name, type, description);
  }

  private int countFields() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT count(*) FROM dataset_fields WHERE dataset_uuid = :uuid")
                .bind("uuid", dataset.getUuid())
                .mapTo(Integer.class)
                .one());
  }
}
