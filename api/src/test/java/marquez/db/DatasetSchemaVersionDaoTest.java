/*
 * Copyright 2018-2024 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static marquez.db.OpenLineageDefaults.DEFAULT_NAMESPACE_OWNER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import org.mockito.ArgumentCaptor;

@ExtendWith(MarquezJdbiExternalPostgresExtension.class)
class DatasetSchemaVersionDaoTest {
  private static final Instant CREATED_AT = Instant.parse("2024-01-01T00:00:00Z");

  private static Jdbi jdbi;
  private static DatasetSchemaVersionDao schemaVersionDao;
  private static DatasetFieldDao fieldDao;
  private static DatasetDao datasetDao;
  private static NamespaceDao namespaceDao;
  private static SourceDao sourceDao;

  private DatasetRow dataset;

  @BeforeAll
  static void setUpOnce(Jdbi jdbi) {
    DatasetSchemaVersionDaoTest.jdbi = jdbi;
    schemaVersionDao = jdbi.onDemand(DatasetSchemaVersionDao.class);
    fieldDao = jdbi.onDemand(DatasetFieldDao.class);
    datasetDao = jdbi.onDemand(DatasetDao.class);
    namespaceDao = jdbi.onDemand(NamespaceDao.class);
    sourceDao = jdbi.onDemand(SourceDao.class);
  }

  @BeforeEach
  void setUp() {
    NamespaceRow namespace =
        namespaceDao.upsertNamespaceRow(
            UUID.randomUUID(), CREATED_AT, "schema-version-test", DEFAULT_NAMESPACE_OWNER);
    SourceRow source =
        sourceDao.upsertOrDefault(
            UUID.randomUUID(), "POSTGRES", CREATED_AT, "schema-version-test-source", "");
    dataset =
        datasetDao.upsert(
            UUID.randomUUID(),
            DatasetType.DB_TABLE,
            CREATED_AT,
            namespace.getUuid(),
            namespace.getName(),
            source.getUuid(),
            source.getName(),
            "schema-version-test-dataset",
            "schema-version-test-dataset");
  }

  @AfterEach
  void tearDown() {
    JdbiUtils.cleanDatabase(jdbi);
  }

  @Test
  void upsertFieldMappingsSkipsEmptyInputAndUsesBoundedUuidArrays() {
    DatasetSchemaVersionDao batchingDao = mock(DatasetSchemaVersionDao.class, CALLS_REAL_METHODS);
    UUID schemaVersionUuid = UUID.randomUUID();

    batchingDao.upsertFieldMappingsInTransaction(schemaVersionUuid, List.of());

    verify(batchingDao, never()).insertFieldMappingsChunk(eq(schemaVersionUuid), any(UUID[].class));

    List<UUID> fieldUuids = new ArrayList<>();
    for (int index = 0; index <= DatasetSchemaVersionDao.MAX_FIELD_MAPPINGS_PER_INSERT; index++) {
      fieldUuids.add(new UUID(0L, index));
    }

    batchingDao.upsertFieldMappingsInTransaction(schemaVersionUuid, fieldUuids);

    ArgumentCaptor<UUID[]> chunks = ArgumentCaptor.forClass(UUID[].class);
    verify(batchingDao, times(2)).insertFieldMappingsChunk(eq(schemaVersionUuid), chunks.capture());
    assertThat(chunks.getAllValues().get(0))
        .containsExactlyInAnyOrderElementsOf(
            fieldUuids.subList(0, DatasetSchemaVersionDao.MAX_FIELD_MAPPINGS_PER_INSERT));
    assertThat(chunks.getAllValues().get(1))
        .containsExactlyInAnyOrderElementsOf(
            fieldUuids.subList(
                DatasetSchemaVersionDao.MAX_FIELD_MAPPINGS_PER_INSERT, fieldUuids.size()));
  }

  @Test
  void upsertFieldMappingsUsesSetSemantics() {
    UUID schemaVersionUuid = UUID.randomUUID();
    schemaVersionDao
        .upsertSchemaVersion(schemaVersionUuid, dataset.getUuid(), CREATED_AT)
        .orElseThrow();
    DatasetFieldRow first = newField("first-mapping");
    DatasetFieldRow second = newField("second-mapping");

    schemaVersionDao.upsertFieldMappings(
        schemaVersionUuid,
        List.of(second.getUuid(), first.getUuid(), second.getUuid(), first.getUuid()));
    schemaVersionDao.upsertFieldMappings(
        schemaVersionUuid, List.of(first.getUuid(), second.getUuid()));

    assertThat(findMappedFieldUuids(schemaVersionUuid))
        .containsExactlyInAnyOrder(first.getUuid(), second.getUuid());
  }

  @Test
  void upsertFieldMappingsRollsBackEarlierChunksWhenALaterChunkFails() {
    UUID schemaVersionUuid = UUID.randomUUID();
    schemaVersionDao
        .upsertSchemaVersion(schemaVersionUuid, dataset.getUuid(), CREATED_AT)
        .orElseThrow();
    List<UUID> fieldUuids =
        newFields(DatasetSchemaVersionDao.MAX_FIELD_MAPPINGS_PER_INSERT).stream()
            .map(DatasetFieldRow::getUuid)
            .collect(Collectors.toCollection(ArrayList::new));
    fieldUuids.add(UUID.randomUUID());

    assertThatThrownBy(() -> schemaVersionDao.upsertFieldMappings(schemaVersionUuid, fieldUuids))
        .isInstanceOf(RuntimeException.class);

    assertThat(findMappedFieldUuids(schemaVersionUuid)).isEmpty();
  }

  @Test
  void upsertSchemaVersionMapsFieldsOnlyWhenTheSchemaIsNew() {
    DatasetFieldRow field = newField("stable-schema");

    UUID firstVersion =
        schemaVersionDao.upsertSchemaVersion(dataset, List.of(field), CREATED_AT).getValue();
    UUID replayedVersion =
        jdbi.inTransaction(
                handle ->
                    handle
                        .attach(DatasetSchemaVersionDao.class)
                        .upsertSchemaVersionInTransaction(
                            dataset, List.of(field), CREATED_AT.plusSeconds(60)))
            .getValue();

    assertThat(replayedVersion).isEqualTo(firstVersion);
    assertThat(findMappedFieldUuids(firstVersion)).containsExactly(field.getUuid());
    assertThat(countSchemaVersions()).isEqualTo(1);
  }

  @Test
  void upsertSchemaVersionRollsBackTheSchemaWhenAFieldMappingFails() {
    List<DatasetFieldRow> fields =
        new ArrayList<>(newFields(DatasetSchemaVersionDao.MAX_FIELD_MAPPINGS_PER_INSERT));
    fields.add(
        new DatasetFieldRow(
            UUID.randomUUID(),
            "STRING",
            CREATED_AT,
            CREATED_AT,
            dataset.getUuid(),
            "missing-field",
            List.of(),
            null));

    assertThatThrownBy(() -> schemaVersionDao.upsertSchemaVersion(dataset, fields, CREATED_AT))
        .isInstanceOf(RuntimeException.class);

    assertThat(countSchemaVersions()).isZero();
  }

  @Test
  void upsertSchemaVersionInTransactionParticipatesInOuterRollback() {
    DatasetFieldRow field = newField("outer-rollback");

    assertThatThrownBy(
            () ->
                jdbi.useTransaction(
                    handle -> {
                      handle
                          .attach(DatasetSchemaVersionDao.class)
                          .upsertSchemaVersionInTransaction(dataset, List.of(field), CREATED_AT);
                      throw new IllegalStateException("rollback");
                    }))
        .isInstanceOf(IllegalStateException.class);

    assertThat(countSchemaVersions()).isZero();
  }

  private DatasetFieldRow newField(String name) {
    return fieldDao.upsert(UUID.randomUUID(), CREATED_AT, name, "STRING", null, dataset.getUuid());
  }

  private List<DatasetFieldRow> newFields(int count) {
    List<DatasetFieldUpsert> fields = new ArrayList<>(count);
    for (int index = 0; index < count; index++) {
      fields.add(
          new DatasetFieldUpsert(
              UUID.randomUUID(),
              CREATED_AT,
              CREATED_AT,
              dataset.getUuid(),
              "schema-rollback-field-" + index,
              "STRING",
              null));
    }
    return fieldDao.upsertAll(fields);
  }

  private List<UUID> findMappedFieldUuids(UUID schemaVersionUuid) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT dataset_field_uuid "
                        + "FROM dataset_schema_versions_field_mapping "
                        + "WHERE dataset_schema_version_uuid = :schemaVersionUuid")
                .bind("schemaVersionUuid", schemaVersionUuid)
                .mapTo(UUID.class)
                .list());
  }

  private int countSchemaVersions() {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery(
                    "SELECT count(*) FROM dataset_schema_versions WHERE dataset_uuid = :uuid")
                .bind("uuid", dataset.getUuid())
                .mapTo(Integer.class)
                .one());
  }
}
