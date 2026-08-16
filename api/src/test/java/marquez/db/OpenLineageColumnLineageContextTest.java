/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import marquez.db.ColumnLineageDao.ColumnLineageWrite;
import marquez.db.models.ColumnLineageRow;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Boundary tests for column-lineage batching that are independent of projector internals. */
class OpenLineageColumnLineageContextTest {

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
}
