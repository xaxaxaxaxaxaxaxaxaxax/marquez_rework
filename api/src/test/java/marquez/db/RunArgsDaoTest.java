/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import marquez.db.models.RunArgsRow;
import org.junit.jupiter.api.Test;

class RunArgsDaoTest {

  @Test
  void checksumHitReturnsExistingRowWithoutAnInsertAttempt() {
    RunArgsDao dao = mock(RunArgsDao.class, CALLS_REAL_METHODS);
    UUID proposedUuid = UUID.randomUUID();
    Instant now = Instant.parse("2024-01-01T00:00:00Z");
    RunArgsRow existing = new RunArgsRow(UUID.randomUUID(), now.minusSeconds(1), "{}", "sum");
    when(dao.findRunArgsByChecksum("sum")).thenReturn(Optional.of(existing));

    assertThat(dao.upsertRunArgs(proposedUuid, now, "{}", "sum")).isSameAs(existing);

    verify(dao, never()).insertRunArgs(proposedUuid, now, "{}", "sum");
  }

  @Test
  void checksumMissReturnsTheInsertedRowWithoutASecondSelect() {
    RunArgsDao dao = mock(RunArgsDao.class, CALLS_REAL_METHODS);
    UUID proposedUuid = UUID.randomUUID();
    Instant now = Instant.parse("2024-01-01T00:00:00Z");
    RunArgsRow inserted = new RunArgsRow(proposedUuid, now, "{}", "sum");
    when(dao.findRunArgsByChecksum("sum")).thenReturn(Optional.empty());
    when(dao.insertRunArgs(proposedUuid, now, "{}", "sum")).thenReturn(Optional.of(inserted));

    assertThat(dao.upsertRunArgs(proposedUuid, now, "{}", "sum")).isSameAs(inserted);

    verify(dao).findRunArgsByChecksum("sum");
    verify(dao).insertRunArgs(proposedUuid, now, "{}", "sum");
  }

  @Test
  void concurrentInsertLoserSelectsAndReturnsTheWinningRow() {
    RunArgsDao dao = mock(RunArgsDao.class, CALLS_REAL_METHODS);
    UUID proposedUuid = UUID.randomUUID();
    Instant now = Instant.parse("2024-01-01T00:00:00Z");
    RunArgsRow winner = new RunArgsRow(UUID.randomUUID(), now.minusMillis(1), "{}", "sum");
    when(dao.findRunArgsByChecksum("sum")).thenReturn(Optional.empty(), Optional.of(winner));
    when(dao.insertRunArgs(proposedUuid, now, "{}", "sum")).thenReturn(Optional.empty());

    assertThat(dao.upsertRunArgs(proposedUuid, now, "{}", "sum")).isSameAs(winner);

    verify(dao, times(2)).findRunArgsByChecksum("sum");
    verify(dao).insertRunArgs(proposedUuid, now, "{}", "sum");
  }
}
