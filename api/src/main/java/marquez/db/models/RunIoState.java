/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.models;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

/** The ordered dataset-version occurrences projected by one OpenLineage event for one run side. */
@Getter
public final class RunIoState {
  private final UUID runUuid;
  private final IoType ioType;
  private final ProjectionOrder order;
  private final ImmutableList<UUID> datasetVersionUuids;

  public RunIoState(
      UUID runUuid, IoType ioType, ProjectionOrder order, List<UUID> datasetVersionUuids) {
    this.runUuid = Objects.requireNonNull(runUuid, "runUuid");
    this.ioType = Objects.requireNonNull(ioType, "ioType");
    this.order = Objects.requireNonNull(order, "order");
    this.datasetVersionUuids =
        ImmutableList.copyOf(Objects.requireNonNull(datasetVersionUuids, "datasetVersionUuids"));
  }

  public RunIoState(
      UUID runUuid,
      IoType ioType,
      Instant eventTime,
      byte[] eventKey,
      List<UUID> datasetVersionUuids) {
    this(runUuid, ioType, new ProjectionOrder(eventTime, eventKey), datasetVersionUuids);
  }

  public Instant getEventTime() {
    return order.getEventTime();
  }

  public byte[] getEventKey() {
    return order.getEventKey();
  }

  /** Array-valued bean property used by the set-based DAO write. */
  public UUID[] getDatasetVersionUuidArray() {
    return datasetVersionUuids.toArray(UUID[]::new);
  }

  public enum IoType {
    INPUT,
    OUTPUT
  }
}
