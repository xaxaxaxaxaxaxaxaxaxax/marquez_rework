/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.models;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.NonNull;
import lombok.Value;

/** An immutable, cumulative view of the input and output dataset versions associated with a run. */
@Value
public class RunIoSnapshot {
  private static final RunIoSnapshot EMPTY = new RunIoSnapshot(List.of(), List.of());

  @NonNull ImmutableList<ExtendedDatasetVersionRow> inputs;
  @NonNull ImmutableList<ExtendedDatasetVersionRow> outputs;

  public RunIoSnapshot(
      List<ExtendedDatasetVersionRow> inputs, List<ExtendedDatasetVersionRow> outputs) {
    this.inputs = ImmutableList.copyOf(inputs);
    this.outputs = ImmutableList.copyOf(outputs);
  }

  public static RunIoSnapshot empty() {
    return EMPTY;
  }

  public static RunIoSnapshot from(Iterable<RunIoRow> rows) {
    ImmutableList.Builder<ExtendedDatasetVersionRow> inputs = ImmutableList.builder();
    ImmutableList.Builder<ExtendedDatasetVersionRow> outputs = ImmutableList.builder();
    for (RunIoRow row : rows) {
      switch (row.getIoType()) {
        case INPUT -> inputs.add(row.getDatasetVersion());
        case OUTPUT -> outputs.add(row.getDatasetVersion());
      }
    }
    ImmutableList<ExtendedDatasetVersionRow> inputRows = inputs.build();
    ImmutableList<ExtendedDatasetVersionRow> outputRows = outputs.build();
    return inputRows.isEmpty() && outputRows.isEmpty()
        ? empty()
        : new RunIoSnapshot(inputRows, outputRows);
  }
}
