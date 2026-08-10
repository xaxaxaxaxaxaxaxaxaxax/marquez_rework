/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.models;

import lombok.NonNull;
import lombok.Value;

/** A dataset version associated with a run, tagged with its lineage direction. */
@Value
public class RunIoRow {
  @NonNull IoType ioType;
  @NonNull ExtendedDatasetVersionRow datasetVersion;

  public enum IoType {
    INPUT,
    OUTPUT
  }
}
