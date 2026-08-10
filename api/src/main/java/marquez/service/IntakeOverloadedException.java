/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

/** Indicates that an OpenLineage event could not be admitted for processing. */
public class IntakeOverloadedException extends RuntimeException {
  public IntakeOverloadedException(Throwable cause) {
    super("OpenLineage intake is at capacity", cause);
  }
}
