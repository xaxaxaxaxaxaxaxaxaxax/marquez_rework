/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.service;

/** Indicates that a statistics query is incomplete, inconsistent, or outside supported bounds. */
public final class InvalidStatsQueryException extends IllegalArgumentException {
  private static final long serialVersionUID = 1L;

  public InvalidStatsQueryException(String message) {
    super(message);
  }

  public InvalidStatsQueryException(String message, Throwable cause) {
    super(message, cause);
  }
}
