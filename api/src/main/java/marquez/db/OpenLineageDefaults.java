/*
 * Copyright 2018-2026 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import java.util.Map;
import marquez.common.Utils;

/** Shared defaults used while projecting OpenLineage events into the Marquez model. */
public final class OpenLineageDefaults {
  public static final String DEFAULT_SOURCE_NAME = "default";
  public static final String DEFAULT_NAMESPACE_OWNER = "anonymous";
  public static final String EMPTY_RUN_ARGS_JSON = Utils.toJson(Map.of());
  public static final String EMPTY_RUN_ARGS_CHECKSUM = Utils.checksumFor(Map.of());

  private OpenLineageDefaults() {}
}
