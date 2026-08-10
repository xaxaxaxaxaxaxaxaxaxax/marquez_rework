/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import lombok.NonNull;
import org.postgresql.util.PGobject;

final class FacetUtils {

  private FacetUtils() {}

  static PGobject toPgObject(@NonNull Object facetContainer) {
    return Columns.toPgObject(facetContainer);
  }

  static boolean isEmpty(@NonNull PGobject facetContainer) {
    return "{}".equals(facetContainer.getValue());
  }
}
