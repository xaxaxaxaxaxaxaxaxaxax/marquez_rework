/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db.models;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import marquez.db.BaseDao;
import marquez.db.NamespaceDao;
import org.junit.jupiter.api.Test;

class ModelDaosTest {
  @Test
  void requiresItsBaseDaoAtConstructionAndCreatesEachChildOnce() {
    BaseDao baseDao = mock(BaseDao.class);
    NamespaceDao namespaceDao = mock(NamespaceDao.class);
    when(baseDao.createNamespaceDao()).thenReturn(namespaceDao);

    ModelDaos daos = new ModelDaos(baseDao);

    assertThat(daos.getNamespaceDao()).isSameAs(namespaceDao);
    assertThat(daos.getNamespaceDao()).isSameAs(namespaceDao);
    verify(baseDao, times(1)).createNamespaceDao();
    assertThatNullPointerException().isThrownBy(() -> new ModelDaos(null));
  }
}
