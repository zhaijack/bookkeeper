/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.meta;

import static org.apache.bookkeeper.meta.ZkLedgerLayoutUtils.deleteLayout;
import static org.apache.bookkeeper.meta.ZkLedgerLayoutUtils.readLayout;
import static org.apache.bookkeeper.meta.ZkLedgerLayoutUtils.storeLayout;
import static org.apache.bookkeeper.util.BookKeeperConstants.LAYOUT_ZNODE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.junit.Test;

/**
 * Unit test of {@link ZkLedgerLayoutUtils}.
 */
public class TestZkLayoutManager {

    private static final String ledgersRootPath = "/path/to/ledgers/root";
    private static final String layoutPath = ledgersRootPath + "/" + LAYOUT_ZNODE;
    private static final int managerVersion = 78;

    private final ZooKeeper zk;
    private final LedgerLayout layout;

    public TestZkLayoutManager() {
        this.zk = mock(ZooKeeper.class);
        this.layout = new LedgerLayout(
            HierarchicalLedgerManagerFactory.class.getName(),
            managerVersion);
    }

    @Test
    public void testReadLayout() throws Exception {
        when(zk.getData(eq(layoutPath), eq(false), eq(null)))
            .thenReturn(layout.serialize());

        assertEquals(layout, readLayout(zk, ledgersRootPath));
    }

    @Test
    public void testStoreLayout() throws Exception {
        List<ACL> acls = Ids.OPEN_ACL_UNSAFE;

        storeLayout(layout, zk, ledgersRootPath, acls);

        verify(zk, times(1))
            .create(eq(layoutPath), eq(layout.serialize()), eq(acls), eq(CreateMode.PERSISTENT));
    }

    @Test
    public void testDeleteLayout() throws Exception {
        deleteLayout(zk, ledgersRootPath);

        verify(zk, times(1))
            .delete(eq(layoutPath), eq(-1));
    }

}
