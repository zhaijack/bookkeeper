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

import static org.apache.bookkeeper.util.BookKeeperConstants.LAYOUT_ZNODE;

import java.io.IOException;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * Provide utils for writing/reading/deleting layout in zookeeper.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class ZkLedgerLayoutUtils {

    /**
     * Read ledger layout from zookeeper.
     *
     * @param zk            ZooKeeper Client
     * @param ledgersRoot   Root of the ledger namespace to check
     * @return ledger layout, or null if none set in zookeeper
     */
    public static LedgerLayout readLayout(final ZooKeeper zk, final String ledgersRoot)
            throws KeeperException, InterruptedException, IOException {
        String ledgersLayout = ledgersRoot + "/" + LAYOUT_ZNODE;
        byte[] layoutData = zk.getData(ledgersLayout, false, null);
        return LedgerLayout.parseLayout(layoutData);
    }

    /**
     * Store the ledger layout to zookeeper.
     *
     * @param layout ledger layout to store in zookeeper
     * @param zk zookeeper client
     * @param ledgersRoot ledgers root path
     * @param zkAcls list of zokeeper acls
     */
    public static void storeLayout(LedgerLayout layout,
                                   ZooKeeper zk,
                                   String ledgersRoot,
                                   List<ACL> zkAcls)
            throws IOException, KeeperException, InterruptedException {
        String ledgersLayout = ledgersRoot + "/" + LAYOUT_ZNODE;
        zk.create(ledgersLayout, layout.serialize(), zkAcls,
            CreateMode.PERSISTENT);
    }

    /**
     * Delete the ledger layout from zookeeper.
     *
     * @param zk zookeeper client
     * @param ledgersRoot ledgers root path
     */
    public static void deleteLayout(ZooKeeper zk,
                                    String ledgersRoot)
            throws KeeperException, InterruptedException {
        String ledgersLayout = ledgersRoot + "/" + LAYOUT_ZNODE;
        zk.delete(ledgersLayout, -1);
    }

}
