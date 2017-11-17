/*
 *
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
 *
 */
package org.apache.bookkeeper.client;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import java.io.InputStream;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.conf.ClientConfiguration;

/**
 * Ledger entry. Its a simple tuple containing the ledger id, the entry-id, and
 * the entry content.
 *
 * @Deprecated since 4.6.0, in favor of using {@link org.apache.bookkeeper.client.impl.LedgerEntryImpl}.
 */
public class LedgerEntry implements org.apache.bookkeeper.client.api.LedgerEntry {

    final LedgerEntryImpl ledgerEntry;

    LedgerEntry(long lId, long eId) {
        ledgerEntry = LedgerEntryImpl.create(lId, eId, 0L, null);
    }

    @Override
    public long getLedgerId() {
        return ledgerEntry.getLedgerId();
    }

    @Override
    public long getEntryId() {
        return ledgerEntry.getEntryId();
    }

    @Override
    public long getLength() {
        return ledgerEntry.getLength();
    }

    /**
     * Returns the content of the entry.
     * This method can be called only once. While using v2 wire protocol this method will automatically release
     * the internal ByteBuf
     * 
     * @return the content of the entry
     * @throws IllegalStateException if this method is called twice
     */
    @Override
    public byte[] getEntry() {
        ByteBuf data = ledgerEntry.getEntryBuffer();
        Preconditions.checkState(null != data, "entry content can be accessed only once");
        byte[] entry = new byte[data.readableBytes()];
        data.readBytes(entry);
        data.release();
        return entry;
    }

    /**
     * Returns the content of the entry.
     * This method can be called only once. While using v2 wire protocol this method will automatically release
     * the internal ByteBuf when calling the close
     * method of the returned InputStream
     *
     * @return an InputStream which gives access to the content of the entry
     * @throws IllegalStateException if this method is called twice
     */
    public InputStream getEntryInputStream() {
        ByteBuf data = ledgerEntry.getEntryBuffer();
        Preconditions.checkState(null != data, "entry content can be accessed only once");
        ByteBufInputStream res = new ByteBufInputStream(data);
        return res;
    }

    /**
     * Return the internal buffer that contains the entry payload.
     *
     * Note: Using v2 wire protocol it is responsibility of the caller to ensure to release the buffer after usage.
     *
     * @return a ByteBuf which contains the data
     *
     * @see ClientConfiguration#setNettyUsePooledBuffers(boolean)
     * @throws IllegalStateException if the entry has been retrieved by {@link #getEntry()}
     * or {@link #getEntryInputStream()}.
     */
    @Override
    public ByteBuf getEntryBuffer() {
        ByteBuf data = ledgerEntry.getEntryBuffer();
        Preconditions.checkState(null != data, "entry content has been retrieved" +
            " by #getEntry or #getEntryInputStream");
        return data;
    }

    @Override
    public org.apache.bookkeeper.client.api.LedgerEntry duplicate() {
        // new method in api, not implemented
        return null;
    }

    @Override
    public void close() throws Exception {
        // new method in api, not implemented
    }
}
