/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.client.meta;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.resolver.SimpleNameResolverFactory;
import org.apache.bookkeeper.client.utils.RpcUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.proto.rpc.common.StatusCode;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerIdAllocateRequest;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerIdAllocateResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceFutureStub;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class RpcLedgerManagerFactory extends LedgerManagerFactory {

    private static final int CUR_VERSION = 1;

    private final Optional<ManagedChannelBuilder> channelBuilder;

    // variables initialized by {@link #initialize()}
    ClientConfiguration conf;
    ManagedChannel channel;
    LedgerMetadataServiceFutureStub lmService;

    RpcLedgerManagerFactory() {
        this(Optional.empty());
    }

    @VisibleForTesting
    RpcLedgerManagerFactory(Optional<ManagedChannelBuilder> channelBuilder) {
        this.channelBuilder = channelBuilder;
    }

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public LedgerManagerFactory initialize(AbstractConfiguration conf, ZooKeeper zk, int factoryVersion)
            throws IOException {
        if (CUR_VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : " + factoryVersion);
        }
        this.conf = new ClientConfiguration();
        this.conf.loadConf(conf);

        // initialize the channel
        // TODO: initialize TLS channel
        ManagedChannelBuilder builder = channelBuilder.orElse(
            NettyChannelBuilder
                .forTarget("bookie")
                .nameResolverFactory(SimpleNameResolverFactory.of(this.conf.getClientBootstrapBookies()))
                .usePlaintext(true));
        this.channel = builder.build();

        // configure the ledger metadata rpc service
        this.lmService = RpcUtils.configureRpcStub(
            LedgerMetadataServiceGrpc.newFutureStub(channel),
            Optional.empty());
        return this;
    }

    @Override
    public void uninitialize() throws IOException {
        if (null != channel) {
            channel.shutdown();
        }
    }

    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        return new RpcLedgerIdGenerator(lmService);
    }

    @Override
    public LedgerManager newLedgerManager() {
        return null;
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager()
            throws KeeperException, InterruptedException, CompatibilityException {
        throw new UnsupportedOperationException("Ledger Underreplicated Manager is not supported yet");
    }

    private static class RpcLedgerIdGenerator implements LedgerIdGenerator {

        private static final LedgerIdAllocateRequest ALLOCATE_REQUEST = LedgerIdAllocateRequest.newBuilder().build();

        private final LedgerMetadataServiceFutureStub lmService;

        RpcLedgerIdGenerator(LedgerMetadataServiceFutureStub lmService) {
            this.lmService = lmService;
        }

        @Override
        public void generateLedgerId(GenericCallback<Long> cb) {
            Futures.addCallback(
                lmService.allocate(ALLOCATE_REQUEST),
                new FutureCallback<LedgerIdAllocateResponse>() {

                    @Override
                    public void onSuccess(LedgerIdAllocateResponse resp) {
                        if (StatusCode.SUCCESS == resp.getCode()) {
                            cb.operationComplete(Code.OK, resp.getLedgerId());
                        } else {
                            cb.operationComplete(Code.MetaStoreException, null);
                        }
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        cb.operationComplete(Code.MetaStoreException, null);
                    }
                });
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }

    private static class RpcLedgerManager implements LedgerManager {

        private final LedgerMetadataServiceFutureStub lmService;

        RpcLedgerManager(LedgerMetadataServiceFutureStub lmService) {
            this.lmService = lmService;
        }

        @Override
        public void createLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb) {
            LedgerMetadataRequest request = LedgerMetadataRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setExpectedVersion(-1L)
                .setMetadata(metadata.toProtoFormat())
                .build();

            Futures.addCallback(
                lmService.create(request),
                new FutureCallback<LedgerMetadataResponse>() {
                    @Override
                    public void onSuccess(LedgerMetadataResponse ledgerMetadataResponse) {

                    }

                    @Override
                    public void onFailure(Throwable throwable) {

                    }
                }
            );
        }

        @Override
        public void removeLedgerMetadata(long ledgerId, Version version, GenericCallback<Void> vb) {

        }

        @Override
        public void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> readCb) {

        }

        @Override
        public void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb) {

        }

        @Override
        public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {

        }

        @Override
        public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {

        }

        @Override
        public void asyncProcessLedgers(Processor<Long> processor,
                                        VoidCallback finalCb,
                                        Object context,
                                        int successRc,
                                        int failureRc) {

        }

        @Override
        public LedgerRangeIterator getLedgerRanges() {
            return null;
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }
}
