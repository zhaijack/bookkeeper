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
package org.apache.bookkeeper.client.meta;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Maps;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.utils.RpcUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.rpc.common.StatusCode;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceFutureStub;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceImplBase;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceStub;
import org.apache.bookkeeper.versioning.LongVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRpcLedgerManager {
    private final static Logger LOG = LoggerFactory.getLogger(TestRpcLedgerManager.class);

    private final String serverName = "fake server for " + getClass();
    private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
    private Server fakeServer;
    private RpcLedgerManager rpcLedgerManager;
    private ManagedChannel channel;
    LedgerMetadataServiceStub lmService;
    LedgerMetadataServiceFutureStub lmFutureService;
    Resource<OrderedScheduler> schedulerResource = new Resource<OrderedScheduler>() {
        private static final String name = "test-rpc-scheduler";

        @Override
        public OrderedScheduler create() {
            return OrderedScheduler.newSchedulerBuilder()
              .numThreads(Runtime.getRuntime().availableProcessors() * 2)
              .name(name)
              .build();
        }

        @Override
        public void close(OrderedScheduler instance) {
            instance.shutdown();
        }

        @Override
        public String toString() {
            return name;
        }
    };

    LedgerMetadata createLedgerMetadata() {
        long version = System.currentTimeMillis();
        Map<String, byte[]> customMetadata = Maps.newHashMap();
        customMetadata.put("test_key", "test_value".getBytes(UTF_8));
        LedgerMetadata metadata = new LedgerMetadata(5,
          3,
          2,
          BookKeeper.DigestType.CRC32,
          "password".getBytes(UTF_8),
          customMetadata);
        metadata.setVersion(new LongVersion(version));

        return metadata;
    }

    @Before
    public void setUp() throws Exception {
        fakeServer = InProcessServerBuilder
          .forName(serverName)
          .fallbackHandlerRegistry(serviceRegistry)
          .directExecutor()
          .build()
          .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        Optional<String> token =  Optional.empty();
        lmService = RpcUtils.configureRpcStub(
            LedgerMetadataServiceGrpc.newStub(channel),
            token);
        lmFutureService = RpcUtils.configureRpcStub(
            LedgerMetadataServiceGrpc.newFutureStub(channel),
            Optional.empty());

        rpcLedgerManager = new RpcLedgerManager(lmService, lmFutureService, schedulerResource);
    }
    @After
    public void tearDown() throws Exception {
        if (null != rpcLedgerManager) {
            rpcLedgerManager.close();
        }
        if (null != fakeServer) {
            fakeServer.shutdown();
        }
    }

    @Test
    public void testCreateLedgerMetadata() {
        long ledgerId = System.currentTimeMillis();
        long version = System.currentTimeMillis();
        LedgerMetadata metadata = createLedgerMetadata();
        final List<Integer> resultCode = new ArrayList<Integer>(1);
        GenericCallback<Void> callback = new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                resultCode.add(rc);
            }
        };

        LedgerMetadataResponse response = LedgerMetadataResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setVersion(version)
            .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase () {
            @Override
            public void create(org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest request,
                               StreamObserver<LedgerMetadataResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());

        rpcLedgerManager.createLedgerMetadata(ledgerId, metadata, callback);
        assertEquals(StatusCode.SUCCESS, resultCode.get(0));
        assertEquals(version, metadata.getVersion());
    }

}
