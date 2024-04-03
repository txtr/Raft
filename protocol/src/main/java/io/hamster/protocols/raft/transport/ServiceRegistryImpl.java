/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hamster.protocols.raft.transport;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.util.MutableHandlerRegistry;
import io.hamster.utils.Managed;
import io.hamster.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;


/**
 * Service registry implementation.
 */
public class ServiceRegistryImpl implements ServiceRegistry, Managed {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistryImpl.class);

    private final ClusterService clusterService;
    private final MutableHandlerRegistry registry = new MutableHandlerRegistry();
    private int port;
    private Server server;

    public ServiceRegistryImpl(ClusterService clusterService) {
        this.clusterService = clusterService;
    }


    @Override
    public void register(BindableService service) {
        registry.addService(service);
    }

    @Override
    public void register(ServerServiceDefinition service) {
        registry.addService(service);
    }

    @Override
    public CompletableFuture<Void> start() {
        if (port == 0) {
            port = clusterService.getLocalNode().port();
        }
        server = ServerBuilder.forPort(port)
                .fallbackHandlerRegistry(registry)
                .build();
        try {
            server.start();
        } catch (IOException e) {
            return Futures.exceptionalFuture(e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isRunning() {
        return !server.isShutdown();
    }

    @Override
    public CompletableFuture<Void> stop() {
        if (server != null) {
            server.shutdownNow();
        }
        return CompletableFuture.completedFuture(null);
    }
}
