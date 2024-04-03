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

import io.grpc.Channel;

import java.util.function.Function;

/**
 * gRPC service provider.
 */
public interface ServiceProvider {

    /**
     * Returns a new service factory for the given stub factory.
     *
     * @param factory the stub factory
     * @param <T>     the service type
     * @return the service factory
     */
    <T> ServiceFactory<T> getFactory(Function<Channel, T> factory);

}
