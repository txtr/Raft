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

/**
 * Service factory.
 */
public interface ServiceFactory<T> {

    /**
     * Returns a service for the given member.
     *
     * @param member the member
     * @return the service for the given member
     */
    T getService(String member);

    /**
     * Returns a service for the given member.
     *
     * @param host the member host
     * @param port the member port
     * @return the service for the given member
     */
    T getService(String host, int port);

}
