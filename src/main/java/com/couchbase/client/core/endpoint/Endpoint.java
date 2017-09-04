/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.internal.EndpointHealth;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.Stateful;
import rx.Observable;
import rx.Single;

/**
 * Represents a stateful {@link Endpoint} which abstracts the underlying channel.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface Endpoint extends Stateful<LifecycleState> {

    /**
     * Connect the {@link Endpoint} to the underlying channel.
     *
     * @return a {@link Observable} with the state after the connect process finishes.
     */
    Observable<LifecycleState> connect();

    /**
     * Disconnect the {@link Endpoint} from the underlying channel.
     *
     * @return a {@link Observable} with the state after the disconnect process finishes.
     */
    Observable<LifecycleState> disconnect();

    /**
     * Sends a {@link CouchbaseRequest} into the endpoint and eventually returns a {@link CouchbaseResponse}.
     *
     * @param request the request to send.
     */
    void send(CouchbaseRequest request);

    /**
     * If this endpoint is free to take a request. This is especially important in non-pipelined
     * endpoint cases since if a request is in-flight this will return false.
     *
     * @return true if free to accept a request, false otherwise.
     */
    boolean isFree();

    /**
     * Returns the timestamp of the last response completed.
     */
    long lastResponse();

    /**
     * Returns health information for this endpoint.
     */
    Single<EndpointHealth> healthCheck(ServiceType type);

}
