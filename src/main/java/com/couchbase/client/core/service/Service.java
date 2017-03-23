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
package com.couchbase.client.core.service;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.Stateful;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;

/**
 * Represents a {@link Service} on a {@link Node}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface Service extends Stateful<LifecycleState> {

    /**
     * Sends a {@link CouchbaseRequest} into the service and eventually returns a {@link CouchbaseResponse}.
     *
     * The {@link CouchbaseResponse} is not returned directly, but is wrapped into a {@link Observable}.
     *
     * @param request the request to send.
     */
    void send(CouchbaseRequest request);

    /**
     * Returns the mapping for the given {@link Service}.
     *
     * @return the mapping.
     */
    BucketServiceMapping mapping();

    /**
     * Returns the type for the given {@link Service}.
     *
     * @return the type.
     */
    ServiceType type();

    /**
     * Connects all currently enabled {@link Service} {@link Endpoint}s.
     *
     * @return the states of the {@link Service} after the connect process for all enabled {@link Endpoint}s.
     */
    Observable<LifecycleState> connect();

    /**
     * Disconnects all currently enabled {@link Service} {@link Endpoint}s.
     *
     * @return the states of the {@link Service} after the disconnect process for all enabled {@link Endpoint}s.
     */
    Observable<LifecycleState> disconnect();

    /**
     * A helper factory which generates endpoints.
     */
    interface EndpointFactory {
        /**
         * Create a new {@link Endpoint}.
         *
         * @param hostname the hostname of the endpoint.
         * @param bucket the bucket name of the endpoint.
         * @param username the user authorized for bucket access.
         * @param password the password of the user.
         * @param port the port of the endpoint.
         * @param env the shared environment.
         * @param responseBuffer the response buffer for messages.
         * @return a new {@link Endpoint}.
         */
        Endpoint create(String hostname, String bucket, String username, String password, int port, CoreEnvironment env,
            RingBuffer<ResponseEvent> responseBuffer);

    }
}
