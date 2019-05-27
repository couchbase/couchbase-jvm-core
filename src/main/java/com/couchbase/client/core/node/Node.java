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
package com.couchbase.client.core.node;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.EndpointHealth;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.Stateful;
import com.couchbase.client.core.utils.NetworkAddress;
import rx.Observable;

import java.util.List;
import java.util.Map;

/**
 * Represents a Couchbase Node.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public interface Node extends Stateful<LifecycleState> {

    /**
     * Sends a {@link CouchbaseRequest} into the node and eventually returns a {@link CouchbaseResponse}.
     *
     * The {@link CouchbaseResponse} is not returned directly, but is wrapped into a {@link Observable}.
     *
     * @param request the request to send.
     */
    void send(CouchbaseRequest request);

    /**
     * Returns the configured hostname for the {@link Node}.
     *
     * @return the hostname.
     */
    String hostname();

    /**
     * Connects all currently enabled {@link Service}s.
     *
     * @return the states of the {@link Node} after the connect process for all enabled {@link Service}s.
     */
    Observable<LifecycleState> connect();

    /**
     * Disconnects all currently enabled {@link Service}s.
     *
     * @return the states of the {@link Node} after the disconnect process for all enabled {@link Service}s.
     */
    Observable<LifecycleState> disconnect();

    Observable<Service> addService(AddServiceRequest request);

    Observable<Service> removeService(RemoveServiceRequest request);

    /**
     * True if the given {@link ServiceType} is currently enabled on this node, false otherwise.
     */
    boolean serviceEnabled(ServiceType type);

    /**
     * Returns endpoint health information for all endpoints this node is currently associated with.
     */
    Observable<EndpointHealth> diagnostics();

}
