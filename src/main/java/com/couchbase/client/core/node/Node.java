/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.node;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.Stateful;
import rx.Observable;

import java.net.InetAddress;

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
    InetAddress hostname();

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

}
