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
package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.Stateful;
import rx.Observable;

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

}
