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
import reactor.core.composable.Promise;

/**
 * An {@link Endpoint} represents the lowest layer before crossing the IO boundary.
 *
 * The {@link Endpoint} is responsible for interacting with the IO layer that handles the raw incoming requests and
 * responses. It is responsible for setting up the appropriate codec pipelines so that the semantically correct
 * requests get translated into raw requests, understandable by the IO framework.
 *
 * Every endpoint exposes its state so that the upper layer can always orchestrate it based on the state. Strictly
 * speaking, a endpoint represents a underlying socket. This means that if multiplexing is needed, the upper layer
 * ({@link com.couchbase.client.core.service.Service} has to handle it.
 *
 * Note that sending will only work if the endpoint is in the {@link LifecycleState#CONNECTED} state. If not, the future
 * will return an error (and the upper layer should close this endpoint, create a new one or choose another one,
 * depending on the semantics needed).
 */
public interface Endpoint extends Stateful<LifecycleState> {

    /**
     * Connect the endpoint to its underlying socket/channel representation.
     *
     * @return the {@link LifecycleState} at the end of the connect phase.
     */
    Promise<LifecycleState> connect();

    /**
     * Send a {@link CouchbaseRequest} and receive a {@link CouchbaseResponse}.
     *
     * @param request the request to send.
     * @param <R> the exact type of the expected response.
     * @return the response message.
     */
    <R extends CouchbaseResponse> Promise<R> send(CouchbaseRequest request);

    /**
     * Disconnect the endpoint and close the underlying socket/channel representation.
     *
     * @return the {@link LifecycleState} at the end of the disconnect phase.
     */
    Promise<LifecycleState> disconnect();

}
