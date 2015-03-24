/**
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.client.core.service;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import rx.Subscriber;
import rx.functions.Action1;

import java.util.concurrent.atomic.AtomicReference;

/**
 * This service lazily creates an Endpoint if needed and reuses it.
 *
 * If no storedEndpoint is currently found, a new one is created and stored - the request is dispatched to it. When
 * subsequent requests come along they are dispatched to the same storedEndpoint. If the storedEndpoint dies for some reason
 * it is cleaned up and a new one is created on the next attempt.
 *
 * @author Michael Nitschinger
 * @since 1.1.2
 */
public abstract class AbstractLazyService extends AbstractDynamicService {

    private final AtomicReference<Endpoint> storedEndpoint = new AtomicReference<Endpoint>();

    public AbstractLazyService(String hostname, String bucket, String password, int port, CoreEnvironment env,
        RingBuffer<ResponseEvent> responseBuffer, EndpointFactory endpointFactory) {
        super(hostname, bucket, password, port, env, 0, responseBuffer, endpointFactory);
    }

    @Override
    protected void dispatch(final CouchbaseRequest request) {
        if (storedEndpoint.get() == null) {
            final Endpoint newEndpoint = createEndpoint();
            if (storedEndpoint.compareAndSet(null, newEndpoint)) {
                endpointStates().register(newEndpoint, newEndpoint);
                newEndpoint
                    .connect()
                    .subscribe(new Subscriber<LifecycleState>() {
                        @Override
                        public void onCompleted() {
                        }

                        @Override
                        public void onError(Throwable e) {
                            request.observable().onError(e);
                        }

                        @Override
                        public void onNext(LifecycleState lifecycleState) {
                            if (lifecycleState == LifecycleState.DISCONNECTED) {
                                request.observable().onError(new CouchbaseException("Could not connect storedEndpoint."));
                            }
                        }
                    });

                whenState(newEndpoint, LifecycleState.DISCONNECTED, new Action1<LifecycleState>() {
                    @Override
                    public void call(LifecycleState lifecycleState) {
                        endpointStates().deregister(newEndpoint);
                        storedEndpoint.set(null);
                    }
                });

            } else {
                newEndpoint.disconnect();
            }
        }

        sendAndFlushWhenConnected(storedEndpoint.get(), request);
    }

    /**
     * Helper method to send the request and also a flush afterwards.
     *
     * @param endpoint the endpoint to send the reques to.
     * @param request the reques to send.
     */
    private static void sendAndFlushWhenConnected(final Endpoint endpoint, final CouchbaseRequest request) {
        whenState(endpoint, LifecycleState.CONNECTED, new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState lifecycleState) {
                endpoint.send(request);
                endpoint.send(SignalFlush.INSTANCE);
            }
        });
    }

    /**
     * Exposes the current endpoint to a subclass for proper testing.
     *
     * @return the endpoint or null if not set.
     */
    Endpoint endpoint() {
        return storedEndpoint.get();
    }

}
