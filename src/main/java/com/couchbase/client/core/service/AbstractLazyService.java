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

import com.couchbase.client.core.CoreContext;
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

    protected AbstractLazyService(String hostname, String bucket, String username, String password, int port, CoreContext ctx, EndpointFactory endpointFactory) {
        super(hostname, bucket, username, password, port, ctx, 0, endpointFactory);
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
