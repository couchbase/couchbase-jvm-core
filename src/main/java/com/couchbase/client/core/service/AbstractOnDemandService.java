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
import rx.functions.Func1;

/**
 * Abstract implementation of a service which enables and disables endpoints on demand.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public abstract class AbstractOnDemandService extends AbstractDynamicService {

    protected AbstractOnDemandService(String hostname, String bucket, String password, int port, CoreEnvironment env,
        RingBuffer<ResponseEvent> responseBuffer, EndpointFactory endpointFactory) {
        super(hostname, bucket, password, port, env, 0, responseBuffer, endpointFactory);
    }

    @Override
    protected void dispatch(final CouchbaseRequest request) {
        final Endpoint endpoint = createEndpoint();
        endpointStates().register(endpoint, endpoint);

        endpoint
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
                        request.observable().onError(new CouchbaseException("Could not connect endpoint."));
                    }
                }
            });

        whenState(endpoint, LifecycleState.CONNECTED, new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState lifecycleState) {
                endpoint.send(request);
                endpoint.send(SignalFlush.INSTANCE);
            }
        });

        whenState(endpoint, LifecycleState.DISCONNECTED, new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState lifecycleState) {
                endpointStates().deregister(endpoint);
            }
        });
    }

    /**
     * Waits until the endpoint goes into the given state, calls the action and then unsubscribes.
     *
     * @param endpoint the endpoint to monitor.
     * @param wanted the wanted state.
     * @param then the action to execute when the state is reached.
     */
    private void whenState(final Endpoint endpoint, final LifecycleState wanted, Action1<LifecycleState> then) {
        endpoint
            .states()
            .filter(new Func1<LifecycleState, Boolean>() {
                @Override
                public Boolean call(LifecycleState state) {
                    return state == wanted;
                }
            })
            .take(1)
            .subscribe(then);
    }
}
