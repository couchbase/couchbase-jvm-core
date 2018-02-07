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

/**
 * Abstract implementation of a service which enables and disables endpoints on demand.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public abstract class AbstractOnDemandService extends AbstractDynamicService {

    protected AbstractOnDemandService(String hostname, String bucket, String username, String password, int port, CoreContext ctx, EndpointFactory endpointFactory) {
        super(hostname, bucket, username, password, port, ctx, 0, endpointFactory);
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


}
