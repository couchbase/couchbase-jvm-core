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
package com.couchbase.client.core.service;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.config.ConfigEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.service.strategies.RandomSelectionStrategy;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import rx.Subscriber;

import java.util.ArrayList;
import java.util.List;

public class ConfigService extends AbstractService {

    private static final SelectionStrategy strategy = new RandomSelectionStrategy();
    private static final EndpointFactory factory = new ConfigEndpointFactory();
    private static final int INITIAL_ENDPOINTS = 1;

    private final String hostname;
    private final String bucket;
    private final String password;
    private final int port;
    private final CoreEnvironment env;
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * Contains a list of pinned {@link Endpoint}s.
     */
    private final List<Endpoint> pinnedEndpoints;

    public ConfigService(String hostname, String bucket, String password, int port, CoreEnvironment env,
        final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, INITIAL_ENDPOINTS, strategy, responseBuffer, factory);
        pinnedEndpoints = new ArrayList<Endpoint>();
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
        this.port = port;
        this.env = env;
        this.responseBuffer = responseBuffer;
    }

    @Override
    public ServiceType type() {
        return ServiceType.CONFIG;
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (request instanceof BucketStreamingRequest) {
            final Endpoint endpoint = factory.create(hostname, bucket, password, port, env, responseBuffer);
            endpointStates.add(endpoint.states());
            endpoint
                .connect()
                .subscribe(new Subscriber<LifecycleState>() {
                    @Override
                    public void onCompleted() {
                        pinnedEndpoints.add(endpoint);
                        endpoint.send(request);
                        endpoint.send(SignalFlush.INSTANCE);
                    }

                    @Override
                    public void onError(Throwable e) {
                        request.observable().onError(e);
                    }

                    @Override
                    public void onNext(LifecycleState state) {

                    }
                });
        } else {
            super.send(request);
        }
    }

    static class ConfigEndpointFactory implements EndpointFactory {
        @Override
        public Endpoint create(String hostname, String bucket, String password, int port, CoreEnvironment env,
                               RingBuffer<ResponseEvent> responseBuffer) {
            return new ConfigEndpoint(hostname, bucket, password, port, env, responseBuffer);
        }
    }

}
