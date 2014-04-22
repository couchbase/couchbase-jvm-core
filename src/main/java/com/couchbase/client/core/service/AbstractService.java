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

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

public abstract class AbstractService extends AbstractStateMachine<LifecycleState> implements Service {

    /**
     * The logger to use for all endpoints.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Service.class);

    private final String hostname;
    private final Environment environment;
    private final SelectionStrategy strategy;
    private final Endpoint[] endpoints;

    protected AbstractService(String hostname, Environment env, int numEndpoints, SelectionStrategy strategy,
        final RingBuffer<ResponseEvent> responseBuffer) {
        super(LifecycleState.DISCONNECTED);

        this.hostname = hostname;
        this.environment = env;
        this.strategy = strategy;

        endpoints = new Endpoint[numEndpoints];
        for (int i = 0; i < numEndpoints; i++) {
            endpoints[i] = newEndpoint(responseBuffer);
        }
    }

    /**
     * Create a new {@link Endpoint} to be used by this service.
     *
     * @return the endpoint to be used.
     */
    protected abstract Endpoint newEndpoint(final RingBuffer<ResponseEvent> responseBuffer);

    @Override
    public BucketServiceMapping mapping() {
        return type().mapping();
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (request instanceof SignalFlush) {
            for (int i = 0; i < endpoints.length; i++) {
                endpoints[i].send(request);
            }
            return;
        }
        strategy.select(request, endpoints).send(request);
    }

    @Override
    public Observable<LifecycleState> connect() {
        if (state() == LifecycleState.CONNECTED || state() == LifecycleState.CONNECTING) {
            return Observable.from(state());
        }
        transitionState(LifecycleState.CONNECTING);
        return Observable.from(endpoints).flatMap(new Func1<Endpoint, Observable<LifecycleState>>() {
            @Override
            public Observable<LifecycleState> call(final Endpoint endpoint) {
                return endpoint.connect();
            }
        }).toList().map(new Func1<List<LifecycleState>, LifecycleState>() {
            @Override
            public LifecycleState call(List<LifecycleState> endpointStates) {
                LifecycleState serviceState = calculateStateFrom(endpointStates);
                transitionState(serviceState);
                return serviceState;
            }
        });
    }

    @Override
    public Observable<LifecycleState> disconnect() {
        if (state() == LifecycleState.DISCONNECTED || state() == LifecycleState.DISCONNECTING) {
            return Observable.from(state());
        }

        transitionState(LifecycleState.DISCONNECTING);
        return Observable.from(endpoints).flatMap(new Func1<Endpoint, Observable<LifecycleState>>() {
            @Override
            public Observable<LifecycleState> call(Endpoint endpoint) {
                return endpoint.disconnect();
            }
        }).toList().map(new Func1<List<LifecycleState>, LifecycleState>() {
            @Override
            public LifecycleState call(List<LifecycleState> endpointStates) {
                for (LifecycleState endpointState : endpointStates) {
                    if (endpointState != LifecycleState.DISCONNECTED) {
                        LOGGER.warn("Underlying Endpoint did not disconnect cleanly on shutdown.");
                    }
                }
                transitionState(LifecycleState.DISCONNECTED);
                return state();
            }
        });
    }

    protected Environment environment() {
        return environment;
    }

    protected String hostname() {
        return hostname;
    }

    /**
     * Calculates the states for a {@link Service} based on the given {@link Endpoint} states.
     *
     * The rules are as follows in strict order:
     *   2) All Endpoints Connected -> Connected
     *   3) At least one Endpoint Connected -> Degraded
     *   4) At least one Endpoint Connecting -> Connecting
     *   5) At least one Endpoint Disconnecting -> Disconnecting
     *   6) Otherwise -> Disconnected
     *
     * @param endpointStates the input endpoint states.
     * @return the output service states.
     */
    private static LifecycleState calculateStateFrom(final List<LifecycleState> endpointStates) {
        if (endpointStates.isEmpty()) {
            return LifecycleState.DISCONNECTED;
        }
        int connected = 0;
        int connecting = 0;
        int disconnecting = 0;
        for (LifecycleState endpointState : endpointStates) {
            switch (endpointState) {
                case CONNECTED:
                    connected++;
                    break;
                case CONNECTING:
                    connecting++;
                    break;
                case DISCONNECTING:
                    disconnecting++;
                    break;
            }
        }
        if (endpointStates.size() == connected) {
            return LifecycleState.CONNECTED;
        } else if (connected > 0) {
            return LifecycleState.DEGRADED;
        } else if (connecting > 0) {
            return LifecycleState.CONNECTING;
        } else if (disconnecting > 0) {
            return LifecycleState.DISCONNECTING;
        } else {
            return LifecycleState.DISCONNECTED;
        }
    }
}
