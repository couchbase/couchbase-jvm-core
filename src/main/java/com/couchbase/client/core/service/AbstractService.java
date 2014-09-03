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
import com.couchbase.client.core.ResponseHandler;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.FuncN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The base implementation for all {@link Service}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public abstract class AbstractService extends AbstractStateMachine<LifecycleState> implements Service {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Service.class);

    /**
     * The endpoint selection strategy in use.
     */
    private final SelectionStrategy strategy;

    /**
     * All stored endpoints.
     */
    private final Endpoint[] endpoints;

    /**
     * The shared response buffer.
     */
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * The current list of endpoint states.
     */
    protected List<Observable<LifecycleState>> endpointStates;

    /**
     * Creates a new {@link AbstractService} and instantiates needed components and flows.
     *
     * @param hostname the hostname of the service.
     * @param bucket the name of the bucket.
     * @param password the password of the bucket.
     * @param port the port of the service.
     * @param env the shared environment.
     * @param numEndpoints the number of endpoints to init.
     * @param strategy the endpoint selection strategy used.
     * @param responseBuffer the shared response buffer.
     * @param factory the endpoint factory.
     */
    protected AbstractService(final String hostname, final String bucket, final String password, final int port,
        final CoreEnvironment env, final int numEndpoints, final SelectionStrategy strategy,
        final RingBuffer<ResponseEvent> responseBuffer, final EndpointFactory factory) {
        super(LifecycleState.DISCONNECTED);

        this.strategy = strategy;
        this.responseBuffer = responseBuffer;
        endpointStates = new ArrayList<Observable<LifecycleState>>();
        endpoints = new Endpoint[numEndpoints];
        for (int i = 0; i < numEndpoints; i++) {
            Endpoint endpoint = factory.create(hostname, bucket, password, port, env, responseBuffer);
            endpoints[i] = endpoint;
            endpointStates.add(endpoint.states());
        }

        Observable.combineLatest(endpointStates, new FuncN<LifecycleState>() {
            @Override
            public LifecycleState call(Object... args) {
                LifecycleState[] states = Arrays.copyOf(args, args.length, LifecycleState[].class);
                return calculateStateFrom(Arrays.asList(states));
            }
        }).subscribe(new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState state) {
                if (state == state()) {
                    return;
                }
                if (state == LifecycleState.CONNECTED) {
                    LOGGER.debug(logIdent(hostname, AbstractService.this) + "Connected Service.");
                } else if (state == LifecycleState.DISCONNECTED) {
                    LOGGER.debug(logIdent(hostname, AbstractService.this) + "Disconnected Service.");
                }
                transitionState(state);
            }
        });
    }

    @Override
    public BucketServiceMapping mapping() {
        return type().mapping();
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (request instanceof SignalFlush) {
            int length = endpoints.length;
            for (int i = 0; i < length; i++) {
                endpoints[i].send(request);
            }
            return;
        }

        Endpoint endpoint = strategy.select(request, endpoints);
        if (endpoint == null) {
            responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, request, request.observable());
        } else {
            endpoint.send(request);
        }
    }

    @Override
    public Observable<LifecycleState> connect() {
        if (state() == LifecycleState.CONNECTED || state() == LifecycleState.CONNECTING) {
            return Observable.just(state());
        }

        return Observable
            .from(endpoints)
            .flatMap(new Func1<Endpoint, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(final Endpoint endpoint) {
                    return endpoint.connect();
                }
            })
            .lastOrDefault(LifecycleState.DISCONNECTED)
            .map(new Func1<LifecycleState, LifecycleState>() {
                @Override
                public LifecycleState call(final LifecycleState state) {
                    return state();
                }
            });
    }

    @Override
    public Observable<LifecycleState> disconnect() {
        if (state() == LifecycleState.DISCONNECTED || state() == LifecycleState.DISCONNECTING) {
            return Observable.just(state());
        }

        return Observable
            .from(endpoints)
            .flatMap(new Func1<Endpoint, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(Endpoint endpoint) {
                    return endpoint.disconnect();
                }
            })
            .lastOrDefault(LifecycleState.DISCONNECTED)
            .map(new Func1<LifecycleState, LifecycleState>() {
                @Override
                public LifecycleState call(final LifecycleState state) {
                    return state();
                }
            });
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

    /**
     * Simple log helper to give logs a common prefix.
     *
     * @param hostname the address.
     * @param service the service.
     * @return a prefix string for logs.
     */
    protected static String logIdent(final String hostname, final Service service) {
        return "["+hostname+"][" + service.getClass().getSimpleName()+"]: ";
    }
}
