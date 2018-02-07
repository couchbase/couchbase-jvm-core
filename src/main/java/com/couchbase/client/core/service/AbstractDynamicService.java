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
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.EndpointHealth;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parent implementation of a dynamic {@link Service}.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public abstract class AbstractDynamicService extends AbstractStateMachine<LifecycleState> implements Service {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Service.class);

    private final Endpoint[] endpoints;

    private final String hostname;
    private final String bucket;
    private final String username;
    private final String password;
    private final int port;
    private final CoreContext ctx;
    private final int minEndpoints;
    private final EndpointFactory endpointFactory;
    private final EndpointStateZipper endpointStates;
    private final LifecycleState initialState;

    protected AbstractDynamicService(final String hostname, final String bucket, final String username, final String password, final int port,
                                     final CoreContext ctx, final int minEndpoints, final EndpointFactory endpointFactory) {
        super(minEndpoints == 0 ? LifecycleState.IDLE : LifecycleState.DISCONNECTED);
        this.initialState = minEndpoints == 0 ? LifecycleState.IDLE : LifecycleState.DISCONNECTED;
        this.hostname = hostname;
        this.bucket = bucket;
        this.username = username;
        this.password = password;
        this.port = port;
        this.ctx = ctx;
        this.minEndpoints = minEndpoints;
        this.endpointFactory = endpointFactory;
        endpointStates = new EndpointStateZipper(initialState);
        endpoints = new Endpoint[minEndpoints];
        endpointStates.states().subscribe(new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState lifecycleState) {
                transitionState(lifecycleState);
            }
        });
    }

    protected abstract void dispatch(CouchbaseRequest request);

    @Override
    public Observable<LifecycleState> connect() {
        LOGGER.debug(logIdent(hostname, this) + "Got instructed to connect.");
        if (state() == LifecycleState.CONNECTED || state() == LifecycleState.CONNECTING) {
            LOGGER.debug(logIdent(hostname, this) + "Already connected or connecting, skipping connect.");
            return Observable.just(state());
        }

        for (int i = 0; i < minEndpoints; i++) {
            Endpoint endpoint = createEndpoint();
            endpoints[i] = endpoint;
            endpointStates.register(endpoint, endpoint);
        }

        return Observable
            .from(endpoints)
            .flatMap(new Func1<Endpoint, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(final Endpoint endpoint) {
                    LOGGER.debug(logIdent(hostname, AbstractDynamicService.this)
                            + "Initializing connect on Endpoint.");
                    return endpoint.connect();
                }
            })
            .lastOrDefault(initialState)
            .map(new Func1<LifecycleState, LifecycleState>() {
                @Override
                public LifecycleState call(final LifecycleState state) {
                    return state();
                }
            });
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (request instanceof SignalFlush) {
            int length = endpoints.length;
            for (int i = 0; i < length; i++) {
                Endpoint endpoint = endpoints[i];
                // Make sure to only send a flush event
                // to the endpoint if it is actually set
                // on the array
                if (endpoint != null) {
                    endpoint.send(request);
                }
            }
            return;
        }

        dispatch(request);
    }

    @Override
    public Observable<LifecycleState> disconnect() {
        LOGGER.debug(logIdent(hostname, this) + "Got instructed to disconnect.");
        if (state() == LifecycleState.DISCONNECTED || state() == LifecycleState.DISCONNECTING) {
            LOGGER.debug(logIdent(hostname, this) + "Already disconnected or disconnecting, skipping disconnect.");
            return Observable.just(state());
        }

        return Observable
                .from(endpoints)
                .flatMap(new Func1<Endpoint, Observable<LifecycleState>>() {
                    @Override
                    public Observable<LifecycleState> call(Endpoint endpoint) {
                        LOGGER.debug(logIdent(hostname, AbstractDynamicService.this)
                                + "Initializing disconnect on Endpoint.");
                        return endpoint.disconnect();
                    }
                })
                .lastOrDefault(initialState)
                .map(new Func1<LifecycleState, LifecycleState>() {
                    @Override
                    public LifecycleState call(final LifecycleState state) {
                        endpointStates.terminate();
                        return state();
                    }
                });
    }

    @Override
    public BucketServiceMapping mapping() {
        return type().mapping();
    }

    @Override
    public Observable<EndpointHealth> diagnostics() {
        List<Observable<EndpointHealth>> diags = new ArrayList<Observable<EndpointHealth>>();
        for (Endpoint endpoint : endpoints()) {
            diags.add(endpoint.diagnostics(type()).toObservable());
        }
        return Observable.merge(diags);
    }

    /**
     * Helper method to create a new endpoint.
     *
     * @return the endpoint to create.
     */
    protected Endpoint createEndpoint() {
        return endpointFactory.create(hostname, bucket, username, password, port, ctx);
    }

    /**
     * Simple log helper to give logs a common prefix.
     *
     * @param hostname the address.
     * @param service the service.
     * @return a prefix string for logs.
     */
    protected static String logIdent(final String hostname, final Service service) {
        return "[" + hostname + "][" + service.getClass().getSimpleName() + "]: ";
    }

    /**
     * Returns the current list of endpoints.
     *
     * @return the list of endpoints.
     */
    protected List<Endpoint> endpoints() {
        return Arrays.asList(endpoints);
    }

    /**
     * Returns the underlying endpoint state zipper.
     *
     * @return the underlying state zipper.
     */
    protected EndpointStateZipper endpointStates() {
        return endpointStates;
    }

    /**
     * Waits until the endpoint goes into the given state, calls the action and then unsubscribes.
     *
     * @param endpoint the endpoint to monitor.
     * @param wanted the wanted state.
     * @param then the action to execute when the state is reached.
     */
    protected static void whenState(final Endpoint endpoint, final LifecycleState wanted, Action1<LifecycleState> then) {
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
