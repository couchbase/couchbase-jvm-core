package com.couchbase.client.core.service;


import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import rx.Observable;

import java.util.List;

public abstract class AbstractService extends AbstractStateMachine<LifecycleState> implements Service {

    private final String hostname;
    private final Environment environment;
    private final SelectionStrategy strategy;
    private final Endpoint[] endpoints;

    protected AbstractService(String hostname, Environment env, int numEndpoints, SelectionStrategy strategy) {
        super(LifecycleState.DISCONNECTED);

        this.hostname = hostname;
        this.environment = env;
        this.strategy = strategy;

        endpoints = new Endpoint[numEndpoints];
        for (int i = 0; i < numEndpoints; i++) {
            endpoints[i] = newEndpoint();
        }
    }

    /**
     * Create a new {@link Endpoint} to be used by this service.
     *
     * @return the endpoint to be used.
     */
    protected abstract Endpoint newEndpoint();

    @Override
    public BucketServiceMapping mapping() {
        return type().mapping();
    }

    @Override
    public void send(CouchbaseRequest request) {
        request.observable().onNext(null);
        request.observable().onCompleted();
    }

    @Override
    public Observable<LifecycleState> connect() {
        return Observable.from(LifecycleState.CONNECTED);
    }

    @Override
    public Observable<LifecycleState> disconnect() {
        return Observable.from(LifecycleState.DISCONNECTED);
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
