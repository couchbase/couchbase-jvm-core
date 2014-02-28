package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.selector.Selector;
import reactor.function.Consumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static reactor.event.selector.Selectors.$;


public abstract class AbstractService extends AbstractStateMachine<LifecycleState> implements Service  {

    /**
     * The logger to use for all endpoints.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Service.class);

    /**
     * The strategy on how to pick the right endpint.
     */
    private final SelectionStrategy strategy;

    /**
     * How much endpoints should be opened and kept open.
     */
    private final int endpointCount;

    /**
     * Contains all endpoints.
     */
    private final Registry<Endpoint> endpointRegistry;

    /**
     * Creates a new endpoint.
     *
     * @return the created endpoint.
     */
    protected abstract Endpoint newEndpoint();

    /**
     *
     */
    private final Environment environment;

    /**
     *
     * @param env
     * @param endpointCount
     * @param strategy
     */
    protected AbstractService(Environment env, int endpointCount, SelectionStrategy strategy) {
        super(LifecycleState.DISCONNECTED, env);
        this.endpointCount = endpointCount;
        this.strategy = strategy;
        environment = env;
        endpointRegistry = new CachingRegistry<Endpoint>();

        for (int i = 0; i < endpointCount; i++) {
            Selector selector = $(new Integer(i));
            endpointRegistry.register(selector, newEndpoint());
        }
    }

    @Override
    public Promise<LifecycleState> connect() {
        if (state() == LifecycleState.CONNECTED || state() == LifecycleState.CONNECTING) {
            return Promises.success(state()).get();
        }
        transitionState(LifecycleState.CONNECTING);
        final Deferred<LifecycleState,Promise<LifecycleState>> deferred = Promises.defer(environment.reactorEnv());

        Iterator<Registration<? extends Endpoint>> iterator = endpointRegistry.iterator();
        List<Promise<LifecycleState>> endpointPromises = new ArrayList<Promise<LifecycleState>>();
        while(iterator.hasNext()) {
            Registration<? extends Endpoint> registration = iterator.next();
            if (!registration.isCancelled()) {
                endpointPromises.add(registration.getObject().connect());
            }
        }

        Promises.when(endpointPromises).consume(new Consumer<List<LifecycleState>>() {
            @Override
            public void accept(List<LifecycleState> states) {
                int connected = 0;
                int connecting = 0;
                for (LifecycleState state : states) {
                    if (state == LifecycleState.CONNECTED) {
                        connected++;
                    } else if (state == LifecycleState.CONNECTING) {
                        connecting++;
                    }
                }

                LifecycleState endState;
                if (connected == states.size()) {
                    endState = LifecycleState.CONNECTED;
                } else if (connected > 0) {
                    endState = LifecycleState.DEGRADED;
                } else if (connecting > 0) {
                    endState = LifecycleState.CONNECTING;
                } else {
                    endState = LifecycleState.DISCONNECTED;
                }

                deferred.accept(endState);
            }
        });

        return deferred.compose();
    }

    @Override
    public Promise<LifecycleState> disconnect() {
        final Deferred<LifecycleState,Promise<LifecycleState>> deferred = Promises.defer(environment.reactorEnv());

        Iterator<Registration<? extends Endpoint>> iterator = endpointRegistry.iterator();
        List<Promise<LifecycleState>> endpointPromises = new ArrayList<Promise<LifecycleState>>();
        while(iterator.hasNext()) {
            Registration<? extends Endpoint> registration = iterator.next();
            if (!registration.isCancelled()) {
                endpointPromises.add(registration.getObject().disconnect());
            }
        }

        Promises.when(endpointPromises).consume(new Consumer<List<LifecycleState>>() {
            @Override
            public void accept(List<LifecycleState> states) {
                for (LifecycleState state : states) {
                    if (state != LifecycleState.DISCONNECTED) {
                        LOGGER.warn("Underlying Endpoint did not disconnect cleanly on shutdown.");
                    }
                }
                deferred.accept(LifecycleState.DISCONNECTED);
            }
        });

        return deferred.compose();
    }

    @Override
    public <R extends CouchbaseResponse> Promise<R> send(final CouchbaseRequest request) {
        return select(request).send(request);
    }

    /**
     * Select the appropriate endpoint based on the strategy used.
     *
     * @return
     */
    private Endpoint select(CouchbaseRequest request) {
        return strategy.select(endpointRegistry, request);
    }
}
