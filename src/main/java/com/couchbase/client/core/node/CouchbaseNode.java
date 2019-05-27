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
package com.couchbase.client.core.node;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.system.NodeConnectedEvent;
import com.couchbase.client.core.event.system.NodeDisconnectedEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.EndpointHealth;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceFactory;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.core.logging.RedactableArgument.system;

/**
 * The general implementation of a {@link Node}.
 *
 * A {@link Node} manages one or more {@link Service}s. When a node gets connected, all currently configured
 * {@link Service}s are connected. Those can and will also be added and removed on demand. On disconnect, all
 * services will be shut down asynchronously and then the node is determined to be shutdown.
 *
 * A {@link Node}s states is composed exclusively of the underlying {@link Service} states.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public class CouchbaseNode extends AbstractStateMachine<LifecycleState> implements Node {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Node.class);

    /**
     * The hostname or IP address of the node.
     */
    private final String hostname;

    /**
     * The alternate hostname or IP address of the node.
     */
    private final String alternate;

    /**
     * The environment to use.
     */
    private final CoreEnvironment environment;

    /**
     * The event bus to publish events onto.
     */
    private final EventBus eventBus;

    /**
     * The {@link ResponseEvent} {@link RingBuffer}.
     */
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * A registry containing all of the services associated with one or more buckets.
     */
    private final ServiceRegistry serviceRegistry;

    private final ServiceFactory serviceFactory;

    private final ServiceStateZipper serviceStates;

    /**
     * The core context used.
     */
    private final CoreContext ctx;

    private volatile boolean connected;

    /**
     * Contains the enabled {@link Service}s on a node level.
     */
    private volatile int enabledServices;

    public CouchbaseNode(final String hostname, final CoreContext ctx) {
        this(hostname, new DefaultServiceRegistry(), ctx, ServiceFactory.INSTANCE, null);
    }

    public CouchbaseNode(final String hostname, final CoreContext ctx, String alternate) {
        this(hostname, new DefaultServiceRegistry(), ctx, ServiceFactory.INSTANCE, alternate);
    }

    CouchbaseNode(final String hostname, ServiceRegistry registry, final CoreContext ctx,
                  ServiceFactory serviceFactory) {
        this(hostname, registry, ctx, serviceFactory, null);
    }

    CouchbaseNode(final String hostname, ServiceRegistry registry, final CoreContext ctx,
        ServiceFactory serviceFactory, final String alternate) {
        super(LifecycleState.DISCONNECTED);
        this.hostname = hostname;
        this.alternate = alternate;
        this.serviceRegistry = registry;
        this.environment = ctx.environment();
        this.responseBuffer = ctx.responseRingBuffer();
        this.ctx = ctx;
        this.eventBus = environment.eventBus();
        this.serviceFactory = serviceFactory;
        this.serviceStates = new ServiceStateZipper(LifecycleState.DISCONNECTED);

        serviceStates.states().subscribe(new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState newState) {
                LifecycleState oldState = state();
                if (oldState == newState) {
                    return;
                }

                if (newState == LifecycleState.CONNECTED) {
                    if (!connected) {
                        signalConnected();
                    }
                    connected = true;
                    LOGGER.debug(logIdent() + "Connected (" + state() + ") to Node");
                } else if (newState == LifecycleState.DISCONNECTED) {
                    if (connected) {
                        signalDisconnected();
                    }
                    connected = false;
                    LOGGER.debug(logIdent() + "Disconnected (" + state() + ") from Node");
                } else if (newState == LifecycleState.CONNECTING) {
                    if (connected) {
                        // We've already been connected, so this is a reconnect phase for the node following a
                        // complete disconnect (like a node restart).
                        signalDisconnected();
                        connected = false;
                        LOGGER.debug(logIdent() + "Reconnecting (" + state() + ") from Node");
                    }
                }
                transitionState(newState);
            }
        });
    }

    /**
     * Log that this node is now connected and also inform all susbcribers on the event bus.
     */
    private void signalConnected() {
        if (alternate != null) {
            LOGGER.info("Connected to Node {} ({})",
                system(hostname), system(alternate));
        } else {
            LOGGER.info("Connected to Node {}", system(hostname));
        }
        if (eventBus != null && eventBus.hasSubscribers()) {
            eventBus.publish(new NodeConnectedEvent(hostname));
        }
    }

    /**
     * Log that this node is now disconnected and also inform all susbcribers on the event bus.
     */
    private void signalDisconnected() {
        if (alternate != null) {
            LOGGER.info("Disconnected from Node {} ({})",
                system(hostname), system(alternate));
        } else {
            LOGGER.info("Disconnected from Node {}", system(hostname));
        }
        if (eventBus != null && eventBus.hasSubscribers()) {
            eventBus.publish(new NodeDisconnectedEvent(hostname));
        }
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (request instanceof SignalFlush) {
            for (Service service : serviceRegistry.services()) {
                service.send(request);
            }
        } else {
            request.dispatchHostname(hostname);
            Service service = serviceRegistry.locate(request);
            if (service == null) {
                RetryHelper.retryOrCancel(environment, request, responseBuffer);

            } else {
                service.send(request);
            }
        }
    }

    @Override
    public String hostname() {
        return hostname;
    }

    @Override
    public Observable<LifecycleState> connect() {
        LOGGER.debug(logIdent() + "Got instructed to connect.");

        return Observable
            .from(serviceRegistry.services())
            .flatMap(new Func1<Service, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(final Service service) {
                    LOGGER.debug(logIdent() + "Instructing Service " + service.type() + " to connect.");
                    return service.connect();
                }
            })
            .toList()
            .map(new Func1<List<LifecycleState>, LifecycleState>() {
                @Override
                public LifecycleState call(List<LifecycleState> state) {
                    return state();
                }
            });
    }

    @Override
    public Observable<LifecycleState> disconnect() {
        LOGGER.debug(logIdent() + "Got instructed to disconnect.");

        return Observable
            .from(serviceRegistry.services())
            .flatMap(new Func1<Service, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(final Service service) {
                    LOGGER.debug(logIdent() + "Instructing Service " + service.type() + " to disconnect.");
                    return service.disconnect();
                }
            })
            .toList()
            .map(new Func1<List<LifecycleState>, LifecycleState>() {
                @Override
                public LifecycleState call(List<LifecycleState> state) {
                    return state();
                }
            });
    }

    @Override
    public Observable<Service> addService(final AddServiceRequest request) {
        LOGGER.debug(logIdent() + "Adding Service " + request.type());
        Service addedService = serviceRegistry.serviceBy(request.type(), request.bucket());
        if (addedService != null) {
            LOGGER.debug(logIdent() + "Service " + request.type() + " already added, skipping.");
            return Observable.just(addedService);
        }


        String hostname = alternate != null ? alternate : this.hostname;
        if (alternate != null) {
            LOGGER.debug(logIdent()
                + "Service {} is mapped to alternate hostname {}", request.type(), hostname);
        }
        final Service service = serviceFactory.create(
            hostname,
            request.bucket(),
            request.username(),
            request.password(),
            request.port(),
            ctx,
            request.type()
        );

        serviceStates.register(service, service);
        LOGGER.debug(logIdent() + "Adding Service " + request.type() + " to registry and connecting it.");
        serviceRegistry.addService(service, request.bucket());
        enabledServices |= 1 << service.type().ordinal();
        return service.connect().map(new Func1<LifecycleState, Service>() {
            @Override
            public Service call(LifecycleState state) {
                return service;
            }
        });
    }

    @Override
    public Observable<Service> removeService(final RemoveServiceRequest request) {
        LOGGER.debug(logIdent() + "Removing Service " + request.type());

        final Service service = serviceRegistry.serviceBy(request.type(), request.bucket());
        serviceRegistry.removeService(service, request.bucket());
        serviceStates.deregister(service);
        enabledServices &= ~(1 << service.type().ordinal());
        return service.disconnect().map(new Func1<LifecycleState, Service>() {
            @Override
            public Service call(LifecycleState lifecycleState) {
                return service;
            }
        });
    }

    @Override
    public Observable<EndpointHealth> diagnostics() {
        List<Observable<EndpointHealth>> diags = new ArrayList<Observable<EndpointHealth>>();
        for (Service service : serviceRegistry.services()) {
            diags.add(service.diagnostics());
        }
        return Observable.merge(diags);
    }

    @Override
    public String toString() {
        return "CouchbaseNode{"
            + "hostname=" + hostname
            + ", services=" + serviceRegistry
            + '}';
    }

    /**
     * Simple log helper to give logs a common prefix.
     *
     * @return a prefix string for logs.
     */
    private String logIdent() {
        if (alternate != null) {
            return "[" + hostname + " (" + alternate + ")]: ";
        } else {
            return "[" + hostname + "]: ";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CouchbaseNode that = (CouchbaseNode) o;

        if (!hostname.equals(that.hostname)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return hostname.hashCode();
    }

    @Override
    public boolean serviceEnabled(ServiceType type) {
        return (enabledServices & (1 << type.ordinal())) != 0;
    }
}
