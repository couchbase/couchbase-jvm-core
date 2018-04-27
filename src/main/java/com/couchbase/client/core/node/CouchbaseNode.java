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
import com.couchbase.client.core.utils.NetworkAddress;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
     * The threshold above which reverse DNS lookup is logged as being too slow (in milliseconds).
     */
    private static final long DNS_RESOLUTION_THRESHOLD = TimeUnit.SECONDS.toMillis(1);

    /**
     * The hostname or IP address of the node.
     */
    private final NetworkAddress hostname;

    /**
     * For performance reasons, cache the string representation as well.
     */
    private final String convertedHostname;

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

    public CouchbaseNode(final NetworkAddress hostname, final CoreContext ctx) {
        this(hostname, new DefaultServiceRegistry(), ctx, ServiceFactory.INSTANCE);
    }

    CouchbaseNode(final NetworkAddress hostname, ServiceRegistry registry, final CoreContext ctx,
        ServiceFactory serviceFactory) {
        super(LifecycleState.DISCONNECTED);
        this.hostname = hostname;
        this.convertedHostname = hostname.nameOrAddress();
        this.serviceRegistry = registry;
        this.environment = ctx.environment();
        this.responseBuffer = ctx.responseRingBuffer();
        this.ctx = ctx;
        this.eventBus = environment.eventBus();
        this.serviceFactory = serviceFactory;
        this.serviceStates = new ServiceStateZipper(LifecycleState.DISCONNECTED);

        if (NetworkAddress.ALLOW_REVERSE_DNS) {
            //JVMCBC-229: eagerly trigger and time a reverse DNS lookup
            long lookupStart = System.nanoTime();
            String lookupResult = hostname.nameAndAddress();
            long lookupDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lookupStart);
            if (lookupDurationMs >= DNS_RESOLUTION_THRESHOLD) {
                LOGGER.warn(
                    "DNS Reverse Lookup of {} is slow, took {}ms",
                    system(lookupResult),
                    lookupDurationMs
                );
            }
        }

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
                    LOGGER.debug("Connected (" + state() + ") to Node " + hostname);
                } else if (newState == LifecycleState.DISCONNECTED) {
                    if (connected) {
                        signalDisconnected();
                    }
                    connected = false;
                    LOGGER.debug("Disconnected (" + state() + ") from Node " + hostname);
                } else if (newState == LifecycleState.CONNECTING) {
                    if (connected) {
                        // We've already been connected, so this is a reconnect phase for the node following a
                        // complete disconnect (like a node restart).
                        signalDisconnected();
                        connected = false;
                        LOGGER.debug("Reconnecting (" + state() + ") from Node " + hostname);
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
        LOGGER.info("Connected to Node {}", system(hostname.nameAndAddress()));
        if (eventBus != null && eventBus.hasSubscribers()) {
            eventBus.publish(new NodeConnectedEvent(hostname));
        }
    }

    /**
     * Log that this node is now disconnected and also inform all susbcribers on the event bus.
     */
    private void signalDisconnected() {
        LOGGER.info("Disconnected from Node {}", system(hostname.nameAndAddress()));
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
            request.dispatchHostname(convertedHostname);
            Service service = serviceRegistry.locate(request);
            if (service == null) {
                RetryHelper.retryOrCancel(environment, request, responseBuffer);

            } else {
                service.send(request);
            }
        }
    }

    @Override
    public NetworkAddress hostname() {
        return hostname;
    }

    @Override
    public Observable<LifecycleState> connect() {
        LOGGER.debug(logIdent(hostname) + "Got instructed to connect.");

        return Observable
            .from(serviceRegistry.services())
            .flatMap(new Func1<Service, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(final Service service) {
                    LOGGER.debug(logIdent(hostname) + "Instructing Service " + service.type() + " to connect.");
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
        LOGGER.debug(logIdent(hostname) + "Got instructed to disconnect.");

        return Observable
            .from(serviceRegistry.services())
            .flatMap(new Func1<Service, Observable<LifecycleState>>() {
                @Override
                public Observable<LifecycleState> call(final Service service) {
                    LOGGER.debug(logIdent(hostname) + "Instructing Service " + service.type() + " to disconnect.");
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
        LOGGER.debug(logIdent(hostname) + "Adding Service " + request.type());
        Service addedService = serviceRegistry.serviceBy(request.type(), request.bucket());
        if (addedService != null) {
            LOGGER.debug(logIdent(hostname) + "Service " + request.type() + " already added, skipping.");
            return Observable.just(addedService);
        }

        final Service service = serviceFactory.create(
            request.hostname().nameOrAddress(),
            request.bucket(),
            request.username(),
            request.password(),
            request.port(),
            ctx,
            request.type()
        );

        serviceStates.register(service, service);
        LOGGER.debug(logIdent(hostname) + "Adding Service " + request.type() + " to registry and connecting it.");
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
        LOGGER.debug(logIdent(hostname) + "Removing Service " + request.type());

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
     * @param hostname the address.
     * @return a prefix string for logs.
     */
    protected static String logIdent(final NetworkAddress hostname) {
        return "[" + hostname.nameAndAddress() + "]: ";
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
