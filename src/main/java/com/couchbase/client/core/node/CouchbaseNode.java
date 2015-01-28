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
package com.couchbase.client.core.node;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.system.NodeConnectedEvent;
import com.couchbase.client.core.event.system.NodeDisconnectedEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceFactory;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private final InetAddress hostname;

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

    private final Map<Service, LifecycleState> serviceStates;

    private volatile boolean connected;

    public CouchbaseNode(final InetAddress hostname, final CoreEnvironment environment,
        final RingBuffer<ResponseEvent> responseBuffer) {
        this(hostname, new DefaultServiceRegistry(), environment, responseBuffer);
    }

    CouchbaseNode(final InetAddress hostname, ServiceRegistry registry, final CoreEnvironment environment,
        final RingBuffer<ResponseEvent> responseBuffer) {
        super(LifecycleState.DISCONNECTED);
        this.hostname = hostname;
        this.serviceRegistry = registry;
        this.environment = environment;
        this.responseBuffer = responseBuffer;
        this.eventBus = environment.eventBus();
        this.serviceStates = new ConcurrentHashMap<Service, LifecycleState>();
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (request instanceof SignalFlush) {
            for (Service service : serviceRegistry.services()) {
                service.send(request);
            }
        } else {
            Service service = serviceRegistry.locate(request);
            if (service == null) {
                RetryHelper.retryOrCancel(environment, request, responseBuffer);

            } else {
                service.send(request);
            }
        }
    }

    @Override
    public InetAddress hostname() {
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

        final Service service = ServiceFactory.create(
            request.hostname().getHostName(),
            request.bucket(),
            request.password(),
            request.port(),
            environment,
            request.type(),
            responseBuffer
        );

        serviceStates.put(service, service.state());
        service.states().subscribe(new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState state) {
                serviceStates.put(service, state);
                LifecycleState oldState = state();
                LifecycleState newState = recalculateState();
                if (oldState == newState) {
                    return;
                }

                if (newState == LifecycleState.CONNECTED) {
                    if (!connected) {
                        LOGGER.info("Connected to Node " + hostname.getHostName());

                        if (eventBus !=  null) {
                            eventBus.publish(new NodeConnectedEvent(hostname));
                        }
                    }
                    connected = true;
                    LOGGER.debug("Connected (" + state() + ") to Node " + hostname);
                } else if (newState == LifecycleState.DISCONNECTED) {
                    if (connected) {
                        LOGGER.info("Disconnected from Node " + hostname.getHostName());
                        if (eventBus != null) {
                            eventBus.publish(new NodeDisconnectedEvent(hostname));
                        }
                    }
                    connected = false;
                    LOGGER.debug("Disconnected (" + state() + ") from Node " + hostname);
                }
                transitionState(newState);
            }
        });
        LOGGER.debug(logIdent(hostname) + "Adding Service " + request.type() + " to registry and connecting it.");
        serviceRegistry.addService(service, request.bucket());
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

        Service service = serviceRegistry.serviceBy(request.type(), request.bucket());
        serviceRegistry.removeService(service, request.bucket());
        serviceStates.remove(service);
        return Observable.just(service);
    }

    /**
     * Calculates the states for a {@link CouchbaseNode} based on the given {@link Service} states.
     *
     * The rules are as follows in strict order:
     *   1) No Service States -> Disconnected
     *   2) All Services Connected -> Connected
     *   3) At least one Service Connected -> Degraded
     *   4) At least one Service Connecting -> Connecting
     *   5) At least one Service Disconnecting -> Disconnecting
     *   6) Otherwise -> Disconnected
     *
     * @return the output node states.
     */
    private LifecycleState recalculateState() {
        if (serviceStates.isEmpty()) {
            return LifecycleState.DISCONNECTED;
        }
        int connected = 0;
        int connecting = 0;
        int disconnecting = 0;
        int idle = 0;
        for (LifecycleState serviceState : serviceStates.values()) {
            switch (serviceState) {
                case CONNECTED:
                    connected++;
                    break;
                case CONNECTING:
                    connecting++;
                    break;
                case DISCONNECTING:
                    disconnecting++;
                    break;
                case IDLE:
                    idle++;
                    break;
            }
        }
        if (serviceStates.size() == (connected + idle)) {
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
    protected static String logIdent(final InetAddress hostname) {
        return "[" + hostname.getHostName() + "]: ";
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
}
