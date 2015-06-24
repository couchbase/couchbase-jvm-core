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
package com.couchbase.client.core;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.system.ConfigUpdatedEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.BootstrapMessage;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.query.QueryRequest;
import com.couchbase.client.core.message.view.ViewRequest;
import com.couchbase.client.core.node.CouchbaseNode;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.node.locate.ConfigLocator;
import com.couchbase.client.core.node.locate.DCPLocator;
import com.couchbase.client.core.node.locate.KeyValueLocator;
import com.couchbase.client.core.node.locate.Locator;
import com.couchbase.client.core.node.locate.QueryLocator;
import com.couchbase.client.core.node.locate.ViewLocator;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link RequestHandler} handles the overall concept of {@link Node}s and manages them concurrently.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class RequestHandler implements EventHandler<RequestEvent> {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(RequestHandler.class);

    /**
     * The node locator for the binary service.
     */
    private final Locator binaryLocator = new KeyValueLocator();

    /**
     * The node locator for the view service.
     */
    private final Locator viewLocator = new ViewLocator();

    /**
     * The node locator for the query service.
     */
    private final Locator queryLocator = new QueryLocator();

    /**
     * The node locator for the config service.
     */
    private final Locator configLocator = new ConfigLocator();

    /**
     * The node locator for DCP service.
     */
    private final Locator dcpLocator = new DCPLocator();

    /**
     * The list of currently managed nodes against the cluster.
     */
    private final Set<Node> nodes;

    /**
     * The shared couchbase environment.
     */
    private final CoreEnvironment environment;

    /**
     * Contains the current cluster configuration.
     */
    private final AtomicReference<ClusterConfig> configuration;

    /**
     * The {@link ResponseEvent} {@link RingBuffer}.
     */
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * The event bus to publish events onto.
     */
    private final EventBus eventBus;

    /**
     * Create a new {@link RequestHandler}.
     */
    public RequestHandler(CoreEnvironment environment, Observable<ClusterConfig> configObservable,
        RingBuffer<ResponseEvent> responseBuffer) {
        this(new CopyOnWriteArraySet<Node>(), environment, configObservable, responseBuffer);
    }

    /**
     * Create a new {@link RequestHandler} with a custom node list.
     *
     * This constructor should only be used for testing purposes.
     * @param nodes the node list to start with.
     */
    RequestHandler(Set<Node> nodes, CoreEnvironment environment, Observable<ClusterConfig> configObservable,
        RingBuffer<ResponseEvent> responseBuffer) {
        this.nodes = nodes;
        this.environment = environment;
        this.responseBuffer = responseBuffer;
        this.eventBus = environment.eventBus();
        configuration = new AtomicReference<ClusterConfig>();

        configObservable.subscribe(new Action1<ClusterConfig>() {
            @Override
            public void call(final ClusterConfig config) {
                try {
                    LOGGER.debug("Got notified of a new configuration arriving.");
                    configuration.set(config);
                    reconfigure(config).subscribe();
                    if (eventBus != null && eventBus.hasSubscribers()) {
                        eventBus.publish(new ConfigUpdatedEvent(config));
                    }
                } catch (Exception ex) {
                    LOGGER.error("Error while subscribing to bucket config stream.", ex);
                }
            }
        });
    }

    @Override
    public void onEvent(final RequestEvent event, long sequence, final boolean endOfBatch) throws Exception {
        try {
            final CouchbaseRequest request = event.getRequest();

            ClusterConfig config = configuration.get();
            //prevent non-bootstrap requests to go through if bucket not part of config
            if (!(request instanceof BootstrapMessage)) {
                if (config == null || (request.bucket() != null  && !config.hasBucket(request.bucket()))) {
                    request.observable().onError(new BucketClosedException(request.bucket() + " has been closed"));
                    return;
                }

                //short-circuit some kind of requests for which we know there won't be any handler to respond.
                try {
                    checkFeaturesForRequest(request, config.bucketConfig(request.bucket()));
                } catch (ServiceNotAvailableException e) {
                    request.observable().onError(e);
                    return;
                }
            }

            Node[] found = locator(request).locate(request, nodes, config);

            if (found == null) {
                return;
            }
            if (found.length == 0) {
                RetryHelper.retryOrCancel(environment, request, responseBuffer);
            }
            for (int i = 0; i < found.length; i++) {
                try {
                    found[i].send(request);
                } catch (Exception ex) {
                    request.observable().onError(ex);
                }
            }
        } finally {
            event.setRequest(null);
            if (endOfBatch && nodes != null) {
                for (Node node : nodes) {
                    node.send(SignalFlush.INSTANCE);
                }
            }
        }
    }

    /**
     * Checks, for a sub-set of {@link CouchbaseRequest}, if the current environment has
     * the necessary feature activated. If not, throws an {@link ServiceNotAvailableException}.
     *
     * @param request the request to check.
     * @throws ServiceNotAvailableException if the request type needs a particular feature which isn't activated.
     */
    protected void checkFeaturesForRequest(CouchbaseRequest request, BucketConfig config) {
        if (request instanceof BinaryRequest && !config.serviceEnabled(ServiceType.BINARY)) {
            throw new ServiceNotAvailableException("The KeyValue service is not enabled or no node in the cluster supports it.");
        } else if (request instanceof ViewRequest && !config.serviceEnabled(ServiceType.VIEW)) {
            throw new ServiceNotAvailableException("The View service is not enabled or no node in the cluster supports it.");
        } else if (request instanceof QueryRequest && !(environment.queryEnabled() || config.serviceEnabled(ServiceType.QUERY))) {
            throw new ServiceNotAvailableException("The Query service is not enabled or no node in the cluster supports it.");
        } else if (request instanceof  DCPRequest && !(environment.dcpEnabled() || config.serviceEnabled(ServiceType.DCP))) {
            throw new ServiceNotAvailableException("The DCP service is not enabled or no node in the cluster supports it.");
        }
    }

    /**
     * Add a {@link Node} identified by its hostname.
     *
     * @param hostname the hostname of the node.
     * @return the states of the node (most probably {@link LifecycleState#CONNECTED}).
     */
    public Observable<LifecycleState> addNode(final InetAddress hostname) {
        Node node = nodeBy(hostname);
        if (node != null) {
            LOGGER.debug("Node {} already registered, skipping.", hostname);
            return Observable.just(node.state());
        }
        return addNode(new CouchbaseNode(hostname, environment, responseBuffer));
    }

    /**
     * Adds a {@link Node} to the cluster.
     *
     * The code first initiates a asynchronous connect and then eventually adds it to the node list once it has been
     * connected successfully.
     */
    Observable<LifecycleState> addNode(final Node node) {
        LOGGER.debug("Got instructed to add Node {}", node.hostname());

        if (nodes.contains(node)) {
            LOGGER.debug("Node {} already registered, skipping.", node.hostname());
            return Observable.just(node.state());
        }

        LOGGER.debug("Connecting Node " + node.hostname());
        return node.connect().map(new Func1<LifecycleState, LifecycleState>() {
            @Override
            public LifecycleState call(LifecycleState lifecycleState) {
                LOGGER.debug("Connect finished, registering for use.");
                nodes.add(node);
                return lifecycleState;
            }
        });
    }

    /**
     * Remove a {@link Node} identified by its hostname.
     *
     * @param hostname the hostname of the node.
     * @return the states of the node (most probably {@link LifecycleState#DISCONNECTED}).
     */
    public Observable<LifecycleState> removeNode(final InetAddress hostname) {
        return removeNode(nodeBy(hostname));
    }

    /**
     * Removes a {@link Node} from the cluster.
     *
     * The node first gets removed from the list and then is disconnected afterwards, so that outstanding
     * operations can be handled gracefully.
     */
    Observable<LifecycleState> removeNode(final Node node) {
        LOGGER.debug("Got instructed to remove Node {}", node.hostname());
        nodes.remove(node);
        return node.disconnect();
    }

    /**
     * Add the service to the node.
     *
     * @param request the request which contains infos about the service and node to add.
     * @return an observable which contains the newly created service.
     */
    public Observable<Service> addService(final AddServiceRequest request) {
        LOGGER.debug("Got instructed to add Service {}, to Node {}", request.type(), request.hostname());
        return nodeBy(request.hostname()).addService(request);
    }

    /**
     * Remove a service from a node.
     *
     * @param request the request which contains infos about the service and node to remove.
     * @return an observable which contains the removed service.
     */
    public Observable<Service> removeService(final RemoveServiceRequest request) {
        LOGGER.debug("Got instructed to remove Service {}, from Node {}", request.type(), request.hostname());
        return nodeBy(request.hostname()).removeService(request);
    }

    /**
     * Returns the node by its hostname.
     *
     * @param hostname the hostname of the node.
     * @return the node or null if no hostname for that ip address.
     */
    public Node nodeBy(final InetAddress hostname) {
        if (hostname == null) {
            return null;
        }

        for (Node node : nodes) {
            if (node.hostname().equals(hostname)) {
                return node;
            }
        }
        return null;
    }

    /**
     * Helper method to detect the correct locator for the given request type.
     *
     * @return the locator for the given request type.
     */
    protected Locator locator(final CouchbaseRequest request) {
        if (request instanceof BinaryRequest) {
            return binaryLocator;
        } else if (request instanceof ViewRequest) {
            return viewLocator;
        } else if (request instanceof QueryRequest) {
            return queryLocator;
        } else if (request instanceof ConfigRequest) {
            return configLocator;
        } else if (request instanceof DCPRequest) {
            return dcpLocator;
        } else {
            throw new IllegalArgumentException("Unknown Request Type: " + request);
        }
    }

    /**
     * Helper method which grabs the current configuration and checks if the node setup is out of sync.
     *
     * This method is always called when a new configuration arrives and it will try to sync the actual node
     * and service setup with the one proposed by the configuration.
     */
    public Observable<ClusterConfig> reconfigure(final ClusterConfig config) {
        LOGGER.debug("Starting reconfiguration.");

        if (config.bucketConfigs().values().isEmpty()) {
            LOGGER.debug("No node found in config, disconnecting all nodes.");
            if (nodes.isEmpty()) {
                return Observable.just(config);
            }

            return Observable.from(new HashSet<Node>(nodes)).doOnNext(new Action1<Node>() {
                @Override
                public void call(Node node) {
                    removeNode(node);
                    node.disconnect().subscribe();
                }
            }).last().map(new Func1<Node, ClusterConfig>() {
                @Override
                public ClusterConfig call(Node node) {
                    return config;
                }
            });
        }

        return Observable
            .just(config)
            .flatMap(new Func1<ClusterConfig, Observable<BucketConfig>>() {
                @Override
                public Observable<BucketConfig> call(final ClusterConfig clusterConfig) {
                    return Observable.from(clusterConfig.bucketConfigs().values());
                }
            }).flatMap(new Func1<BucketConfig, Observable<Boolean>>() {
                @Override
                public Observable<Boolean> call(BucketConfig bucketConfig) {
                    return reconfigureBucket(bucketConfig);
                }
            })
            .last()
            .doOnNext(new Action1<Boolean>() {
                @Override
                public void call(Boolean aBoolean) {
                    Set<InetAddress> configNodes = new HashSet<InetAddress>();
                    for (Map.Entry<String, BucketConfig> bucket : config.bucketConfigs().entrySet()) {
                        for (final NodeInfo node : bucket.getValue().nodes()) {
                            configNodes.add(node.hostname());
                        }
                    }

                    for (Node node : nodes) {
                        if (!configNodes.contains(node.hostname())) {
                            LOGGER.debug("Removing and disconnecting node {}.", node.hostname());
                            removeNode(node);
                            node.disconnect().subscribe();
                        }
                    }
                }
            })
            .map(new Func1<Boolean, ClusterConfig>() {
                @Override
                public ClusterConfig call(Boolean aBoolean) {
                    return config;
                }
            });
    }

    /**
     * For every bucket that is open, apply the reconfiguration.
     *
     * @param config the config for this bucket.
     */
    private Observable<Boolean> reconfigureBucket(final BucketConfig config) {
        LOGGER.debug("Starting reconfiguration for bucket {}", config.name());

        List<Observable<Boolean>> observables = new ArrayList<Observable<Boolean>>();
        for (final NodeInfo nodeInfo : config.nodes()) {
            Observable<Boolean> obs = addNode(nodeInfo.hostname())
                .flatMap(new Func1<LifecycleState, Observable<Map<ServiceType, Integer>>>() {
                    @Override
                    public Observable<Map<ServiceType, Integer>> call(final LifecycleState lifecycleState) {
                        Map<ServiceType, Integer> services =
                                environment.sslEnabled() ? nodeInfo.sslServices() : nodeInfo.services();
                        if (!services.containsKey(ServiceType.QUERY) && environment.queryEnabled()) {
                            services.put(ServiceType.QUERY, environment.queryPort());
                        }
                        if (!services.containsKey(ServiceType.DCP) && environment.dcpEnabled()) {
                            services.put(ServiceType.DCP, services.get(ServiceType.BINARY));
                        }
                        return Observable.just(services);
                    }
                }).flatMap(new Func1<Map<ServiceType, Integer>, Observable<AddServiceRequest>>() {
                    @Override
                    public Observable<AddServiceRequest> call(final Map<ServiceType, Integer> services) {
                        List<AddServiceRequest> requests = new ArrayList<AddServiceRequest>(services.size());
                        for (Map.Entry<ServiceType, Integer> service : services.entrySet()) {
                            requests.add(new AddServiceRequest(service.getKey(), config.name(), config.password(),
                                    service.getValue(), nodeInfo.hostname()));
                        }
                        return Observable.from(requests);
                    }
                }).flatMap(new Func1<AddServiceRequest, Observable<Service>>() {
                    @Override
                    public Observable<Service> call(AddServiceRequest request) {
                        return addService(request);
                    }
                }).last().map(new Func1<Service, Boolean>() {
                        @Override
                        public Boolean call(Service service) {
                            return true;
                        }
                    });
            observables.add(obs);
        }

        return Observable.merge(observables).last();
    }

}
