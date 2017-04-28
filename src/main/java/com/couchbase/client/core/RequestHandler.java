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
import com.couchbase.client.core.message.analytics.AnalyticsRequest;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.query.QueryRequest;
import com.couchbase.client.core.message.search.SearchRequest;
import com.couchbase.client.core.message.view.ViewRequest;
import com.couchbase.client.core.node.CouchbaseNode;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.node.locate.AnalyticsLocator;
import com.couchbase.client.core.node.locate.ConfigLocator;
import com.couchbase.client.core.node.locate.DCPLocator;
import com.couchbase.client.core.node.locate.KeyValueLocator;
import com.couchbase.client.core.node.locate.Locator;
import com.couchbase.client.core.node.locate.QueryLocator;
import com.couchbase.client.core.node.locate.SearchLocator;
import com.couchbase.client.core.node.locate.ViewLocator;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.utils.NetworkAddress;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.couchbase.client.core.utils.Observables.failSafe;

/**
 * The {@link RequestHandler} is responsible for adding and removing {@link Node}s as well as dispatching
 * {@link Service} management operations. Its main purpose though is to receive incoming {@link CouchbaseRequest}s
 * and dispatch them to the appropriate {@link Node}s.
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
     * The node locator for the query service.
     */
    private final Locator searchLocator = new SearchLocator();

    /**
     * The node locator for the analytics service.
     */
    private final Locator analyticsLocator = new AnalyticsLocator();

    /**
     * The list of currently managed nodes against the cluster.
     */
    private final CopyOnWriteArrayList<Node> nodes;

    /**
     * The shared couchbase environment.
     */
    private final CoreEnvironment environment;

    /**
     * The {@link ResponseEvent} {@link RingBuffer}.
     */
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * The event bus to publish events onto.
     */
    private final EventBus eventBus;

    /**
     * Contains the current cluster configuration.
     */
    private volatile ClusterConfig configuration;

    /**
     * Create a new {@link RequestHandler}.
     */
    public RequestHandler(final CoreEnvironment environment, final Observable<ClusterConfig> configObservable,
        final RingBuffer<ResponseEvent> responseBuffer) {
        this(new CopyOnWriteArrayList<Node>(), environment, configObservable, responseBuffer);
    }

    /**
     * Create a new {@link RequestHandler} with a custom node list.
     *
     * This constructor should only be used for testing purposes.
     * @param nodes the node list to start with.
     */
    RequestHandler(final CopyOnWriteArrayList<Node> nodes, final CoreEnvironment environment,
        final Observable<ClusterConfig> configObservable, final RingBuffer<ResponseEvent> responseBuffer) {
        this.nodes = nodes;
        this.environment = environment;
        this.responseBuffer = responseBuffer;
        this.eventBus = environment.eventBus();
        configuration = null;

        configObservable.subscribe(new Action1<ClusterConfig>() {
            @Override
            public void call(final ClusterConfig config) {
                try {
                    LOGGER.debug("Got notified of a new configuration arriving.");
                    configuration = config;
                    reconfigure(config).subscribe(new Subscriber<ClusterConfig>() {
                        @Override
                        public void onCompleted() {}

                        @Override
                        public void onError(Throwable e) {
                            LOGGER.warn("Received Error during Reconfiguration.", e);
                        }

                        @Override
                        public void onNext(final ClusterConfig clusterConfig) {
                            int logicalNodes = nodes.size();
                            int configNodes = clusterConfig.allNodeAddresses().size();

                            if (logicalNodes != configNodes) {
                                LOGGER.debug("Number of logical Nodes does not match the number of nodes in the "
                                    + "current configuration! logical: {}, config: {}", logicalNodes, configNodes);
                            }
                        }
                    });

                    if (eventBus != null) {
                        eventBus.publish(new ConfigUpdatedEvent(config));
                    }
                } catch (Exception ex) {
                    LOGGER.error("Error while subscribing to bucket config stream.", ex);
                }
            }
        });
    }

    @Override
    public void onEvent(final RequestEvent event, final long sequence, final boolean endOfBatch) throws Exception {
        try {
            dispatchRequest(event.getRequest());
        } finally {
            event.setRequest(null);
            if (endOfBatch && nodes != null) {
                flush();
            }
        }
    }

    /**
     * Helper method to send the flush signal to all of the nodes available.
     */
    private void flush() {
        for (Node node : nodes) {
            node.send(SignalFlush.INSTANCE);
        }
    }

    /**
     * Helper method to dispatch the incoming {@link CouchbaseRequest} to one or more nodes.
     *
     * @param request the request to dispatch.
     */
    private void dispatchRequest(final CouchbaseRequest request) {
        ClusterConfig config = configuration;

        //prevent non-bootstrap requests to go through if bucket not part of config
        if (!(request instanceof BootstrapMessage)) {
            BucketConfig bucketConfig = config == null ? null : config.bucketConfig(request.bucket());
            if (config == null || (request.bucket() != null  && bucketConfig == null)) {
                failSafe(environment.scheduler(), true, request.observable(),
                        new BucketClosedException(request.bucket() + " has been closed"));
                return;
            }

            //short-circuit some kind of requests for which we know there won't be any handler to respond.
            try {
                checkFeaturesForRequest(request, bucketConfig);
            } catch (ServiceNotAvailableException e) {
                failSafe(environment.scheduler(), true, request.observable(), e);
                return;
            }

            //don't send timed out requests to server
            if(!request.isActive()) {
                return;
            }
        }

        locator(request).locateAndDispatch(request, nodes, config, environment, responseBuffer);
    }

    /**
     * Checks, for a sub-set of {@link CouchbaseRequest}, if the current environment has
     * the necessary feature activated. If not, throws an {@link ServiceNotAvailableException}.
     *
     * @param request the request to check.
     * @throws ServiceNotAvailableException if the request type needs a particular feature which isn't activated.
     */
    protected void checkFeaturesForRequest(final CouchbaseRequest request, final BucketConfig config) {
        if (request instanceof BinaryRequest && !config.serviceEnabled(ServiceType.BINARY)) {
            throw new ServiceNotAvailableException("The KeyValue service is not enabled or no node in the cluster "
                + "supports it.");
        } else if (request instanceof ViewRequest && !config.serviceEnabled(ServiceType.VIEW)) {
            throw new ServiceNotAvailableException("The View service is not enabled or no node in the cluster "
                + "supports it.");
        } else if (request instanceof QueryRequest && !config.serviceEnabled(ServiceType.QUERY)) {
            throw new ServiceNotAvailableException("The Query service is not enabled or no node in the "
                + "cluster supports it.");
        } else if (request instanceof SearchRequest && !config.serviceEnabled(ServiceType.SEARCH)) {
            throw new ServiceNotAvailableException("The Search service is not enabled or no node in the "
                + "cluster supports it.");
        } else if (request instanceof DCPRequest && !(environment.dcpEnabled()
            || config.serviceEnabled(ServiceType.DCP))) {
            throw new ServiceNotAvailableException("The DCP service is not enabled or no node in the cluster "
                + "supports it.");
        } else if (request instanceof AnalyticsRequest && !config.serviceEnabled(ServiceType.ANALYTICS)) {
            throw new ServiceNotAvailableException("The Analytics service is not enabled or no node in the "
                + "cluster supports it.");
        }
    }

    /**
     * Add a {@link Node} identified by its hostname.
     *
     * @param hostname the hostname of the node.
     * @return the states of the node (most probably {@link LifecycleState#CONNECTED}).
     */
    public Observable<LifecycleState> addNode(final NetworkAddress hostname) {
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
                nodes.addIfAbsent(node);
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
    public Observable<LifecycleState> removeNode(final NetworkAddress hostname) {
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
    public Node nodeBy(final NetworkAddress hostname) {
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
        } else if (request instanceof SearchRequest) {
            return searchLocator;
        } else if (request instanceof AnalyticsRequest) {
            return analyticsLocator;
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
            LOGGER.debug("No open bucket found in config, disconnecting all nodes.");
            //JVMCBC-231: a race condition can happen where the nodes set is seen as
            // not empty, while the subsequent Observable.from is not, failing in calling last()
            List<Node> snapshotNodes;
            synchronized (nodes) {
                snapshotNodes = new ArrayList<Node>(nodes);
            }
            if (snapshotNodes.isEmpty()) {
                return Observable.just(config);
            }

            return Observable.from(snapshotNodes).doOnNext(new Action1<Node>() {
                @Override
                public void call(Node node) {
                    removeNode(node);
                    node.disconnect().subscribe(new Subscriber<LifecycleState>() {
                        @Override
                        public void onCompleted() {}

                        @Override
                        public void onError(Throwable e) {
                            LOGGER.warn("Got error during node disconnect.", e);
                        }

                        @Override
                        public void onNext(LifecycleState lifecycleState) {}
                    });
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
                    Set<NetworkAddress> configNodes = config.allNodeAddresses();

                    for (Node node : nodes) {
                        if (!configNodes.contains(node.hostname())) {
                            LOGGER.debug("Removing and disconnecting node {}.", node.hostname());
                            removeNode(node);
                            node.disconnect().subscribe(new Subscriber<LifecycleState>() {
                                @Override
                                public void onCompleted() {}

                                @Override
                                public void onError(Throwable e) {
                                    LOGGER.warn("Got error during node disconnect.", e);
                                }

                                @Override
                                public void onNext(LifecycleState lifecycleState) {}
                            });
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
                        if (services.containsKey(ServiceType.BINARY) && environment.dcpEnabled()) {
                            services.put(ServiceType.DCP, services.get(ServiceType.BINARY));
                        }
                        return Observable.just(services);
                    }
                }).flatMap(new Func1<Map<ServiceType, Integer>, Observable<AddServiceRequest>>() {
                    @Override
                    public Observable<AddServiceRequest> call(final Map<ServiceType, Integer> services) {
                        List<AddServiceRequest> requests = new ArrayList<AddServiceRequest>(services.size());
                        for (Map.Entry<ServiceType, Integer> service : services.entrySet()) {
                            requests.add(new AddServiceRequest(service.getKey(), config.name(), config.username(),
                                    config.password(), service.getValue(), nodeInfo.hostname()));
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
