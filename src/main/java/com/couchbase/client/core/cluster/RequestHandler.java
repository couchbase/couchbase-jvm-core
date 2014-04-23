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
package com.couchbase.client.core.cluster;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.node.CouchbaseNode;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.node.locate.BinaryLocator;
import com.couchbase.client.core.node.locate.Locator;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The {@link RequestHandler} handles the overall concept of {@link Node}s and manages them concurrently.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class RequestHandler implements EventHandler<RequestEvent> {

    /**
     * The initial number of nodes, will expand automatically if more are needed.
     */
    private static final int INITIAL_NODE_SIZE = 128;

    /**
     * The node locator for the binary service.
     */
    private static final Locator BINARY_LOCATOR = new BinaryLocator();

    /**
     * The read/write lock for the list of managed nodes.
     */
    private final ReadWriteLock nodeLock = new ReentrantReadWriteLock();

    /**
     * The list of currently managed nodes against the cluster.
     */
    private final Set<Node> nodes;

    /**
     * The shared couchbase environment.
     */
    private final Environment environment;

    /**
     * Contains the current cluster configuration.
     */
    private final AtomicReference<ClusterConfig> configuration;

    /**
     * The {@link ResponseEvent} {@link RingBuffer}.
     */
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * Create a new {@link RequestHandler}.
     */
    public RequestHandler(Environment environment, Observable<ClusterConfig> configObservable,
        RingBuffer<ResponseEvent> responseBuffer) {
        this(new HashSet<Node>(INITIAL_NODE_SIZE), environment, configObservable, responseBuffer);
    }

    /**
     * Create a new {@link RequestHandler} with a custom node list.
     *
     * This constructor should only be used for testing purposes.
     * @param nodes the node list to start with.
     */
    RequestHandler(Set<Node> nodes, Environment environment, Observable<ClusterConfig> configObservable,
        RingBuffer<ResponseEvent> responseBuffer) {
        this.nodes = nodes;
        this.environment = environment;
        this.responseBuffer = responseBuffer;
        configuration = new AtomicReference<ClusterConfig>();

        configObservable.subscribe(new Action1<ClusterConfig>() {
            @Override
            public void call(final ClusterConfig config) {
                configuration.set(config);
                reconfigure();
            }
        });
    }

    @Override
    public void onEvent(final RequestEvent event, long sequence, final boolean endOfBatch) throws Exception {
        final CouchbaseRequest request = event.getRequest();
        nodeLock.readLock().lock();

        locator(request).locate(request, nodes, configuration.get()).subscribe(new Observer<Node>() {
            @Override
            public void onCompleted() {
                cleanup();
            }

            @Override
            public void onError(final Throwable e) {
                cleanup();
            }

            @Override
            public void onNext(final Node node) {
                node.send(request);
                if (endOfBatch) {
                    node.send(SignalFlush.INSTANCE);
                }
            }

            private void cleanup() {
                nodeLock.readLock().unlock();
                event.setRequest(null);
            }
        });
    }

    /**
     * Add a {@link Node} identified by its hostname.
     *
     * @param hostname the hostname of the node.
     * @return the states of the node (most probably {@link LifecycleState#CONNECTED}).
     */
    public Observable<LifecycleState> addNode(final String hostname) {
        Node node = nodeBy(hostname);
        if (node != null) {
            return Observable.from(node.state());
        }
        return addNode(new CouchbaseNode(hostname, environment, responseBuffer));
    }

    /**
     * Remove a {@link Node} identified by its hostname.
     *
     * @param hostname the hostname of the node.
     * @return the states of the node (most probably {@link LifecycleState#DISCONNECTED}).
     */
    public Observable<LifecycleState> removeNode(final String hostname) {
        return removeNode(nodeBy(hostname));
    }

    /**
     * Add the service to the node.
     *
     * @param request the request which contains infos about the service and node to add.
     * @return an observable which contains the newly created service.
     */
    public Observable<Service> addService(final AddServiceRequest request) {
        return nodeBy(request.hostname()).addService(request);
    }

    /**
     * Remove a service from a node.
     *
     * @param request the request which contains infos about the service and node to remove.
     * @return an observable which contains the removed service.
     */
    public Observable<Service> removeService(final RemoveServiceRequest request) {
        return nodeBy(request.hostname()).removeService(request);
    }

    /**
     * Adds a {@link Node} to the cluster.
     *
     * The code first initiates a asynchronous connect and then eventually adds it to the node list once it has been
     * connected successfully.
     */
    Observable<LifecycleState> addNode(final Node node) {
        try {
            nodeLock.readLock().lock();
            if (nodes.contains(node)) {
                return Observable.from(node.state());
            }
        } finally {
            nodeLock.readLock().unlock();
        }

        return node.connect().map(new Func1<LifecycleState, LifecycleState>() {
            @Override
            public LifecycleState call(LifecycleState lifecycleState) {
                try {
                    nodeLock.writeLock().lock();
                    nodes.add(node);
                } finally {
                    nodeLock.writeLock().unlock();
                }
                return lifecycleState;
            }
        });
    }

    /**
     * Removes a {@link Node} from the cluster.
     *
     * The node first gets removed from the list and then is disconnected afterwards, so that outstanding
     * operations can be handled gracefully.
     */
    Observable<LifecycleState> removeNode(final Node node) {
        nodeLock.writeLock().lock();
        nodes.remove(node);
        nodeLock.writeLock().unlock();
        return node.disconnect();
    }

    /**
     * Returns the node by its hostname.
     *
     * @param hostname the hostname of the node.
     * @return the node or null if no hostname for that ip address.
     */
    public Node nodeBy(final String hostname) {
        if (hostname == null) {
            return null;
        }

        try {
            nodeLock.readLock().lock();
            for (Node node : nodes) {
                if (node.hostname().equals(hostname)) {
                    return node;
                }
            }
            return null;
        } finally {
            nodeLock.readLock().unlock();
        }
    }

    /**
     * Helper method to detect the correct locator for the given request type.
     *
     * @return the locator for the given request type.
     */
    protected Locator locator(final CouchbaseRequest request) {
        if (request instanceof BinaryRequest) {
            return BINARY_LOCATOR;
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
    private void reconfigure() {
        ClusterConfig config = configuration.get();
        for (Map.Entry<String, BucketConfig> bucket : config.bucketConfigs().entrySet()) {
            BucketConfig bucketConfig = bucket.getValue();
            reconfigureBucket(bucketConfig);
        }
    }

    private void reconfigureBucket(final BucketConfig config) {
        for (final NodeInfo nodeInfo : config.nodes()) {
            addNode(nodeInfo.hostname()).flatMap(new Func1<LifecycleState, Observable<ServiceType>>() {
                @Override
                public Observable<ServiceType> call(LifecycleState lifecycleState) {
                    return Observable.from(config.services());
                }
            }).flatMap(new Func1<ServiceType, Observable<Service>>() {
                @Override
                public Observable<Service> call(ServiceType serviceType) {
                    AddServiceRequest request = new AddServiceRequest(serviceType, config.name(), nodeInfo.hostname());
                    return addService(request);
                }
            }).subscribe();
        }
    }

}
