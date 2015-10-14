/**
 * Copyright (C) 2014-2015 Couchbase, Inc.
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
package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.ReplicaNotAvailableException;
import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.ObserveRequest;
import com.couchbase.client.core.message.kv.ObserveSeqnoRequest;
import com.couchbase.client.core.message.kv.ReplicaGetRequest;
import com.couchbase.client.core.message.kv.StatRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.state.LifecycleState;

import java.net.InetAddress;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * This {@link Locator} finds the proper {@link Node}s for every incoming {@link BinaryRequest}.
 *
 * Depending on the bucket type used, it either uses partition/vbucket (couchbase) or ketama (memcache) hashing. For
 * broadcast-type operations, it will return all suitable nodes without hashing by key.
 *
 * @since 1.0.0
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
public class KeyValueLocator implements Locator {

    /**
     * The Logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(KeyValueLocator.class);

    /**
     * An empty node array which can be reused and does not need to be re-created all the time.
     */
    private static final Node[] EMPTY_NODES = new Node[] { };

    @Override
    public Node[] locate(final CouchbaseRequest request, final Set<Node> nodes, final ClusterConfig cluster) {
        if (request instanceof GetBucketConfigRequest) {
            return handleBucketConfigRequest((GetBucketConfigRequest) request, nodes);
        }
        if (request instanceof StatRequest) {
            return handleStatRequest((StatRequest)request, nodes);
        }

        BucketConfig bucket = cluster.bucketConfig(request.bucket());
        if (bucket instanceof CouchbaseBucketConfig) {
            return locateForCouchbaseBucket((BinaryRequest) request, nodes, (CouchbaseBucketConfig) bucket);
        } else if (bucket instanceof MemcachedBucketConfig) {
            return locateForMemcacheBucket((BinaryRequest) request, nodes, (MemcachedBucketConfig) bucket);
        } else {
            throw new IllegalStateException("Unsupported Bucket Type: " + bucket + " for request " + request);
        }
    }

    /**
     * Special node handling for the get bucket config request.
     *
     * This is necessary to properly bootstrap the driver. if the hostnames are not equal, it is not the node where
     * the service has been enabled, so the code looks for the next one until it finds one. If none is found, the
     * operation is rescheduled.
     *
     * @param request the request to check
     * @param nodes the nodes to iterate
     * @return either the found node or an empty list indicating to retry later.
     */
    private static Node[] handleBucketConfigRequest(GetBucketConfigRequest request, Set<Node> nodes) {
        return locateByHostname(request.hostname(), nodes);
    }

    private static Node[] handleStatRequest(StatRequest request, Set<Node> nodes) {
        return locateByHostname(request.hostname(), nodes);
    }

    private static Node[] locateByHostname(final InetAddress hostname, Set<Node> nodes) {
        for (Node node : nodes) {
            if (node.isState(LifecycleState.CONNECTED)) {
                if (!hostname.equals(node.hostname())) {
                    continue;
                }
                return new Node[] { node };
            }
        }
        return EMPTY_NODES;
    }

    /**
     * Locates the proper {@link Node}s for a Couchbase bucket.
     *
     * @param request the request.
     * @param nodes the managed nodes.
     * @param config the bucket configuration.
     * @return an observable with one or more nodes to send the request to.
     */
    private static Node[] locateForCouchbaseBucket(final BinaryRequest request, final Set<Node> nodes,
        final CouchbaseBucketConfig config) {

        int partitionId = partitionForKey(request.keyBytes(), config.numberOfPartitions());
        request.partition((short) partitionId);

        int nodeId = calculateNodeId(partitionId, request, config);
        if (nodeId < 0) {
            return errorObservables(nodeId, request, config.name());
        }

        NodeInfo nodeInfo = config.nodeAtIndex(nodeId);

        for (Node node : nodes) {
            if (node.hostname().equals(nodeInfo.hostname())) {
                return new Node[] { node };
            }
        }

        if (config.nodes().size() != nodes.size()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Node list and configuration's partition hosts sizes : {} <> {}, rescheduling",
                    nodes.size(), config.nodes().size());
            }
            return EMPTY_NODES;
        }

        throw new IllegalStateException("Node not found for request" + request);
    }

    /**
     * Helper method to calculate the node if for the given partition and request type.
     *
     * @param partitionId the partition id.
     * @param request the request used.
     * @param config the current bucket configuration.
     * @return the calculated node id.
     */
    private static int calculateNodeId(int partitionId, BinaryRequest request, CouchbaseBucketConfig config) {
        if (request instanceof ReplicaGetRequest) {
            return config.nodeIndexForReplica(partitionId, ((ReplicaGetRequest) request).replica() - 1);
        } else if (request instanceof ObserveRequest && ((ObserveRequest) request).replica() > 0) {
            return config.nodeIndexForReplica(partitionId, ((ObserveRequest) request).replica() - 1);
        } else if (request instanceof ObserveSeqnoRequest && ((ObserveSeqnoRequest) request).replica() > 0) {
            return config.nodeIndexForReplica(partitionId, ((ObserveSeqnoRequest) request).replica() - 1);
        } else {
            return config.nodeIndexForMaster(partitionId);
        }
    }

    /**
     * Fail observables because the partitions do not match up.
     *
     * If the replica is not even available in the configuration (identified by a -2 node index),
     * it is clear that this replica is not configured. If a -1 is returned it is configured, but
     * currently not available (not enough nodes in the cluster, for example if a node is seen down,
     * after a failover, or during rebalance. Replica partitions in general take longer to heal than
     * active partitions, since they are sacrificed for application availability.
     *
     * @param nodeId the current node id of the partition
     * @param request the request to error
     * @param name the name of the bucket
     * @return A node array indicating to retry or move on.
     */
    private static Node[] errorObservables(int nodeId, BinaryRequest request, String name) {
        if (nodeId == -2) {
            if (request instanceof ReplicaGetRequest) {
                request.observable().onError(new ReplicaNotConfiguredException("Replica number "
                        + ((ReplicaGetRequest) request).replica() + " not configured for bucket " + name));
            } else if (request instanceof ObserveRequest) {
                request.observable().onError(new ReplicaNotConfiguredException("Replica number "
                        + ((ObserveRequest) request).replica() + " not configured for bucket " + name));
            } else if (request instanceof ObserveSeqnoRequest) {
                request.observable().onError(new ReplicaNotConfiguredException("Replica number "
                        + ((ObserveSeqnoRequest) request).replica() + " not configured for bucket " + name));
            }

            return null;
        }

        if (nodeId == -1) {
            if (request instanceof ObserveRequest) {
                request.observable().onError(new ReplicaNotAvailableException("Replica number "
                        + ((ObserveRequest) request).replica() + " not available for bucket " + name));
                return null;
            } else if (request instanceof ReplicaGetRequest) {
                request.observable().onError(new ReplicaNotAvailableException("Replica number "
                        + ((ReplicaGetRequest) request).replica() + " not available for bucket " + name));
                return null;
            } else if (request instanceof ObserveSeqnoRequest) {
                request.observable().onError(new ReplicaNotAvailableException("Replica number "
                        + ((ObserveSeqnoRequest) request).replica() + " not available for bucket " + name));
                return null;
            }

            return EMPTY_NODES;
        }

        return EMPTY_NODES;
    }

    /**
     * Calculate the vbucket for the given key.
     *
     * @param key the key to calculate from.
     * @param numPartitions the number of partitions in the bucket.
     * @return the calculated partition.
     */
    private static int partitionForKey(byte[] key, int numPartitions) {
        CRC32 crc32 = new CRC32();
        crc32.update(key);
        long rv = (crc32.getValue() >> 16) & 0x7fff;
        return (int) rv &numPartitions - 1;
    }

    /**
     * Locates the proper {@link Node}s for a Memcache bucket.
     *
     * @param request the request.
     * @param nodes the managed nodes.
     * @param config the bucket configuration.
     * @return an observable with one or more nodes to send the request to.
     */
    private static Node[] locateForMemcacheBucket(final BinaryRequest request, final Set<Node> nodes,
        final MemcachedBucketConfig config) {
        InetAddress hostname = config.nodeForId(request.keyBytes());
        request.partition((short) 0);

        for (Node node : nodes) {
            if (node.hostname().equals(hostname)) {
                return new Node[] { node };
            }
        }

        if (config.nodes().size() != nodes.size()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Node list and configuration's partition hosts sizes : {} <> {}, rescheduling",
                    nodes.size(), config.nodes().size());
            }
            return EMPTY_NODES;
        }

        throw new IllegalStateException("Node not found for request" + request);
    }

}
