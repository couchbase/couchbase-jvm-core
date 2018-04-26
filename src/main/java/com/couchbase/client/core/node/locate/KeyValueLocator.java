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
package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.ReplicaNotAvailableException;
import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.DefaultCouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.GetAllMutationTokensRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.NoopRequest;
import com.couchbase.client.core.message.kv.ObserveRequest;
import com.couchbase.client.core.message.kv.ObserveSeqnoRequest;
import com.couchbase.client.core.message.kv.ReplicaGetRequest;
import com.couchbase.client.core.message.kv.StatRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.utils.NetworkAddress;
import com.lmax.disruptor.RingBuffer;

import java.util.List;
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

    private static final int MIN_KEY_BYTES = 1;
    private static final int MAX_KEY_BYTES = 250;

    @Override
    public void locateAndDispatch(final CouchbaseRequest request, final List<Node> nodes, final ClusterConfig cluster,
        CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {
        if (request instanceof GetBucketConfigRequest) {
            locateByHostname(request, ((GetBucketConfigRequest) request).hostname(), nodes, env, responseBuffer);
            return;
        }
        if (request instanceof StatRequest) {
            locateByHostname(request, ((StatRequest) request).hostname(), nodes, env, responseBuffer);
            return;
        }
        if (request instanceof GetAllMutationTokensRequest) {
            locateByHostname(request, ((GetAllMutationTokensRequest) request).hostname(), nodes, env, responseBuffer);
            return;
        }
        if (request instanceof NoopRequest) {
            locateByHostname(request, ((NoopRequest) request).hostname(), nodes, env, responseBuffer);
            return;
        }

        BucketConfig bucket = cluster.bucketConfig(request.bucket());
        if (bucket instanceof CouchbaseBucketConfig) {
            locateForCouchbaseBucket((BinaryRequest) request, nodes, (CouchbaseBucketConfig) bucket, env, responseBuffer);
        } else if (bucket instanceof MemcachedBucketConfig) {
            locateForMemcacheBucket((BinaryRequest) request, nodes, (MemcachedBucketConfig) bucket, env, responseBuffer);
        } else {
            throw new IllegalStateException("Unsupported Bucket Type: " + bucket + " for request " + request);
        }
    }


    private static void locateByHostname(final CouchbaseRequest request, final NetworkAddress hostname, List<Node> nodes,
                                         CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {
        for (Node node : nodes) {
            if (node.isState(LifecycleState.CONNECTED) || node.isState(LifecycleState.DEGRADED)) {
                if (!hostname.equals(node.hostname())) {
                    continue;
                }
                node.send(request);
                return;
            }
        }
        RetryHelper.retryOrCancel(env, request, responseBuffer);
    }

    /**
     * Locates the proper {@link Node}s for a Couchbase bucket.
     *
     * @param request the request.
     * @param nodes the managed nodes.
     * @param config the bucket configuration.
     */
    private static void locateForCouchbaseBucket(final BinaryRequest request, final List<Node> nodes,
        final CouchbaseBucketConfig config, CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {

        if (!keyIsValid(request)) {
            return;
        }

        int partitionId = partitionForKey(request.keyBytes(), config.numberOfPartitions());
        request.partition((short) partitionId);

        int nodeId = calculateNodeId(partitionId, request, config);
        if (nodeId < 0) {
            errorObservables(nodeId, request, config.name(), env, responseBuffer);
            return;
        }

        NodeInfo nodeInfo = config.nodeAtIndex(nodeId);

        for (Node node : nodes) {
            if (node.hostname().equals(nodeInfo.hostname())) {
                node.send(request);
                return;
            }
        }

        if(handleNotEqualNodeSizes(config.nodes().size(), nodes.size())) {
            RetryHelper.retryOrCancel(env, request, responseBuffer);
            return;
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
        boolean useFastForward = request.retryCount() > 0 && config.hasFastForwardMap();

        if (request instanceof ReplicaGetRequest) {
            return config.nodeIndexForReplica(partitionId, ((ReplicaGetRequest) request).replica() - 1, useFastForward);
        } else if (request instanceof ObserveRequest && ((ObserveRequest) request).replica() > 0) {
            return config.nodeIndexForReplica(partitionId, ((ObserveRequest) request).replica() - 1, useFastForward);
        } else if (request instanceof ObserveSeqnoRequest && ((ObserveSeqnoRequest) request).replica() > 0) {
            return config.nodeIndexForReplica(partitionId, ((ObserveSeqnoRequest) request).replica() - 1, useFastForward);
        } else {
            return config.nodeIndexForMaster(partitionId, useFastForward);
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
     */
    private static void errorObservables(int nodeId, BinaryRequest request, String name, CoreEnvironment env,
        RingBuffer<ResponseEvent> responseBuffer) {
        if (nodeId == DefaultCouchbaseBucketConfig.PARTITION_NOT_EXISTENT) {
            if (request instanceof ReplicaGetRequest) {
                request.observable().onError(new ReplicaNotConfiguredException("Replica number "
                        + ((ReplicaGetRequest) request).replica() + " not configured for bucket " + name, null));
                return;
            } else if (request instanceof ObserveRequest) {
                request.observable().onError(new ReplicaNotConfiguredException("Replica number "
                        + ((ObserveRequest) request).replica() + " not configured for bucket " + name, ((ObserveRequest) request).cas()));
                return;
            } else if (request instanceof ObserveSeqnoRequest) {
                request.observable().onError(new ReplicaNotConfiguredException("Replica number "
                        + ((ObserveSeqnoRequest) request).replica() + " not configured for bucket " + name, ((ObserveSeqnoRequest) request).cas()));
                return;
            }

            RetryHelper.retryOrCancel(env, request, responseBuffer);
            return;
        }

        if (nodeId == -1) {
            if (request instanceof ObserveRequest) {
                request.observable().onError(new ReplicaNotAvailableException("Replica number "
                        + ((ObserveRequest) request).replica() + " not available for bucket " + name,
                    ((ObserveRequest) request).cas()));
                return;
            } else if (request instanceof ReplicaGetRequest) {
                request.observable().onError(new ReplicaNotAvailableException("Replica number "
                        + ((ReplicaGetRequest) request).replica() + " not available for bucket " + name, null));
                return;
            } else if (request instanceof ObserveSeqnoRequest) {
                request.observable().onError(new ReplicaNotAvailableException("Replica number "
                        + ((ObserveSeqnoRequest) request).replica() + " not available for bucket " + name, ((ObserveSeqnoRequest) request).cas()));
                return;
            }

            RetryHelper.retryOrCancel(env, request, responseBuffer);
            return;
        }

        throw new IllegalStateException("Unknown NodeId: " + nodeId + ", request: " + request);
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
     */
    private static void locateForMemcacheBucket(final BinaryRequest request, final List<Node> nodes,
        final MemcachedBucketConfig config, CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {

        if (!keyIsValid(request)) {
            return;
        }

        NetworkAddress hostname = config.nodeForId(request.keyBytes());
        request.partition((short) 0);

        for (Node node : nodes) {
            if (node.hostname().equals(hostname)) {
                node.send(request);
                return;
            }
        }

        if(handleNotEqualNodeSizes(config.nodes().size(), nodes.size())) {
            RetryHelper.retryOrCancel(env, request, responseBuffer);
            return;
        }

        throw new IllegalStateException("Node not found for request" + request);
    }

    /**
     * Helper method to handle potentially different node sizes in the actual list and in the config.
     *
     * @return true if they are not equal, false if they are.
     */
    private static boolean handleNotEqualNodeSizes(int configNodeSize, int actualNodeSize) {
        if (configNodeSize != actualNodeSize) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Node list and configuration's partition hosts sizes : {} <> {}, rescheduling",
                        actualNodeSize, configNodeSize);
            }
            return true;
        }
        return false;
    }

    /**
     * Helper method to check if the given request key is valid.
     *
     * If false is returned, the request observable is already failed.
     *
     * @param request the request to extract and validate the key from.
     * @return true if valid, false otherwise.
     */
    private static boolean keyIsValid(final BinaryRequest request) {
        if (request.keyBytes() == null || request.keyBytes().length < MIN_KEY_BYTES) {
            request.observable().onError(new IllegalArgumentException("The Document ID must not be null or empty."));
            return false;
        }

        if (request.keyBytes().length > MAX_KEY_BYTES) {
            request.observable().onError(new IllegalArgumentException(
                "The Document ID must not be longer than 250 bytes."));
            return false;
        }

        return true;
    }

}
