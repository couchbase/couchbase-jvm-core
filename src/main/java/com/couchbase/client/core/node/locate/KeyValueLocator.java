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
package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.config.Partition;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.ObserveRequest;
import com.couchbase.client.core.message.kv.ReplicaGetRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.state.LifecycleState;
import io.netty.util.CharsetUtil;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.SortedMap;
import java.util.zip.CRC32;

/**
 * This {@link Locator} finds the proper {@link Node}s for every incoming {@link CouchbaseRequest}.
 *
 * Depending on the bucket type used, it either uses partition/vbucket (couchbase) or ketama (memcache) hashing. For
 * broadcast-type operations, it will return all suitable nodes without hashing by key.
 */
public class KeyValueLocator implements Locator {

    @Override
    public Node[] locate(final CouchbaseRequest request, final Set<Node> nodes, final ClusterConfig cluster) {
        if (request instanceof GetBucketConfigRequest) {
            for (Node node : nodes) {
                if (node.isState(LifecycleState.CONNECTED)) {
                    return new Node[] { node };
                }
            }

            return new Node[] {};
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
     * Locates the proper {@link Node}s for a Couchbase bucket.
     *
     * @param request the request.
     * @param nodes the managed nodes.
     * @param config the bucket configuration.
     * @return an observable with one or more nodes to send the request to.
     */
    private Node[] locateForCouchbaseBucket(final BinaryRequest request, final Set<Node> nodes,
        final CouchbaseBucketConfig config) {
        String key = request.key();

        CRC32 crc32 = new CRC32();
        try {
            crc32.update(key.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        long rv = (crc32.getValue() >> 16) & 0x7fff;
        int partitionId = (int) rv & config.partitions().size() - 1;
        request.partition((short) partitionId);


        Partition partition = config.partitions().get(partitionId);

        int nodeId;
        if (request instanceof ReplicaGetRequest) {
            nodeId = partition.replica(((ReplicaGetRequest) request).replica() - 1);
        } else if (request instanceof ObserveRequest && ((ObserveRequest) request).replica() > 0){
            nodeId = partition.replica(((ObserveRequest) request).replica() - 1);
        } else {
            nodeId = partition.master();
        }

        if (nodeId == -2) {
            if (request instanceof ReplicaGetRequest) {
                request.observable().onError(new ReplicaNotConfiguredException("Replica number "
                    + ((ReplicaGetRequest) request).replica() + " not configured for bucket " + config.name()));
            } else if (request instanceof ObserveRequest) {
                request.observable().onError(new ReplicaNotConfiguredException("Replica number "
                    + ((ObserveRequest) request).replica() + " not configured for bucket " + config.name()));
            }

            return null;
        }
        if (nodeId == -1) {
            return new Node[] { };
        }

        NodeInfo nodeInfo = config.partitionHosts().get(nodeId);
        for (Node node : nodes) {
            if (node.hostname().equals(nodeInfo.hostname())) {
                return new Node[] { node };
            }
        }

        throw new IllegalStateException("Node not found for request" + request);
    }

    /**
     * Locates the proper {@link Node}s for a Memcache bucket.
     *
     * @param request the request.
     * @param nodes the managed nodes.
     * @param config the bucket configuration.
     * @return an observable with one or more nodes to send the request to.
     */
    private Node[] locateForMemcacheBucket(final BinaryRequest request, final Set<Node> nodes,
        final MemcachedBucketConfig config) {

        long hash = ketamaHash(request.key());
        if (!config.ketamaNodes().containsKey(hash)) {
            SortedMap<Long, NodeInfo> tailMap = config.ketamaNodes().tailMap(hash);
            if (tailMap.isEmpty()) {
                hash = config.ketamaNodes().firstKey();
            } else {
                hash = tailMap.firstKey();
            }
        }

        NodeInfo found = config.ketamaNodes().get(hash);
        request.partition((short) 0);
        for (Node node : nodes) {
            if (node.hostname().equals(found.hostname())) {
                return new Node[] { node };
            }
        }

        throw new IllegalStateException("Node not found for request" + request);
    }

    private long ketamaHash(final String key) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(key.getBytes(CharsetUtil.UTF_8));
            byte[] digest = md5.digest();
            long rv = ((long) (digest[3] & 0xFF) << 24)
                | ((long) (digest[2] & 0xFF) << 16)
                | ((long) (digest[1] & 0xFF) << 8)
                | (digest[0] & 0xFF);
            return rv & 0xffffffffL;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Could not encode ketama hash.", e);
        }
    }

}
