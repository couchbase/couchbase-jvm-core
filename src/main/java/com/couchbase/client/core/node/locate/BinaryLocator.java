package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcacheBucketConfig;
import com.couchbase.client.core.config.Partition;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.message.binary.ReplicaGetRequest;
import com.couchbase.client.core.node.Node;

import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * This {@link Locator} finds the proper {@link Node}s for every incoming {@link CouchbaseRequest}.
 *
 * Depending on the bucket type used, it either uses partition/vbucket (couchbase) or ketama (memcache) hashing. For
 * broadcast-type operations, it will return all suitable nodes without hashing by key.
 */
public class BinaryLocator implements Locator {

    @Override
    public Node[] locate(final CouchbaseRequest request, final Set<Node> nodes, final ClusterConfig cluster) {
        if (request instanceof GetBucketConfigRequest) {
            GetBucketConfigRequest req = (GetBucketConfigRequest) request;
            for (Node node : nodes) {
                if (node.hostname().equals(req.hostname())) {
                    return new Node[] { node };
                }
            }
            throw new IllegalStateException("Node not found for request" + request);
        }

        BucketConfig bucket = cluster.bucketConfig(request.bucket());
        if (bucket instanceof CouchbaseBucketConfig) {
            return locateForCouchbaseBucket((BinaryRequest) request, nodes, (CouchbaseBucketConfig) bucket);
        } else if (bucket instanceof MemcacheBucketConfig) {
            return locateForMemcacheBucket((BinaryRequest) request, nodes, (MemcacheBucketConfig) bucket);
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
        int nodeId = request instanceof ReplicaGetRequest
            ? partition.replica(((ReplicaGetRequest) request).replica()-1) : partition.master();

        if (nodeId == -2) {
            request.observable().onError(new ReplicaNotConfiguredException("Replica number "
                + ((ReplicaGetRequest) request).replica() + " not configured for bucket " + config.name()));
            return null;
        }
        if (nodeId == -1) {
            return new Node[] { };
        }
        String hostname = config.partitionHosts().get(nodeId);
        for (Node node : nodes) {
            if (node.hostname().getHostName().equals(hostname)) {
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
        final MemcacheBucketConfig config) {
        // todo: ketama lookup
        throw new UnsupportedOperationException("implement me");
    }

}
