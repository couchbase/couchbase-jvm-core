package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcacheBucketConfig;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.node.Node;
import io.netty.util.CharsetUtil;
import rx.Observable;

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
    public Observable<Node> locate(final CouchbaseRequest request, final Set<Node> nodes, final ClusterConfig cluster) {
        if (request instanceof GetBucketConfigRequest) {
            return Observable.from(nodes);
        }

        System.out.println("Bucket for request is: " + request.bucket());

        BucketConfig bucket = cluster.bucketConfig(request.bucket());
        if (bucket instanceof CouchbaseBucketConfig) {
            return locateForCouchbaseBucket((BinaryRequest) request, nodes, (CouchbaseBucketConfig) bucket);
        } else if (bucket instanceof MemcacheBucketConfig) {
            return locateForMemcacheBucket((BinaryRequest) request, nodes, (MemcacheBucketConfig) bucket);
        } else {
            throw new IllegalStateException("Unsupported Bucket Type: " + bucket);
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
    private Observable<Node> locateForCouchbaseBucket(final BinaryRequest request, final Set<Node> nodes,
        final CouchbaseBucketConfig config) {
        String key = request.key();

        CRC32 crc32 = new CRC32();
        crc32.update(key.getBytes(CharsetUtil.UTF_8));
        long rv = (crc32.getValue() >> 16) & 0x7fff;
        int partitionId = (int) rv & config.partitions().size() - 1;
        request.partition((short) partitionId);

        int nodeId = config.partitions().get(partitionId).master();
        String hostname = config.partitionHosts().get(nodeId);
        for (Node node : nodes) {
            if (node.hostname().equals(hostname)) {
                return Observable.from(node);
            }
        }
        return Observable.error(new IllegalStateException("Node not found for request" + request));
    }

    /**
     * Locates the proper {@link Node}s for a Memcache bucket.
     *
     * @param request the request.
     * @param nodes the managed nodes.
     * @param config the bucket configuration.
     * @return an observable with one or more nodes to send the request to.
     */
    private Observable<Node> locateForMemcacheBucket(final BinaryRequest request, final Set<Node> nodes,
        final MemcacheBucketConfig config) {
        // todo: ketama lookup
        throw new UnsupportedOperationException("implement me");
    }

}
