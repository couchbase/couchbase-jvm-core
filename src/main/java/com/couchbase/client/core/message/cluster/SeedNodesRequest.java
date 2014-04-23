package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

import java.util.Arrays;
import java.util.List;

/**
 * Sends the targeted bootstrap seed nodes before opening a bucket.
 */
public class SeedNodesRequest extends AbstractCouchbaseRequest implements ClusterRequest {

    private List<String> nodes;

    public SeedNodesRequest(final String node) {
        this(Arrays.asList(node));
    }

    public SeedNodesRequest(final List<String> nodes) {
        super(null, null);
        this.nodes = nodes;
    }

    public List<String> nodes() {
        return nodes;
    }
}
