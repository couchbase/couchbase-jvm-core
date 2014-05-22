package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.config.TerseBucketConfigRequest;
import com.couchbase.client.core.message.config.VerboseBucketConfigRequest;
import com.couchbase.client.core.node.Node;

import java.util.Set;

/**
 * Created by michael on 22/05/14.
 */
public class ConfigLocator implements Locator {

    @Override
    public Node[] locate(CouchbaseRequest request, Set<Node> nodes, ClusterConfig config) {
        if (request instanceof TerseBucketConfigRequest) {
            TerseBucketConfigRequest req = (TerseBucketConfigRequest) request;
            for (Node node : nodes) {
                if (node.hostname().equals(req.hostname())) {
                    return new Node[] { node };
                }
            }
        } else if (request instanceof VerboseBucketConfigRequest) {
            VerboseBucketConfigRequest req = (VerboseBucketConfigRequest) request;
            for (Node node : nodes) {
                if (node.hostname().equals(req.hostname())) {
                    return new Node[] { node };
                }
            }
        } else {
            throw new IllegalStateException("Unknown request" + request);
        }
        return new Node[0];
    }
}
