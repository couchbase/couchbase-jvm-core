package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.config.FlushRequest;
import com.couchbase.client.core.message.config.GetDesignDocumentsRequest;
import com.couchbase.client.core.node.Node;

import java.util.Set;

public class ConfigLocator implements Locator {

    private long counter = 0;

    @Override
    public Node[] locate(final CouchbaseRequest request, final Set<Node> nodes, final ClusterConfig config) {
        if (request instanceof FlushRequest || request instanceof GetDesignDocumentsRequest) {
            int item = (int) counter % nodes.size();
            int i = 0;
            for (Node node : nodes) {
                if (i++ == item) {
                    return new Node[] { node };
                }
            }
        } else if (request instanceof BucketConfigRequest) {
            BucketConfigRequest req = (BucketConfigRequest) request;
            for (Node node : nodes) {
                if (node.hostname().equals(req.hostname())) {
                    return new Node[]{node};
                }
            }
        } else if (request instanceof BucketStreamingRequest) {
            int item = (int) counter % nodes.size();
            int i = 0;
            for (Node node : nodes) {
                if (i++ == item) {
                    return new Node[] { node };
                }
            }
        } else {
            throw new IllegalStateException("Unknown request " + request);
        }
        return new Node[0];
    }

}
