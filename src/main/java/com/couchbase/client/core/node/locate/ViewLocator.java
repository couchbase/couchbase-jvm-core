package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.node.Node;
import rx.Observable;

import java.util.Set;

public class ViewLocator implements Locator {

    private long counter = 0;

    @Override
    public Observable<Node> locate(CouchbaseRequest request, Set<Node> nodes, ClusterConfig config) {
        int item = (int) counter % nodes.size();
        int i = 0;
        for (Node node : nodes) {
            if (i++ == item) {
                return Observable.from(node);
            }
        }
        return Observable.error(new IllegalStateException("Node not found for request" + request));
    }
}
