package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.config.Configuration;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.node.Node;
import rx.Observable;

import java.util.Set;

public class BinaryLocator implements Locator {

    @Override
    public Observable<Node> locate(final CouchbaseRequest request, final Set<Node> nodes, final Configuration config) {
        return Observable.from(nodes.iterator().next());
    }
}
