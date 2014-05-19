package com.couchbase.client.core.service.strategies;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.service.SelectionStrategy;
import com.couchbase.client.core.state.LifecycleState;

public class RandomSelectionStrategy implements SelectionStrategy {

    private volatile short next;

    @Override
    public Endpoint select(CouchbaseRequest request, Endpoint[] endpoints) {
        for (int i = 0; i < endpoints.length; i++) {
            Endpoint endpoint = endpoints[next++ % endpoints.length];
            if (endpoint.isState(LifecycleState.CONNECTED)) {
                return endpoint;
            }
        }
        return null;
    }
}
