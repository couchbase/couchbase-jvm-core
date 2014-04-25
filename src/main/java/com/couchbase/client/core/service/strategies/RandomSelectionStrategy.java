package com.couchbase.client.core.service.strategies;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.service.SelectionStrategy;
import com.couchbase.client.core.state.LifecycleState;

import java.util.Random;

public class RandomSelectionStrategy implements SelectionStrategy {

    @Override
    public Endpoint select(CouchbaseRequest request, Endpoint[] endpoints) {
        // TODO: this is not the right thing to burn! if no endpoint can be found in number of tries
        // TODO: then complete it with retry :/
        Endpoint endpoint;
        do {
            int rand = new Random().nextInt(endpoints.length);
            endpoint = endpoints[rand];
        } while(endpoint.state() != LifecycleState.CONNECTED);
        return endpoint;
    }
}
