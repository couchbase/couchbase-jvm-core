package com.couchbase.client.core.service.strategies;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.service.SelectionStrategy;
import reactor.event.registry.Registry;

public class RandomSelectionStrategy implements SelectionStrategy {

    @Override
    public Endpoint select(final Registry<Endpoint> endpointRegistry, final CouchbaseRequest request) {
        return null;
    }
}
