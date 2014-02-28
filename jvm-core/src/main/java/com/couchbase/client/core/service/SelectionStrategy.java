package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.message.CouchbaseRequest;
import reactor.event.registry.Registry;

/**
 * The strategy on how the endpoint is selected.
 */
public interface SelectionStrategy {

    /**
     * Select a endpoint from the registry.
     *
     * @param endpointRegistry the registry with all endpoints.
     * @return the selected endpoint.
     */
    Endpoint select(Registry<Endpoint> endpointRegistry, CouchbaseRequest request);
}
