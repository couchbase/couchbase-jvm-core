package com.couchbase.client.core.service;


import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.message.CouchbaseRequest;

public interface SelectionStrategy {

    Endpoint select(CouchbaseRequest request, Endpoint[] endpoints);
}
