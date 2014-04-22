package com.couchbase.client.core.service.strategies;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.service.SelectionStrategy;
import com.couchbase.client.core.state.LifecycleState;

public class KeyHashSelectionStrategy implements SelectionStrategy {

    @Override
    public Endpoint select(CouchbaseRequest request, Endpoint[] endpoints) {
        if (request instanceof GetBucketConfigRequest) {
            for (Endpoint endpoint : endpoints) {
                if (endpoint.state() == LifecycleState.CONNECTED) {
                    return endpoint;
                }
            }
        } else if (request instanceof BinaryRequest) {
            BinaryRequest binaryRequest = (BinaryRequest) request;
            short partition = binaryRequest.partition();
            int id = partition % endpoints.length;
            return endpoints[id];
        }
        return null;
    }
}
