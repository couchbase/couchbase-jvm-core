package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.BinaryEndpoint;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.KeyHashSelectionStrategy;

public class BinaryService extends AbstractService {

    private static final SelectionStrategy strategy = new KeyHashSelectionStrategy();

    public BinaryService(String hostname, Environment env) {
        super(hostname, env, env.binaryServiceEndpoints(), strategy);
    }

    @Override
    public ServiceType type() {
        return ServiceType.BINARY;
    }

    @Override
    protected Endpoint newEndpoint() {
        return new BinaryEndpoint(hostname(), environment());
    }
}
