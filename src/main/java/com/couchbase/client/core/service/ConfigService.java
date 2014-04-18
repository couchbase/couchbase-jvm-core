package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.ConfigEndpoint;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.KeyHashSelectionStrategy;

public class ConfigService extends AbstractService {

    private static final SelectionStrategy strategy = new KeyHashSelectionStrategy();

    public ConfigService(String hostname, Environment env) {
        super(hostname, env, env.configServiceEndpoints(), strategy);
    }

    @Override
    public ServiceType type() {
        return ServiceType.CONFIG;
    }

    @Override
    protected Endpoint newEndpoint() {
        return new ConfigEndpoint(hostname(), environment());
    }
}
