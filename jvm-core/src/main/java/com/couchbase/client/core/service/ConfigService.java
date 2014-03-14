package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.config.ConfigEndpoint;
import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.service.strategies.RandomSelectionStrategy;
import java.net.InetSocketAddress;

public class ConfigService extends AbstractService {

    private static final SelectionStrategy strategy = new RandomSelectionStrategy();

    public ConfigService(InetSocketAddress address, Environment env) {
        super(address, env, 1, strategy);
    }

    @Override
    protected Endpoint newEndpoint() {
        return new ConfigEndpoint(address(), environment());
    }
}
