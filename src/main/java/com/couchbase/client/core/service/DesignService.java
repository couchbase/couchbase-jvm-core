package com.couchbase.client.core.service;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.KeyHashSelectionStrategy;

/**
 * Created by michael on 16/04/14.
 */
public class DesignService extends AbstractService {

    private static final SelectionStrategy strategy = new KeyHashSelectionStrategy();

    public DesignService(String hostname, Environment env) {
        super(hostname, env, env.designServiceEndpoints(), strategy);
    }

    @Override
    public ServiceType type() {
        return ServiceType.DESIGN;
    }

    @Override
    protected Endpoint newEndpoint() {
        throw new UnsupportedOperationException("implement me");
    }
}
