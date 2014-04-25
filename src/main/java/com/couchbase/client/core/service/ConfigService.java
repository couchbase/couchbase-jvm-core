package com.couchbase.client.core.service;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.ConfigEndpoint;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.PartitionSelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public class ConfigService extends AbstractService {

    private static final SelectionStrategy strategy = new PartitionSelectionStrategy();

    public ConfigService(String hostname, Environment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, env, env.configServiceEndpoints(), strategy, responseBuffer);
    }

    @Override
    public ServiceType type() {
        return ServiceType.CONFIG;
    }

    @Override
    protected Endpoint newEndpoint(final RingBuffer<ResponseEvent> responseBuffer) {
        return new ConfigEndpoint(hostname(), environment(), responseBuffer);
    }
}
