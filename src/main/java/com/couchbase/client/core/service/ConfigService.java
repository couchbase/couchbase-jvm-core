package com.couchbase.client.core.service;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.config.ConfigEndpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.PartitionSelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public class ConfigService extends AbstractService {

    private static final SelectionStrategy strategy = new PartitionSelectionStrategy();
    private static final EndpointFactory factory = new ConfigEndpointFactory();

    public ConfigService(String hostname, String bucket, String password, int port, Environment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, env.configServiceEndpoints(), strategy, responseBuffer, factory);
    }

    @Override
    public ServiceType type() {
        return ServiceType.CONFIG;
    }

    static class ConfigEndpointFactory implements EndpointFactory {
        @Override
        public Endpoint create(String hostname, String bucket, String password, int port, Environment env,
                               RingBuffer<ResponseEvent> responseBuffer) {
            return new ConfigEndpoint(hostname, bucket, password, port, env, responseBuffer);
        }
    }

}
