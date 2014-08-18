package com.couchbase.client.core.service;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.binary.BinaryEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.service.strategies.PartitionSelectionStrategy;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public class BinaryService extends AbstractService {

    private static final SelectionStrategy strategy = new PartitionSelectionStrategy();
    private static final EndpointFactory factory = new BinaryEndpointFactory();

    public BinaryService(String hostname, String bucket, String password, int port, CoreEnvironment env,
        final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, env.binaryServiceEndpoints(), strategy, responseBuffer, factory);
    }

    @Override
    public ServiceType type() {
        return ServiceType.BINARY;
    }

    static class BinaryEndpointFactory implements EndpointFactory {
        @Override
        public Endpoint create(String hostname, String bucket, String password, int port, CoreEnvironment env,
            RingBuffer<ResponseEvent> responseBuffer) {
            return new BinaryEndpoint(hostname, bucket, password, port, env, responseBuffer);
        }
    }
}
