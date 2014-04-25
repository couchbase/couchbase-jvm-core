package com.couchbase.client.core.service;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.binary.BinaryEndpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.PartitionSelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public class BinaryService extends AbstractService {

    private static final SelectionStrategy strategy = new PartitionSelectionStrategy();

    public BinaryService(String hostname, Environment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, env, env.binaryServiceEndpoints(), strategy, responseBuffer);
    }

    @Override
    public ServiceType type() {
        return ServiceType.BINARY;
    }

    @Override
    protected Endpoint newEndpoint(final RingBuffer<ResponseEvent> responseBuffer) {
        return new BinaryEndpoint(hostname(), environment(), responseBuffer);
    }
}
