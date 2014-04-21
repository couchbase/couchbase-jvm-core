package com.couchbase.client.core.service;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.KeyHashSelectionStrategy;
import com.lmax.disruptor.RingBuffer;

/**
 * Created by michael on 16/04/14.
 */
public class DesignService extends AbstractService {

    private static final SelectionStrategy strategy = new KeyHashSelectionStrategy();

    public DesignService(String hostname, Environment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, env, env.designServiceEndpoints(), strategy, responseBuffer);
    }

    @Override
    public ServiceType type() {
        return ServiceType.DESIGN;
    }

    @Override
    protected Endpoint newEndpoint(final RingBuffer<ResponseEvent> responseBuffer) {
        throw new UnsupportedOperationException("implement me");
    }
}
