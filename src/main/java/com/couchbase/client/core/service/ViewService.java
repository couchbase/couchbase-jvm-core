package com.couchbase.client.core.service;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.view.ViewEndpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.RandomSelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public class ViewService extends AbstractService {

    private static final SelectionStrategy strategy = new RandomSelectionStrategy();

    public ViewService(String hostname, String bucket, String password, Environment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, env, env.viewServiceEndpoints(), strategy, responseBuffer);
    }

    @Override
    public ServiceType type() {
        return ServiceType.VIEW;
    }

    @Override
    protected Endpoint newEndpoint(final RingBuffer<ResponseEvent> responseBuffer) {
        return new ViewEndpoint(hostname(), bucket(), password(), environment(), responseBuffer);
    }
}
