package com.couchbase.client.core.service;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.query.QueryEndpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.service.strategies.RandomSelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public class QueryService extends AbstractService {

    private static final SelectionStrategy strategy = new RandomSelectionStrategy();
    private static final EndpointFactory factory = new QueryEndpointFactory();

    public QueryService(String hostname, String bucket, String password, int port, Environment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, env.queryServiceEndpoints(), strategy, responseBuffer, factory);
    }

    @Override
    public ServiceType type() {
        return ServiceType.QUERY;
    }

    static class QueryEndpointFactory implements EndpointFactory {
        @Override
        public Endpoint create(String hostname, String bucket, String password, int port, Environment env,
            RingBuffer<ResponseEvent> responseBuffer) {
            return new QueryEndpoint(hostname, bucket, password, port, env, responseBuffer);
        }
    }
}
