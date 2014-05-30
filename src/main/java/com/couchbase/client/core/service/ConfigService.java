package com.couchbase.client.core.service;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.config.ConfigEndpoint;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.service.strategies.RandomSelectionStrategy;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import rx.Subscriber;

import java.util.ArrayList;
import java.util.List;

public class ConfigService extends AbstractService {

    private static final SelectionStrategy strategy = new RandomSelectionStrategy();
    private static final EndpointFactory factory = new ConfigEndpointFactory();
    private static final int INITIAL_ENDPOINTS = 1;

    private final String hostname;
    private final String bucket;
    private final String password;
    private final int port;
    private final Environment env;
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * Contains a list of pinned {@link Endpoint}s.
     */
    private final List<Endpoint> pinnedEndpoints;

    public ConfigService(String hostname, String bucket, String password, int port, Environment env,
        final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, INITIAL_ENDPOINTS, strategy, responseBuffer, factory);
        pinnedEndpoints = new ArrayList<Endpoint>();
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
        this.port = port;
        this.env = env;
        this.responseBuffer = responseBuffer;
    }

    @Override
    public ServiceType type() {
        return ServiceType.CONFIG;
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (request instanceof BucketStreamingRequest) {
            final Endpoint endpoint = factory.create(hostname, bucket, password, port, env, responseBuffer);
            endpointStates.add(endpoint.states());
            endpoint
                .connect()
                .subscribe(new Subscriber<LifecycleState>() {
                    @Override
                    public void onCompleted() {
                        pinnedEndpoints.add(endpoint);
                        endpoint.send(request);
                        endpoint.send(SignalFlush.INSTANCE);
                    }

                    @Override
                    public void onError(Throwable e) {
                        request.observable().onError(e);
                    }

                    @Override
                    public void onNext(LifecycleState state) {

                    }
                });
        } else {
            super.send(request);
        }
    }

    static class ConfigEndpointFactory implements EndpointFactory {
        @Override
        public Endpoint create(String hostname, String bucket, String password, int port, Environment env,
                               RingBuffer<ResponseEvent> responseBuffer) {
            return new ConfigEndpoint(hostname, bucket, password, port, env, responseBuffer);
        }
    }

}
