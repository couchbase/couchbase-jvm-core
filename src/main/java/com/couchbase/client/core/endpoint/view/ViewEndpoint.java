package com.couchbase.client.core.endpoint.view;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.Environment;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;

/**
 * This endpoint defines the pipeline for binary requests and responses.
 */
public class ViewEndpoint extends AbstractEndpoint {

    private static final int PORT = 8092;

    /**
     * Create a new {@link ViewEndpoint}.
     *
     * @param hostname the hostname to connect on this endpoint.
     * @param env the couchbase environment.
     */
    public ViewEndpoint(final String hostname, final Environment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, env, responseBuffer);
    }

    @Override
    protected int port() {
        return PORT;
    }

    @Override
    protected void customEndpointHandlers(final ChannelPipeline pipeline) {
        pipeline
            .addLast(new HttpClientCodec())
            .addLast(new HttpObjectAggregator(Integer.MAX_VALUE))
            .addLast(new ViewCodec());
    }

}