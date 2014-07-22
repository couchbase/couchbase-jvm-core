package com.couchbase.client.core.endpoint.view;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.Environment;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;

/**
 * This endpoint defines the pipeline for binary requests and responses.
 */
public class ViewEndpoint extends AbstractEndpoint {

    /**
     * Create a new {@link ViewEndpoint}.
     *
     * @param hostname the hostname to connect on this endpoint.
     * @param env the couchbase environment.
     */
    public ViewEndpoint(final String hostname, String bucket, String password, int port, final Environment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, responseBuffer);
    }

    @Override
    protected void customEndpointHandlers(final ChannelPipeline pipeline) {
        pipeline
            .addLast(new HttpClientCodec())
            .addLast(new ViewHandler(this, responseBuffer()));
    }

}