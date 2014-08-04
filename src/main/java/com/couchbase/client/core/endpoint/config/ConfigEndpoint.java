package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;

public class ConfigEndpoint extends AbstractEndpoint {

    public ConfigEndpoint(String hostname, String bucket, String password, int port, CoreEnvironment environment, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, environment, responseBuffer);
    }

    @Override
    protected void customEndpointHandlers(ChannelPipeline pipeline) {
        pipeline
            .addLast(new HttpClientCodec())
            .addLast(new ConfigHandler(this, responseBuffer()));
    }
}
