package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.env.Environment;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelPipeline;

public class ConfigEndpoint extends AbstractEndpoint {

    public ConfigEndpoint(String hostname, String bucket, String password, int port, Environment environment, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, environment, responseBuffer);
    }

    @Override
    protected void customEndpointHandlers(ChannelPipeline pipeline) {

    }
}
