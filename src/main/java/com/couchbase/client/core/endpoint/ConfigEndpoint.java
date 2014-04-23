package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.env.Environment;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelPipeline;

public class ConfigEndpoint extends AbstractEndpoint {

    public ConfigEndpoint(String hostname, Environment environment, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, environment, responseBuffer);
    }

    @Override
    protected int port() {
        return 8091;
    }

    @Override
    protected void customEndpointHandlers(ChannelPipeline pipeline) {

    }
}
