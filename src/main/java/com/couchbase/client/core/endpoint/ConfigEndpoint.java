package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.env.Environment;
import io.netty.channel.ChannelPipeline;

public class ConfigEndpoint extends AbstractEndpoint {

    public ConfigEndpoint(String hostname, Environment environment) {
        super(hostname, environment);
    }

    @Override
    protected int port() {
        return 0;
    }

    @Override
    protected void customEndpointHandlers(ChannelPipeline pipeline) {

    }
}
