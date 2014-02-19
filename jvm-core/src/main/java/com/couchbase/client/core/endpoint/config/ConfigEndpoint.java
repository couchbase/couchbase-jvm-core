package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.environment.Environment;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;

import java.net.InetSocketAddress;

public class ConfigEndpoint extends AbstractEndpoint {

    public ConfigEndpoint(InetSocketAddress address, Environment env) {
        super(address, env);
    }

    @Override
    protected void customEndpointHandlers(ChannelPipeline pipeline) {
        pipeline
            .addLast(new HttpClientCodec())
            .addLast(new HttpObjectAggregator(Integer.MAX_VALUE))
            .addLast(new ConfigCodec());
    }
}
