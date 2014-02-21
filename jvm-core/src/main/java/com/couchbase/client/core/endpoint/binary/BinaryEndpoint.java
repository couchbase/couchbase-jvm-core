package com.couchbase.client.core.endpoint.binary;

import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.environment.Environment;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheClientCodec;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheObjectAggregator;

import java.net.InetSocketAddress;

public class BinaryEndpoint extends AbstractEndpoint {

	public BinaryEndpoint(InetSocketAddress address, Environment env) {
		super(address, env);
	}

	@Override
	protected void customEndpointHandlers(ChannelPipeline pipeline) {
		pipeline
			.addLast(new BinaryMemcacheClientCodec())
			.addLast(new BinaryMemcacheObjectAggregator(Integer.MAX_VALUE))
			.addLast(new BinaryCodec());
	}
}
