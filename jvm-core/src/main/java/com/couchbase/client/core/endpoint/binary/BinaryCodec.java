package com.couchbase.client.core.endpoint.binary;

import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.memcache.MemcacheMessage;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequestHeader;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequestHeader;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.util.CharsetUtil;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class BinaryCodec extends MessageToMessageCodec<FullBinaryMemcacheResponse, BinaryRequest> {

	private final Queue<Class<?>> queue = new ArrayDeque<Class<?>>();

	@Override
	protected void encode(ChannelHandlerContext ctx, BinaryRequest msg, List<Object> out) throws Exception {
		queue.offer(msg.getClass());

		if (msg instanceof GetBucketConfigRequest) {
			BinaryMemcacheRequestHeader header = new DefaultBinaryMemcacheRequestHeader();
			header.setOpcode((byte) 0xb5);
			out.add(new DefaultBinaryMemcacheRequest(header));
		}
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg, List<Object> in) throws Exception {
		Class<?> clazz = queue.poll();

		if(clazz.equals(GetBucketConfigRequest.class)) {
			in.add(new GetBucketConfigResponse(msg.content().toString(CharsetUtil.UTF_8)));
		}
	}

}
