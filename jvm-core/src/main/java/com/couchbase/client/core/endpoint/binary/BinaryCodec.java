package com.couchbase.client.core.endpoint.binary;

import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigResponse;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequestHeader;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequestHeader;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
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
		} else if (msg instanceof GetRequest) {
			GetRequest request = (GetRequest) msg;
			BinaryMemcacheRequestHeader header = new DefaultBinaryMemcacheRequestHeader();
			header.setOpcode(BinaryMemcacheOpcodes.GET);
			header.setKeyLength((short) request.key().length());
			header.setTotalBodyLength((short) request.key().length());
			header.setReserved((short) 28);
			out.add(new DefaultBinaryMemcacheRequest(header, request.key()));
		} else if (msg instanceof UpsertRequest) {
			UpsertRequest request = (UpsertRequest) msg;

			ByteBuf extras = ctx.alloc().buffer();
			extras.writeInt(0);
			extras.writeInt(0);

			BinaryMemcacheRequestHeader header = new DefaultBinaryMemcacheRequestHeader();
			header.setOpcode(BinaryMemcacheOpcodes.SET);
			header.setKeyLength((short) request.key().length());
			header.setTotalBodyLength((short) request.key().length() + request.content().readableBytes() + extras.readableBytes());
			header.setReserved((short) 28);
			header.setExtrasLength((byte) extras.readableBytes());

			out.add(new DefaultFullBinaryMemcacheRequest(header, request.key(), extras, request.content()));
		}
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg, List<Object> in) throws Exception {
		Class<?> clazz = queue.poll();

		if(clazz.equals(GetBucketConfigRequest.class)) {
			in.add(new GetBucketConfigResponse(msg.content().toString(CharsetUtil.UTF_8)));
		} else if (clazz.equals(GetRequest.class)) {
			in.add(new GetResponse(msg.content().toString(CharsetUtil.UTF_8)));
		} else if (clazz.equals(UpsertRequest.class)) {
			in.add(new UpsertResponse());
		}
	}

}
