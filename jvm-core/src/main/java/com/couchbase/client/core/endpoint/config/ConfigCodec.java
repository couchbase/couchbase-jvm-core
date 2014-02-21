package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.config.GetBucketConfigRequest;
import com.couchbase.client.core.message.config.GetBucketConfigResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class ConfigCodec extends MessageToMessageCodec<FullHttpResponse, ConfigRequest> {

    private final Queue<Class<?>> queue = new ArrayDeque<Class<?>>();

    @Override
    protected void encode(ChannelHandlerContext ctx, ConfigRequest msg, List<Object> out) throws Exception {
        queue.offer(msg.getClass());

        if (msg instanceof GetBucketConfigRequest) {
            HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/pools/default/buckets/" + msg.bucket());
            out.add(request);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpResponse msg, List<Object> in) throws Exception {
        Class<?> clazz = queue.poll();

        if(clazz.equals(GetBucketConfigRequest.class)) {
            in.add(new GetBucketConfigResponse(msg.content().toString(CharsetUtil.UTF_8)));
        }
    }
}
