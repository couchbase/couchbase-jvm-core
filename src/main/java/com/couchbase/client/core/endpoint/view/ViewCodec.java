package com.couchbase.client.core.endpoint.view;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.message.view.ViewRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class ViewCodec extends MessageToMessageCodec<FullHttpResponse, ViewRequest> {

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<Class<?>> queue;

    /**
     * Creates a new {@link ViewCodec} with the default dequeue.
     */
    public ViewCodec() {
        this(new ArrayDeque<Class<?>>());
    }

    /**
     * Creates a new {@link ViewCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public ViewCodec(final Queue<Class<?>> queue) {
        this.queue = queue;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ViewRequest msg, List<Object> out) throws Exception {
        HttpRequest request;
        if (msg instanceof ViewQueryRequest) {
            request = handleViewQueryRequest((ViewQueryRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown Messgae to encode: " + msg);
        }
        out.add(request);
        queue.offer(msg.getClass());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpResponse msg, List<Object> in) throws Exception {
        Class<?> clazz = queue.poll();

        if (clazz.equals(ViewQueryRequest.class)) {
            in.add(new ViewQueryResponse(ResponseStatus.OK, msg.content().copy()));
        } else {
            throw new IllegalStateException("Got a response message for a request that was not sent." + msg);
        }
    }

    private HttpRequest handleViewQueryRequest(final ViewQueryRequest msg) {
        String uri = "/" + msg.bucket() + "/_design/" + msg.design() + "/_view/" + msg.view();
        if (msg.query() != null && !msg.query().isEmpty()) {
            uri = uri + "?" + msg.query();
        }
        return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    }
}
