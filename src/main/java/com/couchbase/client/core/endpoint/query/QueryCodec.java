/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.endpoint.query;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.message.query.QueryRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * The codec responsible for encoding and decoding query messages.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class QueryCodec extends MessageToMessageCodec<FullHttpResponse, QueryRequest> {

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<Class<?>> queue;

    /**
     * Creates a new {@link QueryCodec} with the default dequeue.
     */
    public QueryCodec() {
        this(new ArrayDeque<Class<?>>());
    }

    /**
     * Creates a new {@link QueryCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public QueryCodec(final Queue<Class<?>> queue) {
        this.queue = queue;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final QueryRequest msg, final List<Object> out)
        throws Exception {
        HttpRequest request;
        if (msg instanceof GenericQueryRequest) {
            request = handleGenericQueryRequest(ctx, (GenericQueryRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown Message to encode: " + msg);
        }
        out.add(request);
        queue.offer(msg.getClass());
    }

    private HttpRequest handleGenericQueryRequest(ChannelHandlerContext ctx, GenericQueryRequest msg) {
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/query");
        ByteBuf query = Unpooled.copiedBuffer(msg.query(), CharsetUtil.UTF_8);
        request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, query.readableBytes());
        request.content().writeBytes(query);
        return request;
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final FullHttpResponse msg, final List<Object> out)
        throws Exception {
        Class<?> request = queue.poll();
        if (request.equals(GenericQueryRequest.class)) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readValue(msg.content().toString(CharsetUtil.UTF_8), JsonNode.class);
            if (root.has("resultset")) {
                for(JsonNode result : root.path("resultset")) {
                    out.add(new GenericQueryResponse(result.toString(), ResponseStatus.CHUNKED, null));
                }
                out.add(new GenericQueryResponse(null, ResponseStatus.SUCCESS, null)); // todo: fixme
            } else if (root.has("error")) {
                out.add(new GenericQueryResponse(root.get("error").toString(), ResponseStatus.FAILURE, null));
            } else {
                throw new IllegalStateException("Dont know how to deal with that response.");
            }
        }
    }
}
