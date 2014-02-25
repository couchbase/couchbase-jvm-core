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

/**
 * Codec that handles encoding of config requests and decoding of config responses.
 */
public class ConfigCodec extends MessageToMessageCodec<FullHttpResponse, ConfigRequest> {

    /**
     * The default HTTP version to use.
     */
    private static final HttpVersion HTTP_VERSION = HttpVersion.HTTP_1_1;

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<Class<?>> queue;

    /**
     * Creates a new {@link ConfigCodec} with the default dequeue.
     */
    public ConfigCodec() {
        this(new ArrayDeque<Class<?>>());
    }

    /**
     * Creates a new {@link ConfigCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public ConfigCodec(final Queue<Class<?>> queue) {
        this.queue = queue;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final ConfigRequest msg, final List<Object> out)
        throws Exception {
        HttpRequest request;
        if (msg instanceof GetBucketConfigRequest) {
            request = handleGetBucketConfigRequest((GetBucketConfigRequest) msg);
        } else {
            throw new IllegalStateException("Got a response message for a request that was not sent." + msg);
        }

        out.add(request);
        queue.offer(msg.getClass());
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final FullHttpResponse msg, final List<Object> in)
        throws Exception {
        Class<?> clazz = queue.poll();

        if(clazz.equals(GetBucketConfigRequest.class)) {
            in.add(new GetBucketConfigResponse(msg.content().toString(CharsetUtil.UTF_8)));
        }
    }

    /**
     * Creates the actual protocol level request for an incoming bucket config request.
     *
     * @param request the incoming request.
     * @return the actual protocl level request.
     */
    private static HttpRequest handleGetBucketConfigRequest(final GetBucketConfigRequest request) {
        String path = "/pools/default/buckets/" + request.bucket();
        return new DefaultFullHttpRequest(HTTP_VERSION, HttpMethod.GET, path);
    }
}
