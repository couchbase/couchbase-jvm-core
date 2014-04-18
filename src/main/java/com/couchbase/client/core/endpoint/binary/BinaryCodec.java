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

package com.couchbase.client.core.endpoint.binary;

import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigResponse;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.memcache.binary.*;
import io.netty.util.CharsetUtil;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * Codec that handles encoding of binary memcache requests and decoding of binary memcache responses.
 */
public class BinaryCodec extends MessageToMessageCodec<FullBinaryMemcacheResponse, BinaryRequest> {

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<Class<?>> queue;

    /**
     * Creates a new {@link BinaryCodec} with the default dequeue.
     */
    public BinaryCodec() {
        this(new ArrayDeque<Class<?>>());
    }

    /**
     * Creates a new {@link BinaryCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public BinaryCodec(final Queue<Class<?>> queue) {
        this.queue = queue;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final BinaryRequest msg, final List<Object> out)
        throws Exception {

        BinaryMemcacheRequest request;
        if (msg instanceof GetBucketConfigRequest) {
            request = handleGetBucketConfigRequest();
        } else if (msg instanceof GetRequest) {
            request = handleGetRequest((GetRequest) msg);
        } else if (msg instanceof UpsertRequest) {
            request = handleUpsertRequest((UpsertRequest) msg, ctx);
        } else {
            throw new IllegalArgumentException("Unknown Messgae to encode: " + msg);
        }

        out.add(request);
        queue.offer(msg.getClass());
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final FullBinaryMemcacheResponse msg, final List<Object> in)
        throws Exception {
        Class<?> clazz = queue.poll();

        if(clazz.equals(GetBucketConfigRequest.class)) {
            in.add(new GetBucketConfigResponse(msg.content().toString(CharsetUtil.UTF_8)));
        } else if (clazz.equals(GetRequest.class)) {
            in.add(new GetResponse(msg.content().toString(CharsetUtil.UTF_8)));
        } else if (clazz.equals(UpsertRequest.class)) {
            in.add(new UpsertResponse());
        } else {
            throw new IllegalStateException("Got a response message for a request that was not sent." + msg);
        }
    }

    /**
     * Creates the actual protocol level request for an incoming get request.
     *
     * @param request the incoming get request.
     * @return the built protocol request.
     */
    private BinaryMemcacheRequest handleGetRequest(final GetRequest request) {
        int length = request.key().length();
        BinaryMemcacheRequest msg = new DefaultBinaryMemcacheRequest(request.key());
        msg.setOpcode(BinaryMemcacheOpcodes.GET);
        msg.setKeyLength((short) length);
        msg.setTotalBodyLength((short) length);
        msg.setReserved(request.partition());
        return msg;
    }

    /**
     * Creates the actual protocol level request for an incoming upsert request.
     *
     * @param request the incoming upsert request.
     * @param ctx the channel handler context for buffer allocations.
     * @return the built protocol request.
     */
    private BinaryMemcacheRequest handleUpsertRequest(final UpsertRequest request, final ChannelHandlerContext ctx) {
        ByteBuf extras = ctx.alloc().buffer(8);
        extras.writeInt(request.flags());
        extras.writeInt(request.expiration());

        FullBinaryMemcacheRequest msg = new DefaultFullBinaryMemcacheRequest(request.key(), extras, request.content());

        msg.setOpcode(BinaryMemcacheOpcodes.SET);
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength((short) request.key().length() + request.content().readableBytes() + extras.readableBytes());
        msg.setReserved(request.partition());
        msg.setExtrasLength((byte) extras.readableBytes());

        return msg;
    }

    /**
     * Creates the actual protocol level request for an incoming bucket config request.
     *
     * @return the built protocol request.
     */
    private BinaryMemcacheRequest handleGetBucketConfigRequest() {
        BinaryMemcacheRequest msg = new DefaultBinaryMemcacheRequest();
        msg.setOpcode((byte) 0xb5);
        return msg;
    }

}