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

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigResponse;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.InsertRequest;
import com.couchbase.client.core.message.binary.InsertResponse;
import com.couchbase.client.core.message.binary.RemoveRequest;
import com.couchbase.client.core.message.binary.RemoveResponse;
import com.couchbase.client.core.message.binary.ReplaceRequest;
import com.couchbase.client.core.message.binary.ReplaceResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * Codec that handles encoding of binary memcache requests and decoding of binary memcache responses.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class BinaryCodec extends MessageToMessageCodec<FullBinaryMemcacheResponse, BinaryRequest> {

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<BinaryRequest> queue;

    private String bucket;

    /**
     * Creates a new {@link BinaryCodec} with the default dequeue.
     */
    public BinaryCodec() {
        this(new ArrayDeque<BinaryRequest>());
    }

    /**
     * Creates a new {@link BinaryCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public BinaryCodec(final Queue<BinaryRequest> queue) {
        this.queue = queue;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final BinaryRequest msg, final List<Object> out)
        throws Exception {
        if (bucket == null) {
            bucket = msg.bucket();
        }

        BinaryMemcacheRequest request;
        if (msg instanceof GetBucketConfigRequest) {
            request = handleGetBucketConfigRequest();
        } else if (msg instanceof GetRequest) {
            request = handleGetRequest((GetRequest) msg);
        } else if (msg instanceof UpsertRequest) {
            request = handleUpsertRequest((UpsertRequest) msg, ctx);
        } else if (msg instanceof InsertRequest) {
            request = handleInsertRequest((InsertRequest) msg, ctx);
        } else if (msg instanceof ReplaceRequest) {
            request = handleReplaceRequest((ReplaceRequest) msg, ctx);
        } else if (msg instanceof RemoveRequest) {
            request = handleRemoveRequest((RemoveRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown Messgae to encode: " + msg);
        }

        out.add(request);
        queue.offer(msg);
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx,
                          final FullBinaryMemcacheResponse msg,
                          final List<Object> in) throws Exception {
        BinaryRequest current = queue.poll();

        ResponseStatus status = convertStatus(msg.getStatus());
        CouchbaseRequest currentRequest = null;
        if (status == ResponseStatus.RETRY) {
            currentRequest = current;
        }

        long cas = msg.getCAS();
        if(current instanceof GetBucketConfigRequest) {
            InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
            in.add(
                new GetBucketConfigResponse(
                    convertStatus(msg.getStatus()),
                    bucket,
                    msg.content().copy(),
                    InetAddress.getByName(addr.getHostName())
                )
            );
        } else if (current instanceof GetRequest) {
            in.add(new GetResponse(status, cas, bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof InsertRequest) {
            in.add(new InsertResponse(status, cas, bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof UpsertRequest) {
            in.add(new UpsertResponse(status, cas, bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof ReplaceRequest) {
            in.add(new ReplaceResponse(status, cas, bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof RemoveRequest) {
            in.add(new RemoveResponse(convertStatus(msg.getStatus()), bucket, msg.content().copy(), currentRequest));
        } else {
            throw new IllegalStateException("Got a response message for a request that was not sent." + msg);
        }
    }

    /**
     * Convert the binary protocol status in a typesafe enum that can be acted upon later.
     *
     * @param status the status to convert.
     * @return the converted response status.
     */
    private ResponseStatus convertStatus(short status) {
        switch (status) {
            case BinaryMemcacheResponseStatus.SUCCESS:
                return ResponseStatus.SUCCESS;
            case BinaryMemcacheResponseStatus.KEY_EEXISTS:
                return ResponseStatus.EXISTS;
            case BinaryMemcacheResponseStatus.KEY_ENOENT:
                return ResponseStatus.NOT_EXISTS;
            case 0x07: // Represents NOT_MY_VBUCKET
                return ResponseStatus.RETRY;
            default:
                return ResponseStatus.FAILURE;
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
     * Creates the actual protocol level request for an incoming replacer request.
     *
     * @param request the incoming replace request.
     * @param ctx the channel handler context for buffer allocations.
     * @return the built protocol request.
     */
    private BinaryMemcacheRequest handleReplaceRequest(final ReplaceRequest request, final ChannelHandlerContext ctx) {
        ByteBuf extras = ctx.alloc().buffer(8);
        extras.writeInt(request.flags());
        extras.writeInt(request.expiration());

        FullBinaryMemcacheRequest msg = new DefaultFullBinaryMemcacheRequest(request.key(), extras, request.content());

        msg.setOpcode(BinaryMemcacheOpcodes.REPLACE);
        msg.setCAS(request.cas());
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength((short) request.key().length() + request.content().readableBytes() + extras.readableBytes());
        msg.setReserved(request.partition());
        msg.setExtrasLength((byte) extras.readableBytes());
        return msg;
    }

    /**
     * Creates the actual protocol level request for an incoming insert request.
     *
     * @param request the incoming insert request.
     * @param ctx the channel handler context for buffer allocations.
     * @return the built protocol request.
     */
    private BinaryMemcacheRequest handleInsertRequest(final InsertRequest request, final ChannelHandlerContext ctx) {
        ByteBuf extras = ctx.alloc().buffer(8);
        extras.writeInt(request.flags());
        extras.writeInt(request.expiration());

        FullBinaryMemcacheRequest msg = new DefaultFullBinaryMemcacheRequest(request.key(), extras, request.content());

        msg.setOpcode(BinaryMemcacheOpcodes.ADD);
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength((short) request.key().length() + request.content().readableBytes() + extras.readableBytes());
        msg.setReserved(request.partition());
        msg.setExtrasLength((byte) extras.readableBytes());
        return msg;
    }

    private BinaryMemcacheRequest handleRemoveRequest(final RemoveRequest request) {
        BinaryMemcacheRequest msg = new DefaultBinaryMemcacheRequest(request.key());

        msg.setOpcode(BinaryMemcacheOpcodes.DELETE);
        msg.setCAS(request.cas());
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength((short) request.key().length());
        msg.setReserved(request.partition());
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