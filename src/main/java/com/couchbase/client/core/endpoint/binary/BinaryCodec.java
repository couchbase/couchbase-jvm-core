
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

import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.binary.BinaryRequest;
import com.couchbase.client.core.message.binary.CounterRequest;
import com.couchbase.client.core.message.binary.CounterResponse;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigResponse;
import com.couchbase.client.core.message.binary.GetRequest;
import com.couchbase.client.core.message.binary.GetResponse;
import com.couchbase.client.core.message.binary.InsertRequest;
import com.couchbase.client.core.message.binary.InsertResponse;
import com.couchbase.client.core.message.binary.ObserveRequest;
import com.couchbase.client.core.message.binary.ObserveResponse;
import com.couchbase.client.core.message.binary.RemoveRequest;
import com.couchbase.client.core.message.binary.RemoveResponse;
import com.couchbase.client.core.message.binary.ReplaceRequest;
import com.couchbase.client.core.message.binary.ReplaceResponse;
import com.couchbase.client.core.message.binary.ReplicaGetRequest;
import com.couchbase.client.core.message.binary.TouchRequest;
import com.couchbase.client.core.message.binary.TouchResponse;
import com.couchbase.client.core.message.binary.UnlockRequest;
import com.couchbase.client.core.message.binary.UnlockResponse;
import com.couchbase.client.core.message.binary.UpsertRequest;
import com.couchbase.client.core.message.binary.UpsertResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.compression.Snappy;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.util.CharsetUtil;

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

    private final Snappy snappy = new Snappy();

    /**
     * The bucket used.
     */
    private final String bucket;

    /**
     * Default the datatypes to non-support.
     */
    private SupportedDatatypes datatypes = new SupportedDatatypes(false, false);

    private final Environment env;

    /**
     * Creates a new {@link BinaryCodec} with the default dequeue.
     */
    public BinaryCodec(String bucket, Environment env) {
        this(bucket, env, new ArrayDeque<BinaryRequest>());
    }

    /**
     * Creates a new {@link BinaryCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public BinaryCodec(String bucket, Environment env, final Queue<BinaryRequest> queue) {
        this.queue = queue;
        this.env = env;
        this.bucket = bucket;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final BinaryRequest msg, final List<Object> out)
        throws Exception {
        BinaryMemcacheRequest request;
        if (msg instanceof GetBucketConfigRequest) {
            request = handleGetBucketConfigRequest();
        } else if (msg instanceof GetRequest) {
            request = handleGetRequest((GetRequest) msg, ctx);
        } else if (msg instanceof ReplicaGetRequest) {
            request = handleReplicaGetRequest((ReplicaGetRequest) msg);
        } else if (msg instanceof UpsertRequest) {
            request = handleUpsertRequest((UpsertRequest) msg, ctx);
        } else if (msg instanceof InsertRequest) {
            request = handleInsertRequest((InsertRequest) msg, ctx);
        } else if (msg instanceof ReplaceRequest) {
            request = handleReplaceRequest((ReplaceRequest) msg, ctx);
        } else if (msg instanceof RemoveRequest) {
            request = handleRemoveRequest((RemoveRequest) msg);
        } else if (msg instanceof CounterRequest) {
            request = handleCounterRequest((CounterRequest) msg, ctx);
        } else if (msg instanceof TouchRequest) {
            request = handleTouchRequest((TouchRequest) msg, ctx);
        } else if (msg instanceof UnlockRequest) {
            request = handleUnlockRequest((UnlockRequest) msg, ctx);
        } else if (msg instanceof ObserveRequest) {
            request = handleObserveRequest((ObserveRequest) msg, ctx);
        } else {
            throw new IllegalArgumentException("Unknown Messgae to encode: " + msg);
        }

        out.add(request);
        queue.offer(msg);
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final FullBinaryMemcacheResponse msg,
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
        } else if (current instanceof GetRequest || current instanceof ReplicaGetRequest) {
            ByteBuf content = msg.content().copy();
            if (msg.getDataType() == 2 || msg.getDataType() == 3) {
                ByteBuf compressed = ctx.alloc().buffer();
                snappy.decode(content, compressed);
                content.release();
                content = compressed;
            }
            int flags = 0;
            if (msg.getExtrasLength() > 0)
            {
                final ByteBuf extrasReleased = msg.getExtras();
                final ByteBuf extras = ctx.alloc().buffer(msg.getExtrasLength());
                extras.writeBytes(extrasReleased, extrasReleased.readerIndex(), extrasReleased.readableBytes());
                flags = extras.getInt(0);
                extras.release();
            }
            in.add(new GetResponse(status, cas, flags, bucket, content, currentRequest));
        } else if (current instanceof InsertRequest) {
            in.add(new InsertResponse(status, cas, bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof UpsertRequest) {
            in.add(new UpsertResponse(status, cas, bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof ReplaceRequest) {
            in.add(new ReplaceResponse(status, cas, bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof RemoveRequest) {
            in.add(new RemoveResponse(convertStatus(msg.getStatus()), bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof CounterRequest) {
            in.add(new CounterResponse(status, bucket, msg.content().getLong(0), msg.getCAS(), currentRequest));
        } else if (current instanceof UnlockRequest) {
            in.add(new UnlockResponse(status, bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof TouchRequest) {
            in.add(new TouchResponse(status, bucket, msg.content().copy(), currentRequest));
        } else if (current instanceof ObserveRequest) {
            ByteBuf content = msg.content();
            short keyLength = content.getShort(2);
            byte observed = content.getByte(keyLength + 4);
            in.add(new ObserveResponse(status, observed, ((ObserveRequest) current).master(), bucket, msg.content().copy(), currentRequest));
        } else {
            throw new IllegalStateException("Got a response message for a request that was not sent." + msg);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SupportedDatatypes) {
            datatypes = (SupportedDatatypes) evt;
            return;
        }
        super.userEventTriggered(ctx, evt);
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
    private BinaryMemcacheRequest handleGetRequest(final GetRequest request, final ChannelHandlerContext ctx) {
        int keyLength = request.key().length();
        BinaryMemcacheRequest msg = new DefaultBinaryMemcacheRequest(request.key());

        byte opcode;
        ByteBuf extras;
        if (request.lock()) {
            opcode = (byte) 0x94;
            extras = ctx.alloc().buffer().writeInt(request.expiry());
        } else if (request.touch()) {
            opcode = 0x1d;
            extras = ctx.alloc().buffer().writeInt(request.expiry());
        } else {
            opcode = BinaryMemcacheOpcodes.GET;
            extras = Unpooled.EMPTY_BUFFER;
        }

        msg.setOpcode(opcode);
        msg.setKeyLength((short) keyLength);
        msg.setTotalBodyLength((short) keyLength + extras.readableBytes());
        msg.setReserved(request.partition());
        msg.setExtras(extras);
        msg.setExtrasLength((byte) extras.readableBytes());
        return msg;
    }

    private BinaryMemcacheRequest handleReplicaGetRequest(final ReplicaGetRequest request) {
        int length = request.key().length();
        BinaryMemcacheRequest msg = new DefaultBinaryMemcacheRequest(request.key());
        msg.setOpcode((byte) 0x83);
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

        ByteBuf content = request.content();
        boolean compress = datatypes.compression() && env.compressionEnabled()
            && content.readableBytes() >= env.compressionLowerLimit();
        if (compress) {
            ByteBuf compressed = ctx.alloc().buffer();
            snappy.encode(content, compressed, content.readableBytes());
            content.release();
            content = compressed;
        }

        FullBinaryMemcacheRequest msg = new DefaultFullBinaryMemcacheRequest(request.key(), extras, content);

        msg.setOpcode(BinaryMemcacheOpcodes.SET);
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength((short) request.key().length() + content.readableBytes() + extras.readableBytes());
        msg.setReserved(request.partition());
        msg.setExtrasLength((byte) extras.readableBytes());
        if (datatypes.json() && request.isJson()) {
            if (compress) {
                msg.setDataType((byte) 0x03);
            } else {
                msg.setDataType((byte) 0x01);
            }
        } else if (compress) {
            msg.setDataType((byte) 0x02);
        }
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

        ByteBuf content = request.content();
        boolean compress = datatypes.compression() && env.compressionEnabled()
            && content.readableBytes() >= env.compressionLowerLimit();
        if (compress) {
            ByteBuf compressed = ctx.alloc().buffer();
            snappy.encode(content, compressed, content.readableBytes());
            content.release();
            content = compressed;
        }

        FullBinaryMemcacheRequest msg = new DefaultFullBinaryMemcacheRequest(request.key(), extras, content);

        msg.setOpcode(BinaryMemcacheOpcodes.REPLACE);
        msg.setCAS(request.cas());
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength((short) request.key().length() + content.readableBytes() + extras.readableBytes());
        msg.setReserved(request.partition());
        msg.setExtrasLength((byte) extras.readableBytes());
        if (datatypes.json() && request.isJson()) {
            if (compress) {
                msg.setDataType((byte) 0x03);
            } else {
                msg.setDataType((byte) 0x01);
            }
        } else if (compress) {
            msg.setDataType((byte) 0x02);
        }
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

        ByteBuf content = request.content();
        boolean compress = datatypes.compression() && env.compressionEnabled()
            && content.readableBytes() >= env.compressionLowerLimit();
        if (compress) {
            ByteBuf compressed = ctx.alloc().buffer();
            snappy.encode(content, compressed, content.readableBytes());
            content.release();
            content = compressed;
        }

        FullBinaryMemcacheRequest msg = new DefaultFullBinaryMemcacheRequest(request.key(), extras, content);

        msg.setOpcode(BinaryMemcacheOpcodes.ADD);
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength((short) request.key().length() + content.readableBytes() + extras.readableBytes());
        msg.setReserved(request.partition());
        msg.setExtrasLength((byte) extras.readableBytes());
        if (datatypes.json() && request.isJson()) {
            if (compress) {
                msg.setDataType((byte) 0x03);
            } else {
                msg.setDataType((byte) 0x01);
            }
        } else if (compress) {
            msg.setDataType((byte) 0x02);
        }
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

    private BinaryMemcacheRequest handleCounterRequest(final CounterRequest request, final ChannelHandlerContext ctx) {
        ByteBuf extras = ctx.alloc().buffer();
        extras.writeLong(Math.abs(request.delta()));
        extras.writeLong(request.initial());
        extras.writeInt(request.expiry());

        BinaryMemcacheRequest msg = new DefaultBinaryMemcacheRequest(request.key(), extras);
        msg.setOpcode(request.delta() < 0 ? (byte) 0x06 : (byte) 0x05);
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength((short) request.key().length() + extras.readableBytes());
        msg.setExtrasLength((byte) extras.readableBytes());
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

    private BinaryMemcacheRequest handleUnlockRequest(final UnlockRequest request, final ChannelHandlerContext ctx) {
        BinaryMemcacheRequest msg = new DefaultBinaryMemcacheRequest(request.key());
        msg.setOpcode((byte) 0x95);
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength(request.key().length());
        msg.setCAS(request.cas());
        msg.setReserved(request.partition());
        return msg;
    }

    private BinaryMemcacheRequest handleTouchRequest(final TouchRequest request, final ChannelHandlerContext ctx) {
        ByteBuf extras = ctx.alloc().buffer();
        extras.writeInt(request.expiry());

        BinaryMemcacheRequest msg = new DefaultBinaryMemcacheRequest(request.key());
        msg.setExtras(extras);
        msg.setOpcode((byte) 0x1c);
        msg.setKeyLength((short) request.key().length());
        msg.setTotalBodyLength(request.key().length() + extras.readableBytes());
        msg.setExtrasLength((byte) extras.readableBytes());
        msg.setReserved(request.partition());
        return msg;
    }

    private BinaryMemcacheRequest handleObserveRequest(final ObserveRequest request, final ChannelHandlerContext ctx) {
        ByteBuf content = ctx.alloc().buffer();
        content.writeShort(request.partition());
        content.writeShort(request.key().length());
        content.writeBytes(request.key().getBytes(CharsetUtil.UTF_8));

        BinaryMemcacheRequest msg = new DefaultFullBinaryMemcacheRequest("", Unpooled.EMPTY_BUFFER, content);

        msg.setOpcode((byte) 0x92);
        msg.setTotalBodyLength(content.readableBytes());
        return msg;
    }

}