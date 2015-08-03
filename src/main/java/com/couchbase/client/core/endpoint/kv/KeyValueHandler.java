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
package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.ServerFeatures;
import com.couchbase.client.core.endpoint.ServerFeaturesEvent;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.AbstractKeyValueRequest;
import com.couchbase.client.core.message.kv.AbstractKeyValueResponse;
import com.couchbase.client.core.message.kv.AppendRequest;
import com.couchbase.client.core.message.kv.AppendResponse;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.BinaryStoreRequest;
import com.couchbase.client.core.message.kv.CounterRequest;
import com.couchbase.client.core.message.kv.CounterResponse;
import com.couchbase.client.core.message.kv.FailoverObserveSeqnoResponse;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.NoFailoverObserveSeqnoResponse;
import com.couchbase.client.core.message.kv.ObserveRequest;
import com.couchbase.client.core.message.kv.ObserveResponse;
import com.couchbase.client.core.message.kv.ObserveSeqnoRequest;
import com.couchbase.client.core.message.kv.PrependRequest;
import com.couchbase.client.core.message.kv.PrependResponse;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.kv.ReplaceRequest;
import com.couchbase.client.core.message.kv.ReplaceResponse;
import com.couchbase.client.core.message.kv.ReplicaGetRequest;
import com.couchbase.client.core.message.kv.TouchRequest;
import com.couchbase.client.core.message.kv.TouchResponse;
import com.couchbase.client.core.message.kv.UnlockRequest;
import com.couchbase.client.core.message.kv.UnlockResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import com.lmax.disruptor.EventSink;
import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;

import java.util.Queue;

/**
 * The {@link KeyValueHandler} is responsible for encoding {@link BinaryRequest}s into lower level
 * {@link BinaryMemcacheRequest}s as well as decoding {@link FullBinaryMemcacheResponse}s into
 * {@link CouchbaseResponse}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class KeyValueHandler
    extends AbstractGenericHandler<FullBinaryMemcacheResponse, BinaryMemcacheRequest, BinaryRequest> {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(KeyValueHandler.class);

    //Memcached OPCODES are defined on 1 byte. Some cbserver specific commands are casted
    // to byte to conform to this limitation and exploit the negative range.
    public static final byte OP_GET_BUCKET_CONFIG = (byte) 0xb5;
    public static final byte OP_GET = BinaryMemcacheOpcodes.GET;
    public static final byte OP_GET_AND_LOCK = (byte) 0x94;
    public static final byte OP_GET_AND_TOUCH = BinaryMemcacheOpcodes.GAT;
    public static final byte OP_GET_REPLICA = (byte) 0x83;
    public static final byte OP_INSERT = BinaryMemcacheOpcodes.ADD;
    public static final byte OP_UPSERT = BinaryMemcacheOpcodes.SET;
    public static final byte OP_REPLACE = BinaryMemcacheOpcodes.REPLACE;
    public static final byte OP_REMOVE = BinaryMemcacheOpcodes.DELETE;
    public static final byte OP_COUNTER_INCR = BinaryMemcacheOpcodes.INCREMENT;
    public static final byte OP_COUNTER_DECR = BinaryMemcacheOpcodes.DECREMENT;
    public static final byte OP_UNLOCK = (byte) 0x95;
    public static final byte OP_OBSERVE = (byte) 0x92;
    public static final byte OP_OBSERVE_SEQ = (byte) 0x91;
    public static final byte OP_TOUCH = BinaryMemcacheOpcodes.TOUCH;
    public static final byte OP_APPEND = BinaryMemcacheOpcodes.APPEND;
    public static final byte OP_PREPEND = BinaryMemcacheOpcodes.PREPEND;
    public static final byte OP_NOOP = BinaryMemcacheOpcodes.NOOP;

    boolean seqOnMutation = false;


    /**
     * Creates a new {@link KeyValueHandler} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public KeyValueHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, boolean isTransient) {
        super(endpoint, responseBuffer, isTransient);
    }

    /**
     * Creates a new {@link KeyValueHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    KeyValueHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, Queue<BinaryRequest> queue, boolean isTransient) {
        super(endpoint, responseBuffer, queue, isTransient);
    }

    @Override
    protected BinaryMemcacheRequest encodeRequest(final ChannelHandlerContext ctx, final BinaryRequest msg)
        throws Exception {
        BinaryMemcacheRequest request;

        if (msg instanceof GetRequest) {
            request = handleGetRequest(ctx, (GetRequest) msg);
        } else if (msg instanceof BinaryStoreRequest) {
            request = handleStoreRequest(ctx, (BinaryStoreRequest) msg);
        } else if (msg instanceof ReplicaGetRequest) {
            request = handleReplicaGetRequest((ReplicaGetRequest) msg);
        } else if (msg instanceof RemoveRequest) {
            request = handleRemoveRequest((RemoveRequest) msg);
        } else if (msg instanceof CounterRequest) {
            request = handleCounterRequest(ctx, (CounterRequest) msg);
        } else if (msg instanceof TouchRequest) {
            request = handleTouchRequest(ctx, (TouchRequest) msg);
        } else if (msg instanceof UnlockRequest) {
            request = handleUnlockRequest((UnlockRequest) msg);
        } else if (msg instanceof ObserveRequest) {
            request = handleObserveRequest(ctx, (ObserveRequest) msg);
        } else if (msg instanceof ObserveSeqnoRequest) {
            request = handleObserveSeqnoRequest(ctx, (ObserveSeqnoRequest) msg);
        } else if (msg instanceof GetBucketConfigRequest) {
            request = handleGetBucketConfigRequest();
        } else if (msg instanceof AppendRequest) {
            request = handleAppendRequest((AppendRequest) msg);
        } else if (msg instanceof PrependRequest) {
            request = handlePrependRequest((PrependRequest) msg);
        } else if (msg instanceof KeepAliveRequest) {
            request = handleKeepAliveRequest((KeepAliveRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown incoming BinaryRequest type "
                + msg.getClass());
        }

        if (msg.partition() >= 0) {
            request.setReserved(msg.partition());
        }

        request.setOpaque(msg.opaque());

        // Retain just the content, since a response could be "Not my Vbucket".
        // The response handler checks the status and then releases if needed.
        // Observe has content, but not external, so it should not be retained.
        if (!(msg instanceof ObserveRequest)
            && !(msg instanceof ObserveSeqnoRequest)
            && (request instanceof FullBinaryMemcacheRequest)) {
            ((FullBinaryMemcacheRequest) request).content().retain();
        }

        return request;
    }

    /**
     * Encodes a {@link GetRequest} into its lower level representation.
     *
     * Depending on the flags set on the {@link GetRequest}, the appropriate opcode gets chosen. Currently, a regular
     * get, as well as "get and touch" and "get and lock" are supported. Latter variants have server-side side-effects
     * but do not differ in response behavior.
     *
     * @param ctx the {@link ChannelHandlerContext} to use for allocation and others.
     * @param msg the incoming message.
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleGetRequest(final ChannelHandlerContext ctx, final GetRequest msg) {
        byte opcode;
        ByteBuf extras;
        if (msg.lock()) {
            opcode = OP_GET_AND_LOCK;
            extras = ctx.alloc().buffer().writeInt(msg.expiry());
        } else if (msg.touch()) {
            opcode = OP_GET_AND_TOUCH;
            extras = ctx.alloc().buffer().writeInt(msg.expiry());
        } else {
            opcode = OP_GET;
            extras = Unpooled.EMPTY_BUFFER;
        }

        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        byte extrasLength = (byte) extras.readableBytes();
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key);
        request
            .setOpcode(opcode)
            .setKeyLength(keyLength)
            .setExtras(extras)
            .setExtrasLength(extrasLength)
            .setTotalBodyLength(keyLength + extrasLength);
        return request;
    }

    /**
     * Encodes a {@link GetBucketConfigRequest} into its lower level representation.
     *
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleGetBucketConfigRequest() {
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest();
        request.setOpcode(OP_GET_BUCKET_CONFIG);
        return request;
    }

    /**
     * Encodes a {@link ReplicaGetRequest} into its lower level representation.
     *
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleReplicaGetRequest(final ReplicaGetRequest msg) {
        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key);

        request.setOpcode(OP_GET_REPLICA)
            .setKeyLength(keyLength)
            .setTotalBodyLength(keyLength);
        return request;
    }

    /**
     * Encodes a {@link BinaryStoreRequest} into its lower level representation.
     *
     * There are three types of store operations that need to be considered: insert, upsert and replace, which
     * directly translate to the add, set and replace binary memcached opcodes. By convention, only the replace
     * command supports setting a CAS value, even if the others theoretically would do as well (but do not provide
     * benefit in such cases).
     *
     * Currently, the content is loaded and sent down in one batch, streaming for requests is not supported.
     *
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleStoreRequest(final ChannelHandlerContext ctx,
        final BinaryStoreRequest msg) {
        ByteBuf extras = ctx.alloc().buffer(8);
        extras.writeInt(msg.flags());
        extras.writeInt(msg.expiration());

        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        byte extrasLength = (byte) extras.readableBytes();
        FullBinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, extras, msg.content());

        if (msg instanceof InsertRequest) {
            request.setOpcode(OP_INSERT);
        } else if (msg instanceof UpsertRequest) {
            request.setOpcode(OP_UPSERT);
        } else if (msg instanceof ReplaceRequest) {
            request.setOpcode(OP_REPLACE);
            request.setCAS(((ReplaceRequest) msg).cas());
        } else {
            throw new IllegalArgumentException("Unknown incoming BinaryStoreRequest type "
                + msg.getClass());
        }

        request.setKeyLength(keyLength);
        request.setTotalBodyLength(keyLength + msg.content().readableBytes() + extrasLength);
        request.setExtrasLength(extrasLength);
        return request;
    }

    /**
     * Encodes a {@link RemoveRequest} into its lower level representation.
     *
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleRemoveRequest(final RemoveRequest msg) {
        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key);

        request.setOpcode(OP_REMOVE);
        request.setCAS(msg.cas());
        request.setKeyLength(keyLength);
        request.setTotalBodyLength(keyLength);
        return request;
    }

    /**
     * Encodes a {@link CounterRequest} into its lower level representation.
     *
     * Depending on if the {@link CounterRequest#delta} is positive or negative, either the incr or decr memcached
     * commands are utilized. The value is converted to its absolute variant to conform with the protocol.
     *
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleCounterRequest(final ChannelHandlerContext ctx,
        final CounterRequest msg) {
        ByteBuf extras = ctx.alloc().buffer();
        extras.writeLong(Math.abs(msg.delta()));
        extras.writeLong(msg.initial());
        extras.writeInt(msg.expiry());

        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        byte extrasLength = (byte) extras.readableBytes();
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key, extras);
        request.setOpcode(msg.delta() < 0 ? OP_COUNTER_DECR : OP_COUNTER_INCR);
        request.setKeyLength(keyLength);
        request.setTotalBodyLength(keyLength + extrasLength);
        request.setExtrasLength(extrasLength);
        return request;
    }

    /**
     * Encodes a {@link UnlockRequest} into its lower level representation.
     *
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleUnlockRequest(final UnlockRequest msg) {
        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key);
        request.setOpcode(OP_UNLOCK);
        request.setKeyLength(keyLength);
        request.setTotalBodyLength(keyLength);
        request.setCAS(msg.cas());
        return request;
    }

    /**
     * Encodes a {@link TouchRequest} into its lower level representation.
     *
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleTouchRequest(final ChannelHandlerContext ctx, final TouchRequest msg) {
        ByteBuf extras = ctx.alloc().buffer();
        extras.writeInt(msg.expiry());

        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        byte extrasLength = (byte) extras.readableBytes();
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key);
        request.setExtras(extras);
        request.setOpcode(OP_TOUCH);
        request.setKeyLength(keyLength);
        request.setTotalBodyLength(keyLength + extrasLength);
        request.setExtrasLength(extrasLength);
        return request;
    }

    /**
     * Encodes a {@link ObserveRequest} into its lower level representation.
     *
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleObserveRequest(final ChannelHandlerContext ctx,
        final ObserveRequest msg) {
        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        ByteBuf content = ctx.alloc().buffer();
        content.writeShort(msg.partition());
        content.writeShort(keyLength);
        content.writeBytes(key.getBytes(CHARSET));

        BinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest("", Unpooled.EMPTY_BUFFER, content);
        request.setOpcode(OP_OBSERVE);
        request.setTotalBodyLength(content.readableBytes());
        return request;
    }

    private static BinaryMemcacheRequest handleObserveSeqnoRequest(final ChannelHandlerContext ctx,
        final ObserveSeqnoRequest msg) {
        ByteBuf content = ctx.alloc().buffer();
        content.writeLong(msg.vbucketUUID());

        BinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest("", Unpooled.EMPTY_BUFFER, content);
        request.setOpcode(OP_OBSERVE_SEQ);
        request.setTotalBodyLength(content.readableBytes());
        return request;
    }

    private static BinaryMemcacheRequest handleAppendRequest(final AppendRequest msg) {
        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        BinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, Unpooled.EMPTY_BUFFER, msg.content());

        request.setOpcode(OP_APPEND);
        request.setKeyLength(keyLength);
        request.setCAS(msg.cas());
        request.setTotalBodyLength(keyLength + msg.content().readableBytes());
        return request;
    }

    private static BinaryMemcacheRequest handlePrependRequest(final PrependRequest msg) {
        String key = msg.key();
        short keyLength = (short) key.getBytes(CharsetUtil.UTF_8).length;
        BinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, Unpooled.EMPTY_BUFFER, msg.content());

        request.setOpcode(OP_PREPEND);
        request.setKeyLength(keyLength);
        request.setCAS(msg.cas());
        request.setTotalBodyLength(keyLength + msg.content().readableBytes());
        return request;
    }

    /**
     * Encodes a {@link KeepAliveRequest} request into a NOOP operation.
     *
     * @param msg the {@link KeepAliveRequest} triggering the NOOP.
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleKeepAliveRequest(KeepAliveRequest msg) {
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest();
        request
                .setOpcode(OP_NOOP)
                .setKeyLength((short) 0)
                .setExtras(Unpooled.EMPTY_BUFFER)
                .setExtrasLength((byte) 0)
                .setTotalBodyLength(0);
        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(final ChannelHandlerContext ctx, final FullBinaryMemcacheResponse msg)
        throws Exception {
        BinaryRequest request = currentRequest();

        if (request.opaque() != msg.getOpaque()) {
            throw new IllegalStateException("Opaque values for " + msg.getClass() + " do not match.");
        }

        ResponseStatus status = ResponseStatusConverter.fromBinary(msg.getStatus());
        if (!status.equals(ResponseStatus.RETRY)) {
           maybeFreeContent(request);
        }

        msg.content().retain();
        CouchbaseResponse response = handleCommonResponseMessages(request, msg, ctx, status, seqOnMutation);

        if (response == null) {
            response = handleOtherResponseMessages(request, msg, status, seqOnMutation);
        }

        if (response == null) {
            throw new IllegalStateException("Unhandled request/response pair: " + request.getClass() + "/"
                    + msg.getClass());
        }

        finishedDecoding();
        return response;
    }

    /**
     * Helper method to decode all common response messages.
     *
     * @param request the current request.
     * @param msg the current response message.
     * @param ctx the handler context.
     * @param status the response status code.
     * @return the decoded response or null if none did match.
     */
    private static CouchbaseResponse handleCommonResponseMessages(BinaryRequest request, FullBinaryMemcacheResponse msg,
         ChannelHandlerContext ctx, ResponseStatus status, boolean seqOnMutation) {
        CouchbaseResponse response = null;
        ByteBuf content = msg.content();
        long cas = msg.getCAS();
        short statusCode = msg.getStatus();
        String bucket = request.bucket();

        if (request instanceof GetRequest || request instanceof ReplicaGetRequest) {
            int flags = extractFlagsFromGetResponse(ctx, msg.getExtras(), msg.getExtrasLength());
            response = new GetResponse(status, statusCode, cas, flags, bucket, content, request);
        } else if (request instanceof GetBucketConfigRequest) {
            response = new GetBucketConfigResponse(status, statusCode, bucket, content,
                    ((GetBucketConfigRequest) request).hostname());
        } else if (request instanceof InsertRequest) {
            MutationToken descr = extractToken(seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new InsertResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof UpsertRequest) {
            MutationToken descr = extractToken(seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new UpsertResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof ReplaceRequest) {
            MutationToken descr = extractToken(seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new ReplaceResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof RemoveRequest) {
            MutationToken descr = extractToken(seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new RemoveResponse(status, statusCode, cas, bucket, content, descr, request);
        }

        return response;
    }

    private static MutationToken extractToken(boolean seqOnMutation, boolean success, ByteBuf extras, long vbid) {
        if (success && seqOnMutation) {
            return new MutationToken(vbid, extras.readLong(), extras.readLong());
        }
        return null;
    }

    /**
     * Helper method to decode all other response messages.
     *
     * @param request the current request.
     * @param msg the current response message.
     * @param status the response status code.
     * @return the decoded response or null if none did match.
     */
    private static CouchbaseResponse handleOtherResponseMessages(BinaryRequest request, FullBinaryMemcacheResponse msg,
        ResponseStatus status, boolean seqOnMutation) {
        CouchbaseResponse response = null;
        ByteBuf content = msg.content();
        long cas = msg.getCAS();
        short statusCode = msg.getStatus();
        String bucket = request.bucket();

        if (request instanceof UnlockRequest) {
            response = new UnlockResponse(status, statusCode, bucket, content, request);
        } else if (request instanceof TouchRequest) {
            response = new TouchResponse(status, statusCode, bucket, content, request);
        } else if (request instanceof AppendRequest) {
            MutationToken descr = extractToken(seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new AppendResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof PrependRequest) {
            MutationToken descr = extractToken(seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new PrependResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof KeepAliveRequest) {
            releaseContent(content);
            response = new KeepAliveResponse(status, statusCode, request);
        } else if (request instanceof CounterRequest) {
            long value = status.isSuccess() ? content.readLong() : 0;
            releaseContent(content);

            MutationToken descr = extractToken(seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new CounterResponse(status, statusCode, bucket, value, cas, descr, request);
        } else if (request instanceof ObserveRequest) {
            byte observed = ObserveResponse.ObserveStatus.UNKNOWN.value();
            long observedCas = 0;
            if (status.isSuccess()) {
                short keyLength = content.getShort(2);
                observed = content.getByte(keyLength + 4);
                observedCas = content.getLong(keyLength + 5);
            }
            releaseContent(content);
            response = new ObserveResponse(status, statusCode, observed, ((ObserveRequest) request).master(),
                    observedCas, bucket, request);
        } else if (request instanceof ObserveSeqnoRequest) {
            if (status.isSuccess()) {
                byte format = content.readByte();
                switch(format) {
                    case 0:
                        response = new NoFailoverObserveSeqnoResponse(
                            ((ObserveSeqnoRequest) request).master(),
                            content.readShort(),
                            content.readLong(),
                            content.readLong(),
                            content.readLong(),
                            status,
                            statusCode,
                            bucket,
                            request
                        );
                        break;
                    case 1:
                        response = new FailoverObserveSeqnoResponse(
                            ((ObserveSeqnoRequest) request).master(),
                            content.readShort(),
                            content.readLong(),
                            content.readLong(),
                            content.readLong(),
                            content.readLong(),
                            content.readLong(),
                            status,
                            statusCode,
                            bucket,
                            request
                        );
                        break;
                    default:
                        throw new IllegalStateException("Unknown format for observe-seq: " + format);
                }
            } else {
                response = new NoFailoverObserveSeqnoResponse(((ObserveSeqnoRequest) request).master(), (short) 0, 0,
                    0, 0, status, statusCode, bucket, request);
            }
            releaseContent(content);
        }

        return response;
    }

    /**
     * Helper method to release content from external resources.
     *
     * This method should be called when it is clear that the request is not tried again.
     *
     * @param request the request where to free the content.
     */
    private static void maybeFreeContent(BinaryRequest request) {
        ByteBuf content = null;
        if (request instanceof BinaryStoreRequest) {
            content = ((BinaryStoreRequest) request).content();
        } else if (request instanceof AppendRequest) {
            content = ((AppendRequest) request).content();
        } else if (request instanceof PrependRequest) {
            content = ((PrependRequest) request).content();
        }
        releaseContent(content);
    }

    /**
     * Helper method to safely release the content.
     *
     * @param content the content to safely release if needed.
     */
    private static void releaseContent(ByteBuf content) {
        if (content != null && content.refCnt() > 0) {
            content.release();
        }
    }

    /**
     * Helper method to extract the flags from the extras buffer.
     *
     * @param ctx the handler context.
     * @param extrasReleased the extras of the msg.
     * @param extrasLength the extras length.
     * @return the extracted flags.
     */
    private static int extractFlagsFromGetResponse(ChannelHandlerContext ctx, ByteBuf extrasReleased,
        int extrasLength) {
        int flags = 0;
        if (extrasLength > 0) {
            final ByteBuf extras = ctx.alloc().buffer(extrasLength);
            extras.writeBytes(extrasReleased, extrasReleased.readerIndex(), extrasReleased.readableBytes());
            flags = extras.getInt(0);
            extras.release();
        }
        return flags;
    }

    /**
     * Releasing the content of requests that are to be cancelled.
     *
     * @param request the request to side effect on.
     */
    @Override
    protected void sideEffectRequestToCancel(final BinaryRequest request) {
        super.sideEffectRequestToCancel(request);

        if (request instanceof BinaryStoreRequest) {
            ((BinaryStoreRequest) request).content().release();
        } else if (request instanceof AppendRequest) {
            ((AppendRequest) request).content().release();
        } else if (request instanceof PrependRequest) {
            ((PrependRequest) request).content().release();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            LOGGER.debug(logIdent(ctx, endpoint()) + "Identified Idle State, signalling config reload.");
            endpoint().signalConfigReload();
        }

        if (evt instanceof ServerFeaturesEvent) {
            seqOnMutation = env().mutationTokensEnabled() &&
                ((ServerFeaturesEvent) evt).supportedFeatures().contains(ServerFeatures.MUTATION_SEQNO);
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected CouchbaseRequest createKeepAliveRequest() {
        return new KeepAliveRequest();
    }

    protected static class KeepAliveRequest extends AbstractKeyValueRequest {

        protected KeepAliveRequest() {
            super(null, null, null);
            partition((short) 0);
        }
    }

    protected static class KeepAliveResponse extends AbstractKeyValueResponse {

        public KeepAliveResponse(ResponseStatus status, short serverStatusCode, CouchbaseRequest request) {
            super(status, serverStatusCode, null, null, request);
        }
    }

    @Override
    protected ServiceType serviceType() {
        return ServiceType.BINARY;
    }
}
