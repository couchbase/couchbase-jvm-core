/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.ServerFeatures;
import com.couchbase.client.core.endpoint.ServerFeaturesEvent;
import com.couchbase.client.core.endpoint.util.Snappy;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.DiagnosticRequest;
import com.couchbase.client.core.message.KeepAlive;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.ResponseStatusDetails;
import com.couchbase.client.core.message.kv.AbstractKeyValueRequest;
import com.couchbase.client.core.message.kv.AbstractKeyValueResponse;
import com.couchbase.client.core.message.kv.AppendRequest;
import com.couchbase.client.core.message.kv.AppendResponse;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.BinaryStoreRequest;
import com.couchbase.client.core.message.kv.CounterRequest;
import com.couchbase.client.core.message.kv.CounterResponse;
import com.couchbase.client.core.message.kv.FailoverObserveSeqnoResponse;
import com.couchbase.client.core.message.kv.GetAllMutationTokensRequest;
import com.couchbase.client.core.message.kv.GetAllMutationTokensResponse;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.InsertResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.NoFailoverObserveSeqnoResponse;
import com.couchbase.client.core.message.kv.NoopRequest;
import com.couchbase.client.core.message.kv.NoopResponse;
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
import com.couchbase.client.core.message.kv.StatRequest;
import com.couchbase.client.core.message.kv.StatResponse;
import com.couchbase.client.core.message.kv.TouchRequest;
import com.couchbase.client.core.message.kv.TouchResponse;
import com.couchbase.client.core.message.kv.UnlockRequest;
import com.couchbase.client.core.message.kv.UnlockResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiLookupRequest;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiMutationRequest;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMutationRequest;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocRequest;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.LookupCommand;
import com.couchbase.client.core.message.kv.subdoc.multi.MultiLookupResponse;
import com.couchbase.client.core.message.kv.subdoc.multi.MultiMutationResponse;
import com.couchbase.client.core.message.kv.subdoc.multi.MultiResult;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.core.message.kv.subdoc.multi.MutationCommand;
import com.couchbase.client.core.message.kv.subdoc.simple.SimpleSubdocResponse;
import com.couchbase.client.core.message.kv.subdoc.simple.SubExistRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubGetCountRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubGetRequest;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import com.lmax.disruptor.EventSink;
import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.IllegalReferenceCountException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.endpoint.kv.ErrorMap.ErrorAttribute.AUTH;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.ErrorAttribute.AUTO_RETRY;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.ErrorAttribute.CONN_STATE_INVALIDATED;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.ErrorAttribute.FETCH_CONFIG;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.ErrorAttribute.ITEM_LOCKED;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.ErrorAttribute.RETRY_LATER;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.ErrorAttribute.RETRY_NOW;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.ErrorAttribute.SUBDOC;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.ErrorAttribute.TEMP;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.RetryStrategy.CONSTANT;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.RetryStrategy.EXPONENTIAL;
import static com.couchbase.client.core.endpoint.kv.ErrorMap.RetryStrategy.LINEAR;

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
    public static final byte OP_STAT = BinaryMemcacheOpcodes.STAT;
    public static final byte OP_GET_ALL_MUTATION_TOKENS = (byte) 0x48;

    public static final byte OP_SUB_GET = (byte) 0xc5;
    public static final byte OP_SUB_EXIST = (byte) 0xc6;
    public static final byte OP_SUB_DICT_ADD = (byte) 0xc7;
    public static final byte OP_SUB_DICT_UPSERT = (byte) 0xc8;
    public static final byte OP_SUB_DELETE = (byte) 0xc9;
    public static final byte OP_SUB_REPLACE = (byte) 0xca;
    public static final byte OP_SUB_ARRAY_PUSH_LAST = (byte) 0xcb;
    public static final byte OP_SUB_ARRAY_PUSH_FIRST = (byte) 0xcc;
    public static final byte OP_SUB_ARRAY_INSERT = (byte) 0xcd;
    public static final byte OP_SUB_ARRAY_ADD_UNIQUE = (byte) 0xce;
    public static final byte OP_SUB_COUNTER = (byte) 0xcf;
    public static final byte OP_SUB_MULTI_LOOKUP = (byte) 0xd0;
    public static final byte OP_SUB_MULTI_MUTATION = (byte) 0xd1;
    public static final byte OP_SUB_GET_COUNT = (byte) 0xd2;

    /**
     * The bitmask for sub-document extras "command" section (third byte of the extras) that activates the
     * creation of missing intermediate nodes in the JSON path.
     */
    public static final byte SUBDOC_BITMASK_MKDIR_P = 1;

    /**
     * The bitmask for sub-document xattr/hidden section of the document
     */
    public static final byte SUBDOC_FLAG_XATTR_PATH = (byte) 0x04;

    /**
     * The bitmask for sub-document create document
     */
    public static final byte SUBDOC_DOCFLAG_MKDOC = (byte) 0x1;

    /**
     * The bitmask for sub-document insert document
     */
    public static final byte SUBDOC_DOCFLAG_INSERT = (byte) 0x2;

    /**
     * The bitmask for sub-document access deleted
     */
    public static final byte SUBDOC_DOCFLAG_ACCESS_DELETED = (byte) 0x04;

    /**
     * The datatype byte used to signal snappy.
     */
    public static final byte DATATYPE_SNAPPY = (byte) 0x02;

    boolean seqOnMutation = false;

    /**
     * If snappy is enabled (got negotiated on HELLO)
     */
    boolean snappyEnabled = false;

    /**
     * Snappy used for encoding and decoding with compression.
     */
    private final Snappy snappy = new Snappy();


    /**
     * Creates a new {@link KeyValueHandler} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public KeyValueHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, boolean isTransient,
                           final boolean pipeline) {
        super(endpoint, responseBuffer, isTransient, pipeline);
    }

    /**
     * Creates a new {@link KeyValueHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    KeyValueHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, Queue<BinaryRequest> queue, boolean isTransient, final boolean pipeline) {
        super(endpoint, responseBuffer, queue, isTransient, pipeline);
    }

    @Override
    protected BinaryMemcacheRequest encodeRequest(final ChannelHandlerContext ctx, final BinaryRequest msg)
        throws Exception {
        BinaryMemcacheRequest request = encodeCommonRequest(ctx, msg);

        if (request == null) {
            request = encodeOtherRequest(ctx, msg);
        }

        if (msg.partition() >= 0) {
            request.setReserved(msg.partition());
        }

        request.setOpaque(msg.opaque());

        try {
            // Retain just the content, since a response could be "Not my Vbucket".
            // The response handler checks the status and then releases if needed.
            // Observe has content, but not external, so it should not be retained.
            if (!(msg instanceof ObserveRequest)
                    && !(msg instanceof ObserveSeqnoRequest)
                    && (request instanceof FullBinaryMemcacheRequest)) {
                ((FullBinaryMemcacheRequest) request).content().retain();
            }
        } catch (IllegalReferenceCountException ex) {
            //release extras bytebuf if there is an exception
            if (request.getExtras() != null && request.getExtras().refCnt() > 0) {
                try {
                    request.getExtras().release();
                } catch (Exception e) {
                    //ignore
                }
            }
            throw ex;
        }

        if (snappyEnabled && isEligibleForCompression(msg)) {
            handleSnappyCompression(ctx, request);
        }

        return request;
    }

    /**
     * Helper method to check if the incoming request is actually allowed to be
     * compressed.
     *
     * Right now the only allowed compressible ops are Insert, Upsert, Replace,
     * Append and Prepend.
     *
     * @param msg the request to check against.
     * @return true if it is, false otherwise.
     */
    private boolean isEligibleForCompression(final BinaryRequest msg) {
        return msg instanceof BinaryStoreRequest
            || msg instanceof AppendRequest
            || msg instanceof PrependRequest;
    }

    /**
     * Helper method which performs snappy compression on the request path.
     *
     * Note that even if we compress and switch out the content, we are not releasing the
     * original buffer! This is happening on the decode side and we still need to keep in mind
     * that a NMVB could be returned and the original msg needs to be re-sent.
     */
    private void handleSnappyCompression(final ChannelHandlerContext ctx, final BinaryMemcacheRequest r) {
        if (!(r instanceof FullBinaryMemcacheRequest)) {
            // we only need to handle requests which send content
            return;
        }

        FullBinaryMemcacheRequest request = (FullBinaryMemcacheRequest) r;

        int uncompressedLength = request.content().readableBytes();
        ByteBuf compressedContent = ctx.alloc().buffer(uncompressedLength);
        ByteBuf uncompressedContent = request.content().slice();
        try {
            snappy.encode(uncompressedContent, compressedContent, uncompressedLength);
        } catch (Exception ex) {
            throw new RuntimeException("Could not snappy-compress value.", ex);
        } finally {
            snappy.reset();
        }

        if (compressedContent.readableBytes() >= uncompressedLength) {
            // compressed is not smaller, so just send the original
            compressedContent.release();
        } else {
            // compressed is smaller, so adapt and apply new content
            request.setDataType((byte)(request.getDataType() | DATATYPE_SNAPPY));
            request.setContent(compressedContent);
            request.setTotalBodyLength(
                request.getExtrasLength()
                    + request.getKeyLength()
                    + compressedContent.readableBytes()
            );
        }
    }

    /**
     * Helper method which performs decompression for snappy compressed values.
     */
    private void handleSnappyDecompression(final ChannelHandlerContext ctx, final FullBinaryMemcacheResponse response) {
        ByteBuf decompressed = ctx.alloc().buffer(response.content().readableBytes());
        try {
            snappy.decode(response.content(), decompressed);
        } catch (Exception ex) {
            throw new RuntimeException("Could not decode snappy-compressed value.", ex);
        } finally {
            snappy.reset();
        }

        response.content().release();
        response.setContent(decompressed);
        response.setTotalBodyLength(
            response.getExtrasLength()
                + response.getKeyLength()
                + decompressed.readableBytes()
        );
        response.setDataType((byte) (response.getDataType() & ~DATATYPE_SNAPPY));
    }


    private BinaryMemcacheRequest encodeCommonRequest(final ChannelHandlerContext ctx, final BinaryRequest msg) {
        if (msg instanceof GetRequest) {
            return handleGetRequest(ctx, (GetRequest) msg);
        } else if (msg instanceof BinaryStoreRequest) {
            return handleStoreRequest(ctx, (BinaryStoreRequest) msg);
        } else if (msg instanceof ReplicaGetRequest) {
            return handleReplicaGetRequest((ReplicaGetRequest) msg);
        } else if (msg instanceof RemoveRequest) {
            return handleRemoveRequest((RemoveRequest) msg);
        } else if (msg instanceof CounterRequest) {
            return handleCounterRequest(ctx, (CounterRequest) msg);
        } else if (msg instanceof TouchRequest) {
            return handleTouchRequest(ctx, (TouchRequest) msg);
        } else if (msg instanceof UnlockRequest) {
            return handleUnlockRequest((UnlockRequest) msg);
        }
        return null;
    }

    private BinaryMemcacheRequest encodeOtherRequest(final ChannelHandlerContext ctx, final BinaryRequest msg) {
        if (msg instanceof ObserveRequest) {
            return handleObserveRequest(ctx, (ObserveRequest) msg);
        } else if (msg instanceof ObserveSeqnoRequest) {
            return handleObserveSeqnoRequest(ctx, (ObserveSeqnoRequest) msg);
        } else if (msg instanceof GetBucketConfigRequest) {
            return handleGetBucketConfigRequest();
        } else if (msg instanceof AppendRequest) {
            return handleAppendRequest((AppendRequest) msg);
        } else if (msg instanceof PrependRequest) {
            return handlePrependRequest((PrependRequest) msg);
        } else if (msg instanceof KeepAliveRequest) {
            return handleKeepAliveRequest((KeepAliveRequest) msg);
        } else if (msg instanceof StatRequest) {
            return handleStatRequest((StatRequest) msg);
        } else if (msg instanceof GetAllMutationTokensRequest) {
            return handleGetAllMutationTokensRequest(ctx, (GetAllMutationTokensRequest) msg);
        } else if (msg instanceof BinarySubdocRequest) {
            return handleSubdocumentRequest(ctx, (BinarySubdocRequest) msg);
        } else if (msg instanceof BinarySubdocMultiLookupRequest) {
            return handleSubdocumentMultiLookupRequest(ctx, (BinarySubdocMultiLookupRequest) msg);
        } else if (msg instanceof BinarySubdocMultiMutationRequest) {
            return handleSubdocumentMultiMutationRequest(ctx, (BinarySubdocMultiMutationRequest) msg);
        } else if (msg instanceof NoopRequest) {
            return handleNoopRequest(ctx, (NoopRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown incoming BinaryRequest type " + msg.getClass());
        }
    }

    /**
     * Encodes a {@link NoopRequest} into its lower level representation.
     *
     * @param ctx the {@link ChannelHandlerContext} to use for allocation and others.
     * @param msg the incoming message.
     * @return a ready {@link BinaryMemcacheRequest}.
     */
    private static BinaryMemcacheRequest handleNoopRequest(final ChannelHandlerContext ctx, final NoopRequest msg) {
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest();
        request.setOpcode(OP_NOOP);
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

        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
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
        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
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

        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
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
        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
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

        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
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
        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
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

        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
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
        short keyLength = (short) msg.keyBytes().length;
        ByteBuf content = ctx.alloc().buffer();
        content.writeShort(msg.partition());
        content.writeShort(keyLength);
        content.writeBytes(key.getBytes(CHARSET));

        BinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(EMPTY_BYTES, Unpooled.EMPTY_BUFFER, content);
        request.setOpcode(OP_OBSERVE);
        request.setTotalBodyLength(content.readableBytes());
        return request;
    }

    private static BinaryMemcacheRequest handleObserveSeqnoRequest(final ChannelHandlerContext ctx,
        final ObserveSeqnoRequest msg) {
        ByteBuf content = ctx.alloc().buffer();
        content.writeLong(msg.vbucketUUID());

        BinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(EMPTY_BYTES, Unpooled.EMPTY_BUFFER, content);
        request.setOpcode(OP_OBSERVE_SEQ);
        request.setTotalBodyLength(content.readableBytes());
        return request;
    }

    private static BinaryMemcacheRequest handleAppendRequest(final AppendRequest msg) {
        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
        BinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, Unpooled.EMPTY_BUFFER, msg.content());

        request.setOpcode(OP_APPEND);
        request.setKeyLength(keyLength);
        request.setCAS(msg.cas());
        request.setTotalBodyLength(keyLength + msg.content().readableBytes());
        return request;
    }

    private static BinaryMemcacheRequest handlePrependRequest(final PrependRequest msg) {
        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
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

    private static BinaryMemcacheRequest handleStatRequest(StatRequest msg) {
        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key);
        request
                .setOpcode(OP_STAT)
                .setKeyLength(keyLength)
                .setTotalBodyLength(keyLength);
        return request;
    }

    private static BinaryMemcacheRequest handleGetAllMutationTokensRequest(ChannelHandlerContext ctx, GetAllMutationTokensRequest msg) {
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(EMPTY_BYTES);

        ByteBuf extras;
        switch (msg.partitionState()) {
            case ANY:
                extras = Unpooled.EMPTY_BUFFER;
                break;
            case ACTIVE:
            case REPLICA:
            case PENDING:
            case DEAD:
            default:
                extras = ctx.alloc().buffer().writeInt(msg.partitionState().value());
        }
        byte extrasLength = (byte) extras.readableBytes();

        request
                .setOpcode(OP_GET_ALL_MUTATION_TOKENS)
                .setExtras(extras)
                .setExtrasLength(extrasLength)
                .setTotalBodyLength(extrasLength);
        return request;
    }

    private static BinaryMemcacheRequest handleSubdocumentRequest(ChannelHandlerContext ctx, BinarySubdocRequest msg) {
        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;

        ByteBuf extras = ctx.alloc().buffer(3, 8); //extras can be 8 bytes if there is an expiry
        byte extrasLength = 3; //by default 2 bytes for pathLength + 1 byte for "command" flags
        extras.writeShort(msg.pathLength());

        long cas = 0L;
        if (msg instanceof BinarySubdocMutationRequest) {
            BinarySubdocMutationRequest mut = (BinarySubdocMutationRequest) msg;
            //for now only possible command flag is MKDIR_P (and it makes sense in mutations only)
            byte flags = 0;
            if (mut.createIntermediaryPath()) {
                flags |= SUBDOC_BITMASK_MKDIR_P;
            }
            if (mut.xattr()) {
                flags |= SUBDOC_FLAG_XATTR_PATH;
            }
            extras.writeByte(flags);

            if (mut.expiration() != 0L) {
                extrasLength = 7;
                extras.writeInt(mut.expiration());
            }

            byte docFlags = 0;
            if (mut.upsertDocument()) {
                docFlags |= SUBDOC_DOCFLAG_MKDOC;
            }
            if (mut.insertDocument()) {
                docFlags |= SUBDOC_DOCFLAG_INSERT;
            }
            if (docFlags != 0) {
                extrasLength++;
                extras.writeByte(docFlags);
            }

            cas = mut.cas();
        } else if (msg instanceof SubGetRequest) {
            SubGetRequest req =  (SubGetRequest)msg;
            if (req.xattr()) {
                extras.writeByte(SUBDOC_FLAG_XATTR_PATH);
            } else {
                extras.writeByte(0);
            }
            if (req.accessDeleted()) {
                extrasLength++;
                extras.writeByte(SUBDOC_DOCFLAG_ACCESS_DELETED);
            }
        } else if (msg instanceof SubExistRequest) {
            SubExistRequest req = (SubExistRequest) msg;
            if (req.xattr()) {
                extras.writeByte(SUBDOC_FLAG_XATTR_PATH);
            } else {
                extras.writeByte(0);
            }
            if (req.accessDeleted()) {
                extrasLength++;
                extras.writeByte(SUBDOC_DOCFLAG_ACCESS_DELETED);
            }
        } else if (msg instanceof SubGetCountRequest) {
            SubGetCountRequest req = (SubGetCountRequest) msg;
            if (req.xattr()) {
                extras.writeByte(SUBDOC_FLAG_XATTR_PATH);
            } else {
                extras.writeByte(0);
            }
            if (req.accessDeleted()) {
                extrasLength++;
                extras.writeByte(SUBDOC_DOCFLAG_ACCESS_DELETED);
            }
        } else {
            extras.writeByte(0);
        }

        FullBinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, extras, msg.content());
        request.setOpcode(msg.opcode())
                .setKeyLength(keyLength)
                .setExtrasLength(extrasLength)
                .setTotalBodyLength(keyLength + msg.content().readableBytes() + extrasLength)
                .setCAS(cas);

        return request;
    }

    private static BinaryMemcacheRequest handleSubdocumentMultiLookupRequest(ChannelHandlerContext ctx,
                                                                             BinarySubdocMultiLookupRequest msg) {
        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;

        byte extrasLength = 0;

        ByteBuf extras = Unpooled.EMPTY_BUFFER;

        if (msg.docFlags() != 0) {
            extrasLength = 1;
            extras = ctx.alloc().buffer(extrasLength, extrasLength);
            extras.writeByte(msg.docFlags());
        }

        FullBinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, extras, msg.content());
        request.setOpcode(OP_SUB_MULTI_LOOKUP)
                .setKeyLength(keyLength)
                .setExtrasLength(extrasLength)
                .setTotalBodyLength(keyLength + extrasLength + msg.content().readableBytes());

        return request;
    }

    private static BinaryMemcacheRequest handleSubdocumentMultiMutationRequest(ChannelHandlerContext ctx,
                                                                             BinarySubdocMultiMutationRequest msg) {
        byte[] key = msg.keyBytes();
        short keyLength = (short) key.length;

        byte extrasLength = 0;
        ByteBuf extras = Unpooled.EMPTY_BUFFER;

        if (msg.expiration() != 0L) {
            extrasLength = 4;
        }

        if (msg.docFlags() != 0) {
            extrasLength += 1;
        }

        if (extrasLength > 0) {
            extras = ctx.alloc().buffer(extrasLength, extrasLength);
            if (msg.expiration() != 0L) {
                extras.writeInt(msg.expiration());
            }
            if (msg.docFlags() != 0) {
                extras.writeByte(msg.docFlags());
            }
        }

        FullBinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, extras, msg.content());
        request.setOpcode(OP_SUB_MULTI_MUTATION)
                .setCAS(msg.cas())
                .setKeyLength(keyLength)
                .setExtrasLength(extrasLength)
                .setTotalBodyLength(keyLength + msg.content().readableBytes() + extrasLength);

        return request;
    }

    /**
     * Helper method to check if the given datatype byte has the snappy bit set.
     *
     * @param datatype the datatype to check against.
     * @return true if set, false otherwise.
     */
    private static boolean hasCompressionDatatype(final byte datatype) {
        return (datatype & DATATYPE_SNAPPY) == DATATYPE_SNAPPY;
    }

    @Override
    protected CouchbaseResponse decodeResponse(final ChannelHandlerContext ctx, final FullBinaryMemcacheResponse msg)
        throws Exception {
        BinaryRequest request = currentRequest();

        if (request.opaque() != msg.getOpaque()) {
            throw new IllegalStateException("Opaque values for " + msg.getClass() + " do not match.");
        }

        ResponseStatus status = ResponseStatusConverter.fromBinary(msg.getStatus());
        ResponseStatusDetails statusDetails = ResponseStatusConverter.detailsFromBinary(
            msg.getDataType(),
            msg.content()
        );

        if (hasCompressionDatatype(msg.getDataType())) {
            if (!snappyEnabled) {
                LOGGER.debug("Snappy DataType bit set, but snappy has not been negotiated! " +
                    "Trying to decompress nonetheless!", msg);
            }
            handleSnappyDecompression(ctx, msg);
        }

        // Only consult the error map if we don't know what the code is!
        ErrorMap.ErrorCode errorCode = status == ResponseStatus.FAILURE ? ResponseStatusConverter.readErrorCodeFromErrorMap(msg.getStatus()) : null;

        if (errorCode != null) {
            LOGGER.debug("ResponseStatus with Extended Error Code {}", errorCode.toString());

            if (errorCode.attributes().contains(FETCH_CONFIG)) {
                LOGGER.debug(logIdent(ctx, endpoint()) +
                        "Config reload requested by the server, sending config reload message");
                endpoint().signalConfigReload();
            }

            if (errorCode.attributes().contains(CONN_STATE_INVALIDATED)) {
                LOGGER.debug(logIdent(ctx, endpoint()) +
                        "Connection state has been invalidated by the server, reconnecting");
                ctx.close();
                status = ResponseStatus.FAILURE;
            }

            if (errorCode.attributes().contains(TEMP)) {
                LOGGER.debug(logIdent(ctx, endpoint()) +
                        "Temporary failure using error code translation");
                status = ResponseStatus.TEMPORARY_FAILURE;
            }

            if (errorCode.attributes().contains(AUTH)) {
                LOGGER.debug(logIdent(ctx, endpoint()) +
                        "Authentication failure using error code translation");
                status = ResponseStatus.ACCESS_ERROR;
            }

            // For LOCKED we need to make sure we are not retrying to preserve
            // backwards compatible behavior (that is, not retry...)!
            if (errorCode.attributes().contains(ITEM_LOCKED)) {
                errorCode.attributes().remove(RETRY_NOW);
                errorCode.attributes().remove(RETRY_LATER);
                errorCode.attributes().remove(AUTO_RETRY);
                status = ResponseStatus.LOCKED;
            }

            if (errorCode.attributes().contains(AUTO_RETRY) ||
                    errorCode.attributes().contains(RETRY_NOW) ||
                    errorCode.attributes().contains(RETRY_LATER)) {
                LOGGER.debug(logIdent(ctx, endpoint()) +
                        "Retry requested by the server");
                status = ResponseStatus.RETRY;
            }

            if (errorCode.attributes().contains(AUTO_RETRY) && request.retryDelay() == null) {
                if (errorCode.retrySpec().strategy() == CONSTANT) {
                    request.retryDelay(Delay.fixed(errorCode.retrySpec().interval(), TimeUnit.MILLISECONDS));
                } else if (errorCode.retrySpec().strategy() == LINEAR) {
                    request.retryDelay(Delay.linear(TimeUnit.MILLISECONDS, errorCode.retrySpec().ceil(), 0, errorCode.retrySpec().interval()));
                } else if (errorCode.retrySpec().strategy() == EXPONENTIAL) {
                    request.retryDelay(Delay.exponential(TimeUnit.MILLISECONDS, errorCode.retrySpec().ceil(), 0, errorCode.retrySpec().interval()));
                }
                request.retryAfter(errorCode.retrySpec().after());
                request.maxRetryDuration(System.currentTimeMillis() + errorCode.retrySpec().maxDuration());
            }
        }

        if (status.equals(ResponseStatus.RETRY)) {
            resetContentReaderIndex(request);
        } else {
            maybeFreeContent(request);
        }

        msg.content().retain();
        CouchbaseResponse response = handleCommonResponseMessages(request, msg, status, seqOnMutation);

        if (response == null) {
            response = handleSubdocumentResponseMessages(request, msg, ctx, status, seqOnMutation);
        }

        if (response == null) {
            response = handleSubdocumentMultiLookupResponseMessages(request, msg, ctx, status);
        }

        if (response == null) {
            response = handleSubdocumentMultiMutationResponseMessages(request, msg, ctx, status, seqOnMutation);
        }

        if (response == null) {
            response = handleOtherResponseMessages(request, msg, status, seqOnMutation, remoteHostname());
        }

        if (response == null) {
            throw new IllegalStateException("Unhandled request/response pair: " + request.getClass() + "/"
                    + msg.getClass());
        }

        // STAT request produces multiple responses followed by response with NULL key,
        // therefore it should be finished manually
        if (request instanceof StatRequest) {
            ((StatRequest)request).add((StatResponse) response);
            if (((StatResponse) response).key() == null) {
                finishedDecoding();
            }
            // Do not use default publish mechanism for STAT responses, instead accumulate
            // them into List and publish all at once in {@link StatRequest#add()}
            return null;
        } else {
            finishedDecoding();
        }

        if (statusDetails != null) {
            response.statusDetails(statusDetails);
        }

        return response;
    }

    /**
     * Helper method to decode all common response messages.
     *
     * @param request the current request.
     * @param msg the current response message.
     * @param status the response status code.
     * @return the decoded response or null if none did match.
     */
    private static CouchbaseResponse handleCommonResponseMessages(BinaryRequest request, FullBinaryMemcacheResponse msg,
        ResponseStatus status, boolean seqOnMutation) {
        CouchbaseResponse response = null;
        ByteBuf content = msg.content();
        long cas = msg.getCAS();
        short statusCode = msg.getStatus();
        String bucket = request.bucket();

        if (request instanceof GetRequest || request instanceof ReplicaGetRequest) {
            int flags = msg.getExtrasLength() > 0 ? msg.getExtras().getInt(0) : 0;
            response = new GetResponse(status, statusCode, cas, flags, bucket, content, request);
        } else if (request instanceof GetBucketConfigRequest) {
            response = new GetBucketConfigResponse(status, statusCode, bucket, content,
                    ((GetBucketConfigRequest) request).hostname());
        } else if (request instanceof InsertRequest) {
            MutationToken descr = extractToken(bucket, seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new InsertResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof UpsertRequest) {
            MutationToken descr = extractToken(bucket, seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new UpsertResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof ReplaceRequest) {
            MutationToken descr = extractToken(bucket, seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new ReplaceResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof RemoveRequest) {
            MutationToken descr = extractToken(bucket, seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new RemoveResponse(status, statusCode, cas, bucket, content, descr, request);
        }

        return response;
    }

    /**
     * Helper method to decode all simple subdocument response messages.
     *
     * @param request the current request.
     * @param msg the current response message.
     * @param ctx the handler context.
     * @param status the response status code.
     * @return the decoded response or null if none did match.
     */
    private static CouchbaseResponse handleSubdocumentResponseMessages(BinaryRequest request, FullBinaryMemcacheResponse msg,
         ChannelHandlerContext ctx, ResponseStatus status, boolean seqOnMutation) {
        if (!(request instanceof BinarySubdocRequest))
            return null;
        BinarySubdocRequest subdocRequest = (BinarySubdocRequest) request;
        long cas = msg.getCAS();
        short statusCode = msg.getStatus();
        String bucket = request.bucket();

        MutationToken mutationToken = null;
        if (msg.getExtrasLength() > 0) {
            mutationToken = extractToken(bucket, seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
        }

        ByteBuf fragment;
        if (msg.content() != null && msg.content().readableBytes() > 0) {
            fragment = msg.content();
        } else if (msg.content() != null) {
            msg.content().release();
            fragment = Unpooled.EMPTY_BUFFER;
        } else {
            fragment = Unpooled.EMPTY_BUFFER;
        }

        return new SimpleSubdocResponse(status, statusCode, bucket, fragment, subdocRequest, cas, mutationToken);
    }

    /**
     * Helper method to decode all multi lookup response messages.
     *
     * @param request the current request.
     * @param msg the current response message.
     * @param ctx the handler context.
     * @param status the response status code.
     * @return the decoded response or null if it wasn't a subdocument multi lookup.
     */
    private static CouchbaseResponse handleSubdocumentMultiLookupResponseMessages(BinaryRequest request,
            FullBinaryMemcacheResponse msg, ChannelHandlerContext ctx, ResponseStatus status) {
        if (!(request instanceof BinarySubdocMultiLookupRequest))
            return null;
        BinarySubdocMultiLookupRequest subdocRequest = (BinarySubdocMultiLookupRequest) request;

        short statusCode = msg.getStatus();
        long cas = msg.getCAS();
        String bucket = request.bucket();

        ByteBuf body = msg.content();
        List<MultiResult<Lookup>> responses;
        if (status.isSuccess() || ResponseStatus.SUBDOC_MULTI_PATH_FAILURE.equals(status)) {
            long bodyLength = body.readableBytes();
            List<LookupCommand> commands = subdocRequest.commands();
            responses = new ArrayList<MultiResult<Lookup>>(commands.size());
            for (LookupCommand cmd : commands) {
                if (msg.content().readableBytes() < 6) {
                    body.release();
                    throw new IllegalStateException("Expected " + commands.size() + " lookup responses, only got " +
                            responses.size() + ", total of " + bodyLength + " bytes");
                }
                short cmdStatus = body.readShort();
                int valueLength = body.readInt();
                ByteBuf value = ctx.alloc().buffer(valueLength, valueLength);
                value.writeBytes(body, valueLength);

                responses.add(MultiResult.create(cmdStatus, ResponseStatusConverter.fromBinary(cmdStatus),
                        cmd.path(), cmd.lookup(), value));
            }
        } else {
            responses = Collections.emptyList();
        }
        body.release();

        return new MultiLookupResponse(status, statusCode, bucket, responses, subdocRequest, cas);
    }

    /**
     * Helper method to decode all multi mutation response messages.
     *
     * @param request the current request.
     * @param msg the current response message.
     * @param ctx the handler context.
     * @param status the response status code.
     * @return the decoded response or null if it wasn't a subdocument multi lookup.
     */
    private static CouchbaseResponse handleSubdocumentMultiMutationResponseMessages(BinaryRequest request,
            FullBinaryMemcacheResponse msg, ChannelHandlerContext ctx, ResponseStatus status, boolean seqOnMutation) {
        if (!(request instanceof BinarySubdocMultiMutationRequest))
            return null;

        BinarySubdocMultiMutationRequest subdocRequest = (BinarySubdocMultiMutationRequest) request;

        long cas = msg.getCAS();
        short statusCode = msg.getStatus();
        String bucket = request.bucket();

        MutationToken mutationToken = null;
        if (msg.getExtrasLength() > 0) {
            mutationToken = extractToken(bucket, seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
        }

        MultiMutationResponse response;
        ByteBuf body = msg.content();
        List<MultiResult<Mutation>> responses;
        if (status.isSuccess()) {
            List<MutationCommand> commands = subdocRequest.commands();
            responses = new ArrayList<MultiResult<Mutation>>(commands.size());
            //MB-17842: Mutations can have a value, so there could be individual results
            //but only mutation commands that provide a value will have an explicit result in the binary response.
            //However, we still want MutationResult for all of the commands
            ListIterator<MutationCommand> it = commands.listIterator();
            int explicitResultSize = 0;
            //as long as there is an explicit response to read...
            while(msg.content().readableBytes() >= 7) {
                explicitResultSize++;
                //...read the data
                byte responseIndex = body.readByte();
                short responseStatus = body.readShort(); //will this always be SUCCESS?
                int responseLength = body.readInt();
                ByteBuf responseValue;
                if (responseLength > 0) {
                    responseValue = ctx.alloc().buffer(responseLength, responseLength);
                    responseValue.writeBytes(body, responseLength);
                } else {
                    responseValue = Unpooled.EMPTY_BUFFER; //can an explicit response be 0-length (empty)?
                }

                //...sanity check response so subsequent loop don't run forever
                if (it.nextIndex() > responseIndex) {
                    body.release();
                    throw new IllegalStateException("Unable to interpret multi mutation response, responseIndex = " +
                        responseIndex + " while next available command was #" + it.nextIndex());
                }

                ///...catch up on all commands before current one that didn't get an explicit response
                while(it.nextIndex() < responseIndex) {
                    MutationCommand noResultCommand = it.next();
                    responses.add(MultiResult.create(KeyValueStatus.SUCCESS.code(), ResponseStatus.SUCCESS,
                            noResultCommand.path(), noResultCommand.mutation(),
                            Unpooled.EMPTY_BUFFER));
                }

                //...then process the one that did get an explicit response
                MutationCommand cmd = it.next();
                responses.add(MultiResult.create(responseStatus, ResponseStatusConverter.fromBinary(responseStatus),
                        cmd.path(), cmd.mutation(), responseValue));
            }
            //...and finally the remainder of commands after the last one that got an explicit response:
            while(it.hasNext()) {
                MutationCommand noResultCommand = it.next();
                responses.add(MultiResult.create(KeyValueStatus.SUCCESS.code(), ResponseStatus.SUCCESS,
                        noResultCommand.path(), noResultCommand.mutation(),
                        Unpooled.EMPTY_BUFFER));
            }

            if (responses.size() != commands.size()) {
                body.release();
                throw new IllegalStateException("Multi mutation spec size and result size differ: " + commands.size() +
                    " vs " + responses.size() + ", including " + explicitResultSize + " explicit results");
            }

            response = new MultiMutationResponse(bucket, subdocRequest, cas, mutationToken, responses);
        } else if (ResponseStatus.SUBDOC_MULTI_PATH_FAILURE.equals(status)) {
            //MB-17842: order of index and status has been swapped
            byte firstErrorIndex = body.readByte();
            short firstErrorCode = body.readShort();
            response = new MultiMutationResponse(status, statusCode, bucket, firstErrorIndex, firstErrorCode,
                    subdocRequest, cas, mutationToken);
        } else {
            response = new MultiMutationResponse(status, statusCode, bucket, subdocRequest, cas, mutationToken);
        }
        body.release();
        return response;
    }

    private static MutationToken extractToken(String bucket, boolean seqOnMutation, boolean success, ByteBuf extras, long vbid) {
        if (success && seqOnMutation) {
            return new MutationToken(vbid, extras.readLong(), extras.readLong(), bucket);
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
        ResponseStatus status, boolean seqOnMutation, String remoteHostname) {
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
            MutationToken descr = extractToken(bucket, seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new AppendResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof PrependRequest) {
            MutationToken descr = extractToken(bucket, seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new PrependResponse(status, statusCode, cas, bucket, content, descr, request);
        } else if (request instanceof KeepAliveRequest) {
            releaseContent(content);
            response = new KeepAliveResponse(status, statusCode, request);
        } else if (request instanceof NoopRequest) {
            releaseContent(content);
            response = new NoopResponse(status, statusCode, request);
        } else if (request instanceof CounterRequest) {
            long value = status.isSuccess() ? content.readLong() : 0;
            releaseContent(content);

            MutationToken descr = extractToken(bucket, seqOnMutation, status.isSuccess(), msg.getExtras(), request.partition());
            response = new CounterResponse(status, statusCode, bucket, value, cas, descr, request);
        } else if (request instanceof StatRequest) {
            String key = null;
            if (msg.getKey() != null) {
                key = new String(msg.getKey(), CHARSET);
            }
            String value = content.toString(CHARSET);
            releaseContent(content);

            response = new StatResponse(status, statusCode, remoteHostname, key, value, bucket, request);
        } else if (request instanceof GetAllMutationTokensRequest) {
            // 2 bytes for partition ID, and 8 bytes for sequence number
            MutationToken[] mutationTokens = new MutationToken[content.readableBytes() / 10];
            for (int i = 0; i < mutationTokens.length; i++) {
                mutationTokens[i] = new MutationToken((long)content.readShort(), 0, content.readLong(), request.bucket());
            }
            releaseContent(content);
            response = new GetAllMutationTokensResponse(mutationTokens, status, statusCode, bucket, request);
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
        releaseContent(contentFromWriteRequest(request));
    }

    /**
     * Helper method to extract the content from requests.
     */
    private static ByteBuf contentFromWriteRequest(BinaryRequest request) {
        ByteBuf content = null;
        if (request instanceof BinaryStoreRequest) {
            content = ((BinaryStoreRequest) request).content();
        } else if (request instanceof AppendRequest) {
            content = ((AppendRequest) request).content();
        } else if (request instanceof PrependRequest) {
            content = ((PrependRequest) request).content();
        } else if (request instanceof BinarySubdocRequest) {
            content = ((BinarySubdocRequest) request).content();
        } else if (request instanceof BinarySubdocMultiLookupRequest) {
            content = ((BinarySubdocMultiLookupRequest) request).content();
        } else if (request instanceof BinarySubdocMultiMutationRequest) {
            content = ((BinarySubdocMultiMutationRequest) request).content();
        }
        return content;
    }

    /**
     * Helper method to reset the reader index of the content so if downstream components
     * on outbound did modify it, it can be reused (looking at you, SSLHandler).
     *
     * @param request the request which may have content that needs to be reset.
     */
    private static void resetContentReaderIndex(BinaryRequest request) {
        ByteBuf content = contentFromWriteRequest(request);
        if (content != null) {
            try {
                content.readerIndex(0);
            } catch (Exception ex) {
                LOGGER.warn("Exception while resetting the content reader index to 0, " +
                    "please report this as a bug.", ex);
            }
        }
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
        if (evt instanceof ServerFeaturesEvent) {
            seqOnMutation = env().mutationTokensEnabled() &&
                ((ServerFeaturesEvent) evt).supportedFeatures().contains(ServerFeatures.MUTATION_SEQNO);
            snappyEnabled = ((ServerFeaturesEvent) evt).supportedFeatures().contains(ServerFeatures.SNAPPY);
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected CouchbaseRequest createKeepAliveRequest() {
        return new KeepAliveRequest();
    }

    protected static class KeepAliveRequest extends AbstractKeyValueRequest implements KeepAlive {

        protected KeepAliveRequest() {
            super(null, null);
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
