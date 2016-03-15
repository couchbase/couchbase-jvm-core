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

package com.couchbase.client.core.endpoint.dcp;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.dcp.AbstractDCPRequest;
import com.couchbase.client.core.message.dcp.ControlParameter;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.DCPResponse;
import com.couchbase.client.core.message.dcp.ExpirationMessage;
import com.couchbase.client.core.message.dcp.FailoverLogEntry;
import com.couchbase.client.core.message.dcp.GetFailoverLogRequest;
import com.couchbase.client.core.message.dcp.GetFailoverLogResponse;
import com.couchbase.client.core.message.dcp.GetLastCheckpointRequest;
import com.couchbase.client.core.message.dcp.GetLastCheckpointResponse;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.client.core.message.dcp.StreamEndMessage;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import com.lmax.disruptor.EventSink;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import rx.functions.Action1;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * @author Sergey Avseyev
 * @since 1.1.0
 */
public class DCPHandler extends AbstractGenericHandler<FullBinaryMemcacheResponse, BinaryMemcacheRequest, DCPRequest> {

    public static final byte OP_OPEN_CONNECTION = 0x50;
    public static final byte OP_STREAM_REQUEST = 0x53;
    public static final byte OP_STREAM_END = 0x55;
    public static final byte OP_SNAPSHOT_MARKER = 0x56;
    public static final byte OP_MUTATION = 0x57;
    public static final byte OP_REMOVE = 0x58;
    public static final byte OP_EXPIRATION = 0x59;
    public static final byte OP_CONTROL = 0x5e;
    public static final byte OP_BUFFER_ACK = 0x5d;
    public static final byte OP_GET_FAILOVER_LOG = 0x54;
    public static final byte OP_GET_LAST_CHECKPOINT = (byte) 0x97;
    /**
     * The Logger used in this handler.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DCPHandler.class);
    private DCPConnection connection;

    /**
     * Creates a new {@link DCPHandler} with the default queue for requests.
     *
     * @param endpoint       the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link EventSink} to push responses into.
     */
    public DCPHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, boolean isTransient) {
        this(endpoint, responseBuffer, new ArrayDeque<DCPRequest>(), isTransient);
    }

    /**
     * Creates a new {@link DCPHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint       the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link EventSink} to push responses into.
     * @param queue          the queue which holds all outstanding open requests.
     */
    public DCPHandler(AbstractEndpoint endpoint, EventSink<ResponseEvent> responseBuffer, Queue<DCPRequest> queue, boolean isTransient) {
        super(endpoint, responseBuffer, queue, isTransient);
    }

    @Override
    protected BinaryMemcacheRequest encodeRequest(ChannelHandlerContext ctx, DCPRequest msg) throws Exception {
        BinaryMemcacheRequest request;

        if (msg instanceof OpenConnectionRequest) {
            OpenConnectionRequest openConnection = (OpenConnectionRequest) msg;
            request = handleOpenConnectionRequest(ctx, openConnection);
            connection = new DCPConnection(env(), openConnection.connectionName(), openConnection.bucket());
        } else if (msg instanceof StreamRequestRequest) {
            request = handleStreamRequestRequest(ctx, (StreamRequestRequest) msg);
        } else if (msg instanceof GetFailoverLogRequest) {
            request = handleFailoverLogsRequest(ctx, (GetFailoverLogRequest) msg);
        } else if (msg instanceof GetLastCheckpointRequest) {
            request = handleGetLastCheckpointRequest(ctx, (GetLastCheckpointRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown incoming DCPRequest type " + msg.getClass());
        }

        if (msg.partition() >= 0) {
            request.setReserved(msg.partition());
        }

        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg)
            throws Exception {
        DCPRequest request = currentRequest();
        DCPResponse response = null;

        if (connection != null) {
            // FIXME: preserve context to allow consumer to signal about progress.
            connection.setLastContext(ctx);
        }
        if (msg.getOpcode() == OP_OPEN_CONNECTION && request instanceof OpenConnectionRequest) {
            response = new OpenConnectionResponse(ResponseStatusConverter.fromBinary(msg.getStatus()), connection, request);
            if (env().dcpConnectionBufferSize() > 0) {
                ctx.writeAndFlush(controlRequest(ctx, ControlParameter.CONNECTION_BUFFER_SIZE, env().dcpConnectionBufferSize()));
            }
        } else if (msg.getOpcode() == OP_STREAM_REQUEST && request instanceof StreamRequestRequest) {
            ByteBuf content = msg.content();
            List<FailoverLogEntry> failoverLog = null;
            long rollbackToSequenceNumber = 0;
            KeyValueStatus status = KeyValueStatus.valueOf(msg.getStatus());
            switch (status) {
                case SUCCESS:
                    failoverLog = readFailoverLogs(content);
                    break;
                case ERR_ROLLBACK:
                    rollbackToSequenceNumber = content.readLong();
                    break;
                default:
                    LOGGER.warn("Unexpected status of StreamRequestResponse: {} (0x{}, {})",
                            status, Integer.toHexString(status.code()), status.description());
            }
            response = new StreamRequestResponse(ResponseStatusConverter.fromBinary(msg.getStatus()),
                    failoverLog, rollbackToSequenceNumber, request, connection);
            connection.consumed(msg);
        } else if (msg.getOpcode() == OP_GET_FAILOVER_LOG && request instanceof GetFailoverLogRequest) {
            response = new GetFailoverLogResponse(ResponseStatusConverter.fromBinary(msg.getStatus()),
                    readFailoverLogs(msg.content()), request);
        } else if (msg.getOpcode() == OP_GET_LAST_CHECKPOINT && request instanceof GetLastCheckpointRequest) {
            long sequenceNumber = msg.content().readLong();
            response = new GetLastCheckpointResponse(ResponseStatusConverter.fromBinary(msg.getStatus()),
                    sequenceNumber, request);
        } else if (msg.getOpcode() == OP_CONTROL || msg.getOpcode() == OP_BUFFER_ACK) {
            KeyValueStatus status = KeyValueStatus.valueOf(msg.getStatus());
            if (status != KeyValueStatus.SUCCESS) {
                LOGGER.warn("Unexpected status of service response (opcode={}): {} (0x{}, {})",
                        Integer.toHexString(msg.getOpcode()), status, Integer.toHexString(status.code()), status.description());
            }
        } else {
            /**
             * FIXME
             * There are two cases:
             *
             * 1) Queue is empty:
             *    In this case outer method {@link AbstractGenericHandler#decode} picks 'null'
             *    as a current request, and it will might lead to NPEs because it does not cover
             *    scenario when we receive some packet without request in the queue.
             *
             * 2) Queue is not empty:
             *    In this case we might notify about errors wrong observable in the outer method,
             *    so we have to substitute it as soon as we detected that incoming message is not
             *    relevant to 'current request'
             */
            final DCPRequest oldRequest = currentRequest();
            final DCPRequest dummy = new AbstractDCPRequest(connection.bucket(), null) {
            };
            dummy.observable().subscribe(new Action1<CouchbaseResponse>() {
                @Override
                public void call(CouchbaseResponse couchbaseResponse) {
                    // skip
                }
            }, new Action1<Throwable>() {
                @Override
                public void call(Throwable throwable) {
                    connection.subject().onError(throwable);
                }
            });
            try {
                currentRequest(dummy);
                handleDCPRequest(ctx, msg);
            } finally {
                currentRequest(oldRequest);
            }
        }
        if (request != null && request.partition() >= 0 && response != null) {
            response.partition(request.partition());
        }

        if (response != null || request == null) {
            finishedDecoding();
        }
        return response;
    }

    /**
     * Handles incoming stream of DCP messages.
     */
    private void handleDCPRequest(final ChannelHandlerContext ctx, final FullBinaryMemcacheResponse msg) {
        DCPRequest request = null;
        int flags = 0;
        long bySeqno = 0;
        long revSeqno = 0;

        switch (msg.getOpcode()) {
            case OP_SNAPSHOT_MARKER:
                long startSequenceNumber = 0;
                long endSequenceNumber = 0;
                if (msg.getExtrasLength() > 0) {
                    final ByteBuf extras = msg.getExtras();
                    startSequenceNumber = extras.readLong();
                    endSequenceNumber = extras.readLong();
                    flags = extras.readInt();
                }
                request = new SnapshotMarkerMessage(connection, msg.getTotalBodyLength(), msg.getStatus(),
                        startSequenceNumber, endSequenceNumber, flags, connection.bucket());
                break;

            case OP_MUTATION:
                int expiration = 0;
                int lockTime = 0;

                if (msg.getExtrasLength() > 0) {
                    final ByteBuf extras = msg.getExtras();
                    bySeqno = extras.readLong();
                    revSeqno = extras.readLong();
                    flags = extras.readInt();
                    expiration = extras.readInt();
                    lockTime = extras.readInt();
                }
                request = new MutationMessage(connection, msg.getTotalBodyLength(), msg.getStatus(), new String(msg.getKey(), CHARSET),
                        msg.content().retain(), expiration, bySeqno, revSeqno, flags, lockTime,
                        msg.getCAS(), connection.bucket());
                break;

            case OP_REMOVE:
                if (msg.getExtrasLength() > 0) {
                    final ByteBuf extras = msg.getExtras();
                    bySeqno = extras.readLong();
                    revSeqno = extras.readLong();
                }
                request = new RemoveMessage(connection, msg.getTotalBodyLength(), msg.getStatus(), new String(msg.getKey(), CHARSET),
                        msg.getCAS(), bySeqno, revSeqno, connection.bucket());
                break;

            case OP_EXPIRATION:
                if (msg.getExtrasLength() > 0) {
                    final ByteBuf extras = msg.getExtras();
                    bySeqno = extras.readLong();
                    revSeqno = extras.readLong();
                }
                request = new ExpirationMessage(connection, msg.getTotalBodyLength(), msg.getStatus(), new String(msg.getKey(), CHARSET),
                        msg.getCAS(), bySeqno, revSeqno, connection.bucket());
                break;

            case OP_STREAM_END:
                final ByteBuf extras = msg.getExtras();
                flags = extras.readInt();
                request = new StreamEndMessage(connection, msg.getTotalBodyLength(), msg.getStatus(),
                        StreamEndMessage.Reason.valueOf(flags), connection.bucket());
                connection.removeStream(msg.getOpaque());
                break;

            default:
                LOGGER.info("Unhandled DCP message: {}, {}", msg.getOpcode(), msg);
        }
        if (request != null) {
            connection.subject().onNext(request);
        }

        if (connection.streamsCount() == 0) {
            connection.subject().onCompleted();
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (connection != null) {
            connection.subject().onCompleted();
        }
        super.handlerRemoved(ctx);
    }

    private FullBinaryMemcacheRequest controlRequest(ChannelHandlerContext ctx, ControlParameter parameter, boolean value) {
        return controlRequest(ctx, parameter, Boolean.toString(value));
    }

    private FullBinaryMemcacheRequest controlRequest(ChannelHandlerContext ctx, ControlParameter parameter, int value) {
        return controlRequest(ctx, parameter, Integer.toString(value));
    }

    private FullBinaryMemcacheRequest controlRequest(ChannelHandlerContext ctx, ControlParameter parameter, String value) {
        byte[] key = parameter.value().getBytes(CharsetUtil.UTF_8);
        short keyLength = (short) key.length;
        byte[] val = value.getBytes(CharsetUtil.UTF_8);
        ByteBuf body = ctx.alloc().buffer(val.length);
        body.writeBytes(val);

        FullBinaryMemcacheRequest request = new DefaultFullBinaryMemcacheRequest(key, Unpooled.EMPTY_BUFFER, body);
        request.setOpcode(OP_CONTROL);
        request.setKeyLength(keyLength);
        request.setTotalBodyLength(keyLength + body.readableBytes());
        return request;
    }

    /**
     * Creates a DCP Open Connection Request.
     *
     * @param ctx the channel handler context.
     * @param msg the open connect message.
     * @return a converted {@link BinaryMemcacheRequest}.
     */
    private BinaryMemcacheRequest handleOpenConnectionRequest(final ChannelHandlerContext ctx,
                                                              final OpenConnectionRequest msg) {
        ByteBuf extras = ctx.alloc().buffer(8);
        extras
                .writeInt(msg.sequenceNumber())
                .writeInt(msg.type().flags());

        byte[] key = msg.connectionName().getBytes(CharsetUtil.UTF_8);
        byte extrasLength = (byte) extras.readableBytes();
        short keyLength = (short) key.length;

        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(key, extras);
        request.setOpcode(OP_OPEN_CONNECTION);
        request.setKeyLength(keyLength);
        request.setExtrasLength(extrasLength);
        request.setTotalBodyLength(keyLength + extrasLength);

        return request;
    }

    /**
     * Creates a DCP Stream Request.
     *
     * See the [Protocol Description](https://github.com/couchbaselabs/dcp-documentation/blob/master/documentation/
     * commands/stream-request.md) for more details.
     *
     * @param ctx the channel handler context.
     * @param msg the stream request message.
     * @return a converted {@link BinaryMemcacheRequest}.
     */
    private BinaryMemcacheRequest handleStreamRequestRequest(final ChannelHandlerContext ctx,
                                                             final StreamRequestRequest msg) {
        ByteBuf extras = ctx.alloc().buffer(48);
        extras
                .writeInt(0) // flags
                .writeInt(0) // reserved
                .writeLong(msg.startSequenceNumber()) // start sequence number
                .writeLong(msg.endSequenceNumber()) // end sequence number
                .writeLong(msg.vbucketUUID()) // vbucket UUID
                .writeLong(msg.snapshotStartSequenceNumber()) // snapshot start sequence number
                .writeLong(msg.snapshotEndSequenceNumber()); // snapshot end sequence number

        byte extrasLength = (byte) extras.readableBytes();

        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(extras);
        request.setOpcode(OP_STREAM_REQUEST);
        request.setExtrasLength(extrasLength);
        request.setTotalBodyLength(extrasLength);
        request.setOpaque(connection.addStream(connection.name()));

        return request;
    }

    private BinaryMemcacheRequest handleFailoverLogsRequest(ChannelHandlerContext ctx, GetFailoverLogRequest msg) {
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest();
        request.setOpcode(OP_GET_FAILOVER_LOG);
        request.setReserved(msg.partition());

        return request;
    }

    private List<FailoverLogEntry> readFailoverLogs(final ByteBuf content) {
        List<FailoverLogEntry> failoverLog = new ArrayList<FailoverLogEntry>(content.readableBytes() / 16);
        while (content.readableBytes() >= 16) {
            FailoverLogEntry entry = new FailoverLogEntry(content.readLong(), content.readLong());
            failoverLog.add(entry);
        }
        return failoverLog;
    }

    private BinaryMemcacheRequest handleGetLastCheckpointRequest(ChannelHandlerContext ctx, GetLastCheckpointRequest msg) {
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest();
        request.setOpcode(OP_GET_LAST_CHECKPOINT);
        request.setReserved(msg.partition());

        return request;
    }

    @Override
    protected ServiceType serviceType() {
        return ServiceType.DCP;
    }
}
