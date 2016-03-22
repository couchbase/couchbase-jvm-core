/*
 * Copyright (c) 2016 Couchbase, Inc.
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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.dcp.DCPMessage;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.FailoverLogEntry;
import com.couchbase.client.core.message.dcp.GetFailoverLogRequest;
import com.couchbase.client.core.message.dcp.GetFailoverLogResponse;
import com.couchbase.client.core.message.dcp.GetLastCheckpointRequest;
import com.couchbase.client.core.message.dcp.GetLastCheckpointResponse;
import com.couchbase.client.core.message.dcp.StreamCloseRequest;
import com.couchbase.client.core.message.dcp.StreamCloseResponse;
import com.couchbase.client.core.message.dcp.StreamEndMessage;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.utils.UnicastAutoReleaseSubject;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.internal.ConcurrentSet;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * DCP connection used to subscribe to DCP streams.
 *
 * @author Sergey Avseyev
 * @since 1.2.6
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class DCPConnection {
    private static final AttributeKey<Integer> CONSUMED_BYTES = AttributeKey.newInstance("CONSUMED_BYTES");
    private static final int MINIMUM_HEADER_SIZE = 24;

    private final SerializedSubject<DCPRequest, DCPRequest> subject;
    private final Set<Short> streams;
    private final ClusterFacade core;
    private final String bucket;
    private final String password;
    private final CoreEnvironment env;
    private final ConcurrentMap<Short, ChannelHandlerContext> contexts;

    public DCPConnection(final CoreEnvironment env, final ClusterFacade core, final String bucket, final String password) {
        this(env, core, bucket, password, UnicastAutoReleaseSubject.<DCPRequest>create(env.autoreleaseAfter(),
                TimeUnit.MILLISECONDS, env.scheduler()).toSerialized());
    }

    public DCPConnection(final CoreEnvironment env, final ClusterFacade core, final String bucket, final String password,
                         final SerializedSubject<DCPRequest, DCPRequest> subject) {
        this.env = env;
        this.core = core;
        this.subject = subject;
        this.bucket = bucket;
        this.password = password;
        this.streams = new ConcurrentSet<Short>();
        this.contexts = new ConcurrentHashMap<Short, ChannelHandlerContext>();
    }

    public String bucket() {
        return bucket;
    }

    public Subject<DCPRequest, DCPRequest> subject() {
        return subject;
    }

    public Observable<ResponseStatus> addStream(short partition) {
        return addStream(partition, 0, 0, 0xffffffff, 0, 0);
    }

    public Observable<ResponseStatus> addStream(final short partition,
                                                final long vbucketUUID,
                                                final long startSequenceNumber,
                                                final long endSequenceNumber,
                                                final long snapshotStartSequenceNumber,
                                                final long snapshotEndSequenceNumber) {
        if (streams.contains(partition)) {
            return Observable.just(ResponseStatus.EXISTS);
        }
        final DCPConnection connection = this;
        return Observable.defer(new Func0<Observable<StreamRequestResponse>>() {
            @Override
            public Observable<StreamRequestResponse> call() {
                return core.send(new StreamRequestRequest(partition, vbucketUUID, startSequenceNumber,
                        endSequenceNumber, snapshotStartSequenceNumber, snapshotEndSequenceNumber,
                        bucket, password, connection));
            }
        }).flatMap(new Func1<StreamRequestResponse, Observable<StreamRequestResponse>>() {
            @Override
            public Observable<StreamRequestResponse> call(StreamRequestResponse response) {
                long rollbackSequenceNumber;
                switch (response.status()) {
                    case RANGE_ERROR:
                        rollbackSequenceNumber = 0;
                        break;
                    case ROLLBACK:
                        rollbackSequenceNumber = response.rollbackToSequenceNumber();
                        break;
                    default:
                        return Observable.just(response);
                }
                return core.send(new StreamRequestRequest(partition, vbucketUUID, rollbackSequenceNumber,
                        endSequenceNumber, rollbackSequenceNumber, snapshotEndSequenceNumber,
                        bucket, password, connection));
            }
        }).map(new Func1<StreamRequestResponse, ResponseStatus>() {
            @Override
            public ResponseStatus call(StreamRequestResponse response) {
                if (response.status() == ResponseStatus.SUCCESS) {
                    streams.add(partition);
                }
                return response.status();
            }
        });
    }


    public Observable<ResponseStatus> removeStream(final short partition) {
        if (!streams.contains(partition)) {
            return Observable.just(ResponseStatus.NOT_EXISTS);
        }
        return Observable.defer(new Func0<Observable<StreamCloseResponse>>() {
            @Override
            public Observable<StreamCloseResponse> call() {
                return core.send(new StreamCloseRequest(partition, bucket, password));
            }
        }).map(new Func1<StreamCloseResponse, ResponseStatus>() {
            @Override
            public ResponseStatus call(StreamCloseResponse response) {
                if (response.status() == ResponseStatus.SUCCESS) {
                    streams.remove(partition);
                }
                return response.status();
            }
        });
    }

    public Observable<MutationToken> getCurrentState() {
        return partitionSize().flatMap(new Func1<Integer, Observable<MutationToken>>() {
            @Override
            public Observable<MutationToken> call(final Integer numPartitions) {
                return Observable.range(0, numPartitions).flatMap(new Func1<Integer, Observable<GetFailoverLogResponse>>() {
                    @Override
                    public Observable<GetFailoverLogResponse> call(final Integer partition) {
                        return core.send(new GetFailoverLogRequest(partition.shortValue(), bucket));
                    }
                }).flatMap(new Func1<GetFailoverLogResponse, Observable<MutationToken>>() {
                    @Override
                    public Observable<MutationToken> call(final GetFailoverLogResponse failoverLogsResponse) {
                        final FailoverLogEntry entry = failoverLogsResponse.failoverLog().get(0);
                        return core.<GetLastCheckpointResponse>send(new GetLastCheckpointRequest(failoverLogsResponse.partition(), bucket))
                                .map(new Func1<GetLastCheckpointResponse, MutationToken>() {
                                    @Override
                                    public MutationToken call(GetLastCheckpointResponse lastCheckpointResponse) {
                                        return new MutationToken(
                                                failoverLogsResponse.partition(),
                                                entry.vbucketUUID(),
                                                lastCheckpointResponse.sequenceNumber(),
                                                bucket);
                                    }
                                });
                    }
                });
            }
        });
    }

    public void consumed(final DCPMessage event) {
        consumed(event.partition(), event.totalBodyLength());
    }

    /*package*/ void consumed(short partition, int delta) {
        if (env.dcpConnectionBufferSize() > 0) {
            ChannelHandlerContext ctx = contexts.get(partition);
            if (ctx == null) {
                return;
            }
            synchronized (ctx) {
                Attribute<Integer> attr = ctx.attr(CONSUMED_BYTES);
                Integer consumedBytes = attr.get();
                if (consumedBytes == null) {
                    consumedBytes = 0;
                }
                consumedBytes += MINIMUM_HEADER_SIZE + delta;
                if (consumedBytes >= env.dcpConnectionBufferSize() * env.dcpConnectionBufferAckThreshold()) {
                    ctx.writeAndFlush(createBufferAcknowledgmentRequest(ctx, consumedBytes));
                    consumedBytes = 0;
                }
                attr.set(consumedBytes);
            }
        }
    }

    /*package*/ void streamClosed(final short partition, final StreamEndMessage.Reason reason) {
        streams.remove(partition);
    }

    /**
     * Helper method to fetch the number of partitions.
     *
     * @return the number of partitions.
     */
    private Observable<Integer> partitionSize() {
        return core
                .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .map(new Func1<GetClusterConfigResponse, Integer>() {
                    @Override
                    public Integer call(GetClusterConfigResponse response) {
                        CouchbaseBucketConfig config = (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                        return config.numberOfPartitions();
                    }
                });
    }

    /*package*/ void registerContext(short partition, ChannelHandlerContext ctx) {
        contexts.put(partition, ctx);
    }

    private BinaryMemcacheRequest createBufferAcknowledgmentRequest(ChannelHandlerContext ctx, int bufferBytes) {
        ByteBuf extras = ctx.alloc().buffer(4).writeInt(bufferBytes);
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(new byte[]{}, extras);
        request.setOpcode(DCPHandler.OP_BUFFER_ACK);
        request.setExtrasLength((byte) extras.readableBytes());
        request.setTotalBodyLength(extras.readableBytes());
        return request;
    }
}
