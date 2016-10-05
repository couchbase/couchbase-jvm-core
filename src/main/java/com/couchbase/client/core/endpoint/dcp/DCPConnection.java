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

package com.couchbase.client.core.endpoint.dcp;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.dcp.DCPMessage;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.FailoverLogEntry;
import com.couchbase.client.core.message.dcp.GetFailoverLogRequest;
import com.couchbase.client.core.message.dcp.GetFailoverLogResponse;
import com.couchbase.client.core.message.dcp.StreamCloseRequest;
import com.couchbase.client.core.message.dcp.StreamCloseResponse;
import com.couchbase.client.core.message.dcp.StreamEndMessage;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;
import com.couchbase.client.core.message.kv.GetAllMutationTokensRequest;
import com.couchbase.client.core.message.kv.GetAllMutationTokensResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.UnicastAutoReleaseSubject;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.internal.ConcurrentSet;
import rx.Observable;
import rx.functions.Action2;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import java.util.HashMap;
import java.util.Map;
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
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DCPConnection.class);

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
                TimeUnit.MILLISECONDS, env.scheduler())
                .withTraceIdentifier("DCPConnection." + env.dcpConnectionName())
                .toSerialized());
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
        return core
                .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .flatMap(new Func1<GetClusterConfigResponse, Observable<NodeInfo>>() {
                    @Override
                    public Observable<NodeInfo> call(GetClusterConfigResponse response) {
                        CouchbaseBucketConfig cfg = (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                        return Observable.from(cfg.nodes());
                    }
                })
                .filter(new Func1<NodeInfo, Boolean>() {
                    @Override
                    public Boolean call(NodeInfo node) {
                        return node.services().containsKey(ServiceType.DCP);
                    }
                })
                .flatMap(new Func1<NodeInfo, Observable<GetAllMutationTokensResponse>>() {
                    @Override
                    public Observable<GetAllMutationTokensResponse> call(NodeInfo node) {
                        return core.send(new GetAllMutationTokensRequest(node.hostname(), bucket));
                    }
                })
                .collect(new Func0<Map<Integer, MutationToken>>() {
                    @Override
                    public Map<Integer, MutationToken> call() {
                        return new HashMap<Integer, MutationToken>(1024);
                    }
                }, new Action2<Map<Integer, MutationToken>, GetAllMutationTokensResponse>() {
                    @Override
                    public void call(Map<Integer, MutationToken> collector, GetAllMutationTokensResponse response) {
                        for (MutationToken token : response.mutationTokens()) {
                            int key = (int) token.vbucketID();
                            MutationToken prev = collector.get(key);
                            MutationToken current = token;
                            if (prev != null && prev.sequenceNumber() != token.sequenceNumber()) {
                                if (current.sequenceNumber() < prev.sequenceNumber()) {
                                    current = prev;
                                }
                                LOGGER.debug("nodes are not agree on sequence number for vbucket {}: old={}, new={}, selected={}",
                                        token.vbucketID(), prev.sequenceNumber(), token.sequenceNumber(), current.sequenceNumber());
                            }
                            collector.put(key, current);
                        }
                    }
                })
                .flatMap(new Func1<Map<Integer, MutationToken>, Observable<MutationToken>>() {
                    @Override
                    public Observable<MutationToken> call(Map<Integer, MutationToken> sequenceNumbers) {
                        return Observable.from(sequenceNumbers.values());
                    }
                })
                .flatMap(new Func1<MutationToken, Observable<MutationToken>>() {
                    @Override
                    public Observable<MutationToken> call(final MutationToken token) {
                        return core.<GetFailoverLogResponse>send(new GetFailoverLogRequest((short) token.vbucketID(), bucket))
                                .map(new Func1<GetFailoverLogResponse, MutationToken>() {
                                    @Override
                                    public MutationToken call(GetFailoverLogResponse failoverLogsResponse) {
                                        final FailoverLogEntry entry = failoverLogsResponse.failoverLog().get(0);
                                        return new MutationToken(
                                                failoverLogsResponse.partition(),
                                                entry.vbucketUUID(),
                                                token.sequenceNumber(),
                                                bucket);
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
