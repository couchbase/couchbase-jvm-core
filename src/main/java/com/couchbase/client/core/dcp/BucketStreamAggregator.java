/*
 * Copyright (c) 2015 Couchbase, Inc.
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

package com.couchbase.client.core.dcp;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.FailoverLogEntry;
import com.couchbase.client.core.message.dcp.GetFailoverLogRequest;
import com.couchbase.client.core.message.dcp.GetFailoverLogResponse;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;
import com.couchbase.client.core.message.kv.GetAllMutationTokensRequest;
import com.couchbase.client.core.message.kv.GetAllMutationTokensResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import rx.Observable;
import rx.functions.Action2;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides a higher level abstraction over a DCP stream.
 *
 * The bucket is expected to be opened already when the {@link #feed()} method is called.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class BucketStreamAggregator {
    public static String DEFAULT_CONNECTION_NAME = "jvmCore";

    private final ClusterFacade core;
    private final String bucket;
    private final String name;
    private final AtomicReference<DCPConnection> connection = new AtomicReference<DCPConnection>();

    public BucketStreamAggregator(final ClusterFacade core, final String bucket) {
        this(DEFAULT_CONNECTION_NAME, core, bucket);
    }

    /**
     * Create BucketStreamAggregator instance
     *
     * @param name   name for DCP connection
     * @param core
     * @param bucket bucket name
     */
    public BucketStreamAggregator(final String name, final ClusterFacade core, final String bucket) {
        this.core = core;
        this.bucket = bucket;
        this.name = name;
    }

    public String name() {
        return name;
    }

    /**
     * Opens a DCP stream with default name and returns the feed of changes from beginning.
     *
     * @return the feed with {@link DCPRequest}s.
     */
    public Observable<DCPRequest> feed() {
        final BucketStreamAggregatorState state = new BucketStreamAggregatorState();
        int numPartitions = partitionSize().toBlocking().first();
        for (short partition = 0; partition < numPartitions; partition++) {
            state.put(new BucketStreamState(partition, 0, 0, 0xffffffff, 0, 0xffffffff));
        }
        return feed(state);
    }

    /**
     * Opens a DCP stream and returns the feed of changes.
     *
     * @param aggregatorState state object
     * @return the feed with {@link DCPRequest}s.
     */
    public Observable<DCPRequest> feed(final BucketStreamAggregatorState aggregatorState) {
        return open()
                .flatMap(new Func1<DCPConnection, Observable<DCPRequest>>() {
                    @Override
                    public Observable<DCPRequest> call(final DCPConnection response) {
                        return Observable
                                .from(aggregatorState)
                                .flatMap(new Func1<BucketStreamState, Observable<StreamRequestResponse>>() {
                                    @Override
                                    public Observable<StreamRequestResponse> call(final BucketStreamState feed) {
                                        Observable<StreamRequestResponse> res =
                                                core.send(new StreamRequestRequest(feed.partition(), feed.vbucketUUID(),
                                                        feed.startSequenceNumber(), feed.endSequenceNumber(),
                                                        feed.snapshotStartSequenceNumber(), feed.snapshotEndSequenceNumber(),
                                                        bucket));
                                        return res.flatMap(new Func1<StreamRequestResponse, Observable<StreamRequestResponse>>() {
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
                                                return core.send(new StreamRequestRequest(feed.partition(), feed.vbucketUUID(),
                                                        rollbackSequenceNumber, feed.endSequenceNumber(),
                                                        rollbackSequenceNumber, feed.snapshotEndSequenceNumber(),
                                                        bucket));
                                            }
                                        });
                                    }
                                })
                                .toList()
                                .flatMap(new Func1<List<StreamRequestResponse>, Observable<DCPRequest>>() {
                                    @Override
                                    public Observable<DCPRequest> call(List<StreamRequestResponse> streamRequestResponses) {
                                        return connection.get().subject();
                                    }
                                });
                    }
                });
    }

    /**
     * Retrieve current state of the partitions.
     *
     * It has all sequence and snapshot numbers set to the last known sequence number.
     *
     * @return state object
     */
    public Observable<BucketStreamAggregatorState> getCurrentState() {
        return open()
                .flatMap(new Func1<DCPConnection, Observable<GetAllMutationTokensResponse>>() {
                    @Override
                    public Observable<GetAllMutationTokensResponse> call(final DCPConnection response) {
                        return core.send(new GetAllMutationTokensRequest(bucket));
                    }
                })
                .flatMap(new Func1<GetAllMutationTokensResponse, Observable<BucketStreamAggregatorState>>() {
                    @Override
                    public Observable<BucketStreamAggregatorState> call(final GetAllMutationTokensResponse response) {
                        BucketStreamAggregatorState state = new BucketStreamAggregatorState();
                        for (MutationToken token : response.mutationTokens()) {
                            state.put(new BucketStreamState((short) token.vbucketID(), token.vbucketUUID(), token.sequenceNumber()));
                        }
                        return Observable.just(state);
                    }
                })
                .flatMap(new Func1<BucketStreamAggregatorState, Observable<BucketStreamAggregatorState>>() {
                    @Override
                    public Observable<BucketStreamAggregatorState> call(final BucketStreamAggregatorState aggregatorState) {
                        return Observable
                                .from(aggregatorState)
                                .flatMap(new Func1<BucketStreamState, Observable<GetFailoverLogResponse>>() {
                                    @Override
                                    public Observable<GetFailoverLogResponse> call(BucketStreamState streamState) {
                                        return core.send(new GetFailoverLogRequest(streamState.partition(), bucket));
                                    }
                                })
                                .collect(new Func0<BucketStreamAggregatorState>() {
                                    @Override
                                    public BucketStreamAggregatorState call() {
                                        return aggregatorState;
                                    }
                                }, new Action2<BucketStreamAggregatorState, GetFailoverLogResponse>() {
                                    @Override
                                    public void call(BucketStreamAggregatorState state, GetFailoverLogResponse response) {
                                        final FailoverLogEntry entry = response.failoverLog().get(0);
                                        state.put(new BucketStreamState(response.partition(), entry.vbucketUUID(),
                                                state.get(response.partition()).startSequenceNumber()));
                                    }
                                });
                    }
                });
    }

    private Observable<DCPConnection> open() {
        if (connection.get() == null) {
            return core.<OpenConnectionResponse>send(new OpenConnectionRequest(name, bucket))
                    .flatMap(new Func1<OpenConnectionResponse, Observable<DCPConnection>>() {
                        @Override
                        public Observable<DCPConnection> call(final OpenConnectionResponse response) {
                            connection.compareAndSet(null, response.connection());
                            return Observable.just(connection.get());
                        }
                    });
        }
        return Observable.just(connection.get());
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
}
