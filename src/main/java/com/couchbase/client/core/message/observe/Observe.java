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

package com.couchbase.client.core.message.observe;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.kv.ObserveRequest;
import com.couchbase.client.core.message.kv.ObserveResponse;
import com.couchbase.client.core.time.Delay;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to handle observe calls and polling logic.
 *
 * @author Michael Nitschinger
 * @since 1.0.1
 */
public class Observe {

    /**
     * The default observe delay for backwards compatibility.
     */
    private static final Delay DEFAULT_DELAY = Delay.fixed(10, TimeUnit.MILLISECONDS);

    /**
     * Defines the possible disk persistence constraints to observe.
     *
     * @author Michael Nitschinger
     * @since 1.0.1
     */
    public static enum PersistTo {
        /**
         * Observe disk persistence to the master node of the document only.
         */
        MASTER((short) -1),

        /**
         * Do not observe any disk persistence constraint.
         */
        NONE((short) 0),

        /**
         * Observe disk persistence of one node (master or replica).
         */
        ONE((short) 1),

        /**
         * Observe disk persistence of two nodes (master or replica).
         */
        TWO((short) 2),

        /**
         * Observe disk persistence of three nodes (master or replica).
         */
        THREE((short) 3),

        /**
         * Observe disk persistence of four nodes (one master and three replicas).
         */
        FOUR((short) 4);

        /**
         * Contains the internal value to map onto.
         */
        private final short value;

        /**
         * Internal constructor for the enum.
         *
         * @param value the value of the persistence constraint.
         */
        PersistTo(short value) {
            this.value = value;
        }

        /**
         * Returns the actual internal persistence representation for the enum.
         *
         * @return the internal persistence representation.
         */
        public short value() {
            return value;
        }

        /**
         * Identifies if this enum property will touch a replica or just the master.
         *
         * @return true if it includes a replica, false if not.
         */
        public boolean touchesReplica() {
            return value > 0;
        }
    }

    /**
     * Defines the possible replication constraints to observe.
     *
     * @author Michael Nitschinger
     * @since 1.0.1
     */
    public static enum ReplicateTo {

        /**
         * Do not observe any replication constraint.
         */
        NONE((short) 0),

        /**
         * Observe replication to one replica.
         */
        ONE((short) 1),

        /**
         * Observe replication to two replicas.
         */
        TWO((short) 2),

        /**
         * Observe replication to three replicas.
         */
        THREE((short) 3);

        /**
         * Contains the internal value to map onto.
         */
        private final short value;

        /**
         * Internal constructor for the enum.
         *
         * @param value the value of the replication constraint.
         */
        ReplicateTo(short value) {
            this.value = value;
        }

        /**
         * Returns the actual internal replication representation for the enum.
         *
         * @return the internal replication representation.
         */
        public short value() {
            return value;
        }

        /**
         * Identifies if this enum property will touch a replica or just the master.
         *
         * @return true if it includes a replica, false if not.
         */
        public boolean touchesReplica() {
            return value > 0;
        }
    }

    public static Observable<Boolean> call(final ClusterFacade core, final String bucket, final String id,
        final long cas, final boolean remove, final PersistTo persistTo, final ReplicateTo replicateTo) {
        return call(core, bucket, id, cas, remove, persistTo, replicateTo, DEFAULT_DELAY);
    }

    public static Observable<Boolean> call(final ClusterFacade core, final String bucket, final String id,
        final long cas, final boolean remove, final PersistTo persistTo, final ReplicateTo replicateTo,
        final Delay delay) {

        final ObserveResponse.ObserveStatus persistIdentifier;
        final ObserveResponse.ObserveStatus replicaIdentifier;
        if (remove) {
            persistIdentifier = ObserveResponse.ObserveStatus.NOT_FOUND_PERSISTED;
            replicaIdentifier = ObserveResponse.ObserveStatus.NOT_FOUND_NOT_PERSISTED;
        } else {
            persistIdentifier = ObserveResponse.ObserveStatus.FOUND_PERSISTED;
            replicaIdentifier = ObserveResponse.ObserveStatus.FOUND_NOT_PERSISTED;
        }

        Observable<ObserveResponse> observeResponses = sendObserveRequests(core, bucket, id, cas, persistTo,
            replicateTo);

        return observeResponses
                .toList()
                .repeatWhen(new Func1<Observable<? extends Void>, Observable<?>>() {
                    @Override
                    public Observable<?> call(Observable<? extends Void> observable) {
                        return observable.zipWith(
                            Observable.range(1, Integer.MAX_VALUE),
                            new Func2<Void, Integer, Integer>() {
                                @Override
                                public Integer call(Void aVoid, Integer attempt) {
                                    return attempt;
                                }
                            }
                        )
                        .flatMap(new Func1<Integer, Observable<?>>() {
                            @Override
                            public Observable<?> call(Integer attempt) {
                                return Observable.timer(delay.calculate(attempt), delay.unit());
                            }
                        });
                    }
                })
                .skipWhile(new Func1<List<ObserveResponse>, Boolean>() {
                    @Override
                    public Boolean call(List<ObserveResponse> observeResponses) {
                        int replicated = 0;
                        int persisted = 0;
                        boolean persistedMaster = false;
                        for (ObserveResponse response : observeResponses) {
                            if (response.content() != null && response.content().refCnt() > 0) {
                                response.content().release();
                            }
                            ObserveResponse.ObserveStatus status = response.observeStatus();
                            if (response.master()) {
                                if (status == persistIdentifier) {
                                    persisted++;
                                    persistedMaster = true;
                                }
                            } else {
                                if (status == persistIdentifier) {
                                    persisted++;
                                    replicated++;
                                } else if (status == replicaIdentifier) {
                                    replicated++;
                                }
                            }
                        }

                        boolean persistDone = false;
                        boolean replicateDone = false;

                        if (persistTo == PersistTo.MASTER && persistedMaster) {
                            persistDone = true;
                        } else if (persisted >= persistTo.value()) {
                            persistDone = true;
                        }

                        if (replicated >= replicateTo.value()) {
                            replicateDone = true;
                        }

                        return !(persistDone && replicateDone);
                    }
                })
                .take(1)
                .map(new Func1<List<ObserveResponse>, Boolean>() {
                    @Override
                    public Boolean call(List<ObserveResponse> observeResponses) {
                        return true;
                    }
                });
    }

    private static Observable<ObserveResponse> sendObserveRequests(final ClusterFacade core, final String bucket,
        final String id, final long cas, final PersistTo persistTo, final ReplicateTo replicateTo) {
        return Observable.defer(new Func0<Observable<ObserveResponse>>() {
            @Override
            public Observable<ObserveResponse> call() {
                return core
                        .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                        .map(new Func1<GetClusterConfigResponse, Integer>() {
                            @Override
                            public Integer call(GetClusterConfigResponse response) {
                                CouchbaseBucketConfig conf =
                                    (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                                return conf.numberOfReplicas();
                            }
                        })
                        .flatMap(new Func1<Integer, Observable<ObserveResponse>>() {
                            @Override
                            public Observable<ObserveResponse> call(Integer replicas) {
                                List<Observable<ObserveResponse>> obs = new ArrayList<Observable<ObserveResponse>>();
                                if (persistTo != PersistTo.NONE) {
                                    obs.add(core.<ObserveResponse>send(new ObserveRequest(id, cas, true, (short) 0, bucket)));
                                }

                                if (persistTo.touchesReplica() || replicateTo.touchesReplica()) {
                                    if (replicas >= 1) {
                                        obs.add(core.<ObserveResponse>send(new ObserveRequest(id, cas, false, (short) 1, bucket)));
                                    }
                                    if (replicas >= 2) {
                                        obs.add(core.<ObserveResponse>send(new ObserveRequest(id, cas, false, (short) 2, bucket)));
                                    }
                                    if (replicas == 3) {
                                        obs.add(core.<ObserveResponse>send(new ObserveRequest(id, cas, false, (short) 3, bucket)));
                                    }
                                }
                                return Observable.merge(obs);
                            }
                        });
            }
        });
    }
}
