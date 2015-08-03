/**
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
package com.couchbase.client.core.message.observe;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.DocumentMutationLostException;
import com.couchbase.client.core.ReplicaNotConfiguredException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.kv.FailoverObserveSeqnoResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.NoFailoverObserveSeqnoResponse;
import com.couchbase.client.core.message.kv.ObserveSeqnoRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.time.Delay;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

import java.util.ArrayList;
import java.util.List;

/**
 * Document observe through mutation token information.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
@InterfaceStability.Uncommitted
@InterfaceAudience.Private
public class ObserveViaMutationToken {

    public static Observable<Boolean> call(final ClusterFacade core, final String bucket, final String id,
       final MutationToken token, final Observe.PersistTo persistTo,
       final Observe.ReplicateTo replicateTo, final Delay delay, final RetryStrategy retryStrategy) {


        Observable<CouchbaseResponse> observeResponses = sendObserveRequests(core, bucket, id, token, persistTo,
                replicateTo, retryStrategy);

        return observeResponses
                .map(new Func1<CouchbaseResponse, ObserveItem>() {
                    @Override
                    public ObserveItem call(CouchbaseResponse response) {
                        if (response instanceof FailoverObserveSeqnoResponse) {
                            FailoverObserveSeqnoResponse fr = (FailoverObserveSeqnoResponse) response;
                            if (fr.lastSeqNoReceived() < token.sequenceNumber()) {
                                throw new DocumentMutationLostException("Document Mutation lost during a hard failover.");
                            }
                            return ObserveItem.from(token, fr);
                        } else if (response instanceof NoFailoverObserveSeqnoResponse) {
                            return ObserveItem.from(token, (NoFailoverObserveSeqnoResponse) response);
                        } else {
                            throw new IllegalStateException("Unknown failover observe response: " + response);
                        }
                    }
                })
                .scan(ObserveItem.empty(), new Func2<ObserveItem, ObserveItem, ObserveItem>() {
                    @Override
                    public ObserveItem call(ObserveItem currentStatus, ObserveItem newStatus) {
                        return currentStatus.add(newStatus);
                    }
                })
                //repetitions will occur unless errors are raised
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
                        ).flatMap(new Func1<Integer, Observable<?>>() {
                            @Override
                            public Observable<?> call(Integer attempt) {
                                return Observable.timer(delay.calculate(attempt), delay.unit());
                            }
                        });
                    }
                })
                //ignore intermediate states as long as they don't match the criteria
                .skipWhile(new Func1<ObserveItem, Boolean>() {
                    @Override
                    public Boolean call(ObserveItem status) {
                        return !status.check(persistTo, replicateTo);
                    }
                })
                //finish as soon as the first poll that matches the whole criteria is encountered
                .take(1)
                .map(new Func1<ObserveItem, Boolean>() {
                    @Override
                    public Boolean call(ObserveItem observeResponses) {
                        return true;
                    }
                });
    }

    private static Observable<CouchbaseResponse> sendObserveRequests(final ClusterFacade core, final String bucket, final String id,
        final MutationToken token, final Observe.PersistTo persistTo, final Observe.ReplicateTo replicateTo,
        RetryStrategy retryStrategy) {
        final boolean swallowErrors = retryStrategy.shouldRetryObserve();
        return Observable.defer(new Func0<Observable<CouchbaseResponse>>() {
            @Override
            public Observable<CouchbaseResponse> call() {
                return core
                        .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                        .map(new Func1<GetClusterConfigResponse, Integer>() {
                            @Override
                            public Integer call(GetClusterConfigResponse response) {
                                CouchbaseBucketConfig conf =
                                        (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                                int numReplicas = conf.numberOfReplicas();

                                if (replicateTo.touchesReplica() && replicateTo.value() > numReplicas) {
                                    throw new ReplicaNotConfiguredException("Not enough replicas configured on " +
                                            "the bucket.");
                                }
                                if (persistTo.touchesReplica() && persistTo.value() - 1 > numReplicas) {
                                    throw new ReplicaNotConfiguredException("Not enough replicas configured on " +
                                            "the bucket.");
                                }
                                return numReplicas;
                            }
                        })
                        .flatMap(new Func1<Integer, Observable<CouchbaseResponse>>() {
                            @Override
                            public Observable<CouchbaseResponse> call(Integer replicas) {
                                List<Observable<CouchbaseResponse>> obs = new ArrayList<Observable<CouchbaseResponse>>();
                                Observable<CouchbaseResponse> masterRes = core.send(new ObserveSeqnoRequest(token.vbucketUUID(), true, (short) 0, id, bucket));
                                if (swallowErrors) {
                                    obs.add(masterRes.onErrorResumeNext(Observable.<CouchbaseResponse>empty()));
                                } else {
                                    obs.add(masterRes);
                                }

                                if (persistTo.touchesReplica() || replicateTo.touchesReplica()) {
                                    for (short i = 1; i <= replicas; i++) {
                                        Observable<CouchbaseResponse> res = core.send(new ObserveSeqnoRequest(token.vbucketUUID(), false, i, id, bucket));
                                        if (swallowErrors) {
                                            obs.add(res.onErrorResumeNext(Observable.<CouchbaseResponse>empty()));
                                        } else {
                                            obs.add(res);
                                        }
                                    }
                                }

                                if (obs.size() == 1) {
                                    return obs.get(0);
                                } else {
                                    //mergeDelayErrors will give a chance to other nodes to respond (maybe with enough
                                    //responses for the whole poll to be considered a success)
                                    return Observable.mergeDelayError(Observable.from(obs));
                                }
                            }
                        });
            }
        });
    }

    static class ObserveItem {

        private final int replicated;
        private final int persisted;
        private final boolean persistedMaster;

        private ObserveItem(int replicated, int persisted, boolean persistedMaster) {
            this.replicated = replicated;
            this.persisted = persisted;
            this.persistedMaster = persistedMaster;
        }

        public static ObserveItem empty() {
            return new ObserveItem(0, 0, false);
        }

        public static ObserveItem from(MutationToken token, FailoverObserveSeqnoResponse response) {
            boolean replicated = response.currentSeqNo() >= token.sequenceNumber();
            boolean persisted = response.lastPersistedSeqNo() >= token.sequenceNumber();

            return new ObserveItem(
                    replicated && !response.master() ? 1 : 0,
                    persisted ? 1 : 0,
                    response.master() && persisted
            );
        }

        public static ObserveItem from(MutationToken token, NoFailoverObserveSeqnoResponse response) {
            boolean replicated = response.currentSeqNo() >= token.sequenceNumber();
            boolean persisted = response.lastPersistedSeqNo() >= token.sequenceNumber();

            return new ObserveItem(
                replicated && !response.master() ? 1 : 0,
                persisted ? 1 : 0,
                response.master() && persisted
            );
        }

        public ObserveItem add(ObserveItem other) {
            return new ObserveItem(
                this.replicated + other.replicated,
                this.persisted + other.persisted,
                this.persistedMaster || other.persistedMaster
            );
        }

        public boolean check(Observe.PersistTo persistTo, Observe.ReplicateTo replicateTo) {
            boolean persistDone = false;
            boolean replicateDone = false;

            if (persistTo == Observe.PersistTo.MASTER) {
                if (persistedMaster) {
                    persistDone = true;
                }
            } else if (persisted >= persistTo.value()) {
                persistDone = true;
            }

            if (replicated >= replicateTo.value()) {
                replicateDone = true;
            }

            return persistDone && replicateDone;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("persisted ").append(persisted);
            if (persistedMaster)
                sb.append(" (master)");
            sb.append(", replicated ").append(replicated);
            return sb.toString();
        }
    }


}
