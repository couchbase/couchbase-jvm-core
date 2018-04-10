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
package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.config.BucketStreamingResponse;
import com.couchbase.client.core.utils.NetworkAddress;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;

/**
 * Keeps the bucket config fresh through a HTTP streaming connection.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class HttpRefresher extends AbstractRefresher {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(HttpRefresher.class);

    private static final String TERSE_PATH = "/pools/default/bs/";
    private static final String VERBOSE_PATH = "/pools/default/bucketsStreaming/";

    public HttpRefresher(final CoreEnvironment env, final ClusterFacade cluster) {
        super(env, cluster);
    }

    @Override
    public Observable<Boolean> registerBucket(final String name, final String password) {
        return registerBucket(name, name, password);
    }

        @Override
    public Observable<Boolean> registerBucket(final String name, final String username, final String password) {
        Observable<BucketStreamingResponse> response = super.registerBucket(name, username, password).flatMap(new Func1<Boolean, Observable<BucketStreamingResponse>>() {
            @Override
            public Observable<BucketStreamingResponse> call(Boolean aBoolean) {
                return cluster()
                    .<BucketStreamingResponse>send(new BucketStreamingRequest(TERSE_PATH, name, username, password))
                    .doOnNext(new Action1<BucketStreamingResponse>() {
                        @Override
                        public void call(BucketStreamingResponse response) {
                            if (!response.status().isSuccess()) {
                                throw new ConfigurationException("Could not load terse config.");
                            }
                        }
                    });
            }
        }).onErrorResumeNext(new Func1<Throwable, Observable<BucketStreamingResponse>>() {
            @Override
            public Observable<BucketStreamingResponse> call(Throwable throwable) {
                return cluster()
                    .<BucketStreamingResponse>send(new BucketStreamingRequest(VERBOSE_PATH, name, username, password))
                    .doOnNext(new Action1<BucketStreamingResponse>() {
                        @Override
                        public void call(BucketStreamingResponse response) {
                            if (!response.status().isSuccess()) {
                                throw new ConfigurationException("Could not load terse config.");
                            }
                        }
                    });
            }
        });

        repeatConfigUntilUnsubscribed(name, response);

        return response.map(new Func1<BucketStreamingResponse, Boolean>() {
            @Override
            public Boolean call(BucketStreamingResponse response) {
                return response.status().isSuccess();
            }
        });
    }

    /**
     * Helper method to push configs until unsubscribed, even when a stream closes.
     *
     * @param name the name of the bucket.
     * @param response the response source observable to resubscribe if needed.
     */
    private void repeatConfigUntilUnsubscribed(final String name, Observable<BucketStreamingResponse> response) {
        response.flatMap(new Func1<BucketStreamingResponse, Observable<ProposedBucketConfigContext>>() {
            @Override
            public Observable<ProposedBucketConfigContext> call(final BucketStreamingResponse response) {
                LOGGER.debug("Config stream started for {} on {}.", name, response.host());

                return response
                    .configs()
                    .map(new Func1<String, ProposedBucketConfigContext>() {
                        @Override
                        public ProposedBucketConfigContext call(String s) {
                            String raw = s.replace("$HOST", response.host());
                            return new ProposedBucketConfigContext(name, raw, NetworkAddress.create(response.host()));
                        }
                    })
                    .doOnCompleted(new Action0() {
                        @Override
                        public void call() {
                            LOGGER.debug("Config stream ended for {} on {}.", name, response.host());
                        }
                    });
            }
        })
        .repeatWhen(new Func1<Observable<? extends Void>, Observable<?>>() {
            @Override
            public Observable<?> call(Observable<? extends Void> observable) {
                return observable.flatMap(new Func1<Void, Observable<?>>() {
                    @Override
                    public Observable<?> call(Void aVoid) {
                        if (HttpRefresher.this.registrations().containsKey(name)) {
                            LOGGER.debug("Resubscribing config stream for bucket {}, still registered.", name);
                            return Observable.just(true);
                        } else {
                            LOGGER.debug("Not resubscribing config stream for bucket {}, not registered.", name);
                            return Observable.empty();
                        }
                    }
                });
            }
        }).subscribe(new Action1<ProposedBucketConfigContext>() {
            @Override
            public void call(ProposedBucketConfigContext ctx) {
                pushConfig(ctx);
            }
        });
    }

    @Override
    public Observable<Boolean> shutdown() {
        return Observable.just(true);
    }

    @Override
    public void markTainted(BucketConfig config) {

    }

    @Override
    public void markUntainted(BucketConfig config) {

    }

    @Override
    public void refresh(ClusterConfig config) {
    }
}
