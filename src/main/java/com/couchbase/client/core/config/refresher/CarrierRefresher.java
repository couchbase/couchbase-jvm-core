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
package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Keeps the bucket config fresh through carrier configuration management.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CarrierRefresher extends AbstractRefresher {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CarrierRefresher.class);

    private final Map<String, Subscription> subscriptions;
    private final CoreEnvironment environment;

    /**
     * Creates a new {@link CarrierRefresher}.
     *
     * @param environment the environment to use.
     * @param cluster the cluster reference.
     */
    public CarrierRefresher(final CoreEnvironment environment, final ClusterFacade cluster) {
        super(cluster);
        subscriptions = new ConcurrentHashMap<String, Subscription>();
        this.environment = environment;
    }

    @Override
    public Observable<Boolean> shutdown() {
        return Observable.just(true);
    }

    @Override
    public void markTainted(final BucketConfig config) {
        if (subscriptions.containsKey(config.name())) {
            return;
        }

        LOGGER.debug("Config for bucket \"" + config.name() + "\" marked as tainted, starting polling.");
        Observable.create(new Observable.OnSubscribe<Object>() {
            @Override
            public void call(final Subscriber<? super Object> subscriber) {
                Subscription subscription = environment.scheduler().createWorker().schedulePeriodically(new Action0() {
                    @Override
                    public void call() {
                        final InetAddress hostname = config.nodes().get(0).hostname();
                        cluster()
                            .<GetBucketConfigResponse>send(new GetBucketConfigRequest(config.name(), hostname))
                            .subscribe(new Subscriber<GetBucketConfigResponse>() {
                                @Override
                                public void onCompleted() {

                                }

                                @Override
                                public void onError(Throwable e) {
                                    LOGGER.debug("Error while loading tainted config, ignoring", e);
                                }

                                @Override
                                public void onNext(GetBucketConfigResponse res) {
                                    ByteBuf content = res.content();
                                    if (res.status().isSuccess() && content!= null && content.readableBytes() > 0) {
                                        String rawConfig = content
                                            .toString(CharsetUtil.UTF_8)
                                            .replace("$HOST", hostname.getHostName())
                                            .trim();
                                        if (rawConfig.startsWith("{")) {
                                            provider().proposeBucketConfig(res.bucket(), rawConfig);
                                        }
                                    }
                                    if (content != null && content.refCnt() > 0) {
                                        content.release();
                                    }
                                }
                            });
                    }
                }, 0, 1, TimeUnit.SECONDS);

                subscriptions.put(config.name(), subscription);
            }
        }).subscribe();
    }

    @Override
    public void markUntainted(final BucketConfig config) {
        Subscription subscription = subscriptions.get(config.name());
        if (subscription != null) {
            LOGGER.debug("Config for bucket \"" + config.name() + "\" marked as untainted, stopping polling.");
            subscription.unsubscribe();
            subscriptions.remove(config.name());
        }
    }

    @Override
    public void refresh(final ClusterConfig config) {
        Observable
            .from(config.bucketConfigs().values())
            .observeOn(environment.scheduler())
            .filter(new Func1<BucketConfig, Boolean>() {
                @Override
                public Boolean call(BucketConfig config) {
                    return registrations().containsKey(config.name());
                }
            })
            .subscribe(new Action1<BucketConfig>() {
                @Override
                public void call(final BucketConfig config) {
                    InetAddress hostname = config.nodes().get(0).hostname();

                    cluster()
                        .<GetBucketConfigResponse>send(new GetBucketConfigRequest(config.name(), hostname))
                        .filter(new Func1<GetBucketConfigResponse, Boolean>() {
                            @Override
                            public Boolean call(GetBucketConfigResponse response) {
                                boolean good = response.status().isSuccess() && response.content() != null;
                                if (!good) {
                                    if (response.content() != null) {
                                        response.content().release();
                                    }
                                }
                                return good;
                            }
                        })
                        .map(new Func1<GetBucketConfigResponse, String>() {
                            @Override
                            public String call(GetBucketConfigResponse response) {
                                String raw = response.content().toString(CharsetUtil.UTF_8).trim();
                                response.content().release();
                                return raw.replace("$HOST", response.hostname().getHostName());
                            }
                        })
                        .subscribe(new Subscriber<String>() {
                            @Override
                            public void onCompleted() {

                            }

                            @Override
                            public void onError(Throwable e) {
                                LOGGER.debug("Error while refreshing bucket config, ignoring.", e);
                            }

                            @Override
                            public void onNext(String rawConfig) {
                                if (rawConfig.startsWith("{")) {
                                    provider().proposeBucketConfig(config.name(), rawConfig);
                                }
                            }
                        });
                }
            });
    }
}
