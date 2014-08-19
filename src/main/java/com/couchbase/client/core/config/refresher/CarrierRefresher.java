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
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.binary.GetBucketConfigRequest;
import com.couchbase.client.core.message.binary.GetBucketConfigResponse;
import io.netty.util.CharsetUtil;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

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

    public CarrierRefresher(final ClusterFacade cluster) {
        super(cluster);
        subscriptions = new ConcurrentHashMap<String, Subscription>();
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
                Subscription subscription = Schedulers.io().createWorker().schedulePeriodically(new Action0() {
                    @Override
                    public void call() {
                        GetBucketConfigRequest req = new GetBucketConfigRequest(config.name(), config.nodes().get(0).hostname());
                        cluster()
                            .<GetBucketConfigResponse>send(req)
                            .subscribe(new Action1<GetBucketConfigResponse>() {
                                @Override
                                public void call(GetBucketConfigResponse res) {
                                    provider().proposeBucketConfig(res.bucket(), res.content().toString(CharsetUtil.UTF_8));
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
            .flatMap(new Func1<BucketConfig, Observable<GetBucketConfigResponse>>() {
                @Override
                public Observable<GetBucketConfigResponse> call(BucketConfig config) {
                    GetBucketConfigRequest req = new GetBucketConfigRequest(config.name(), config.nodes().get(0).hostname());
                    return cluster().send(req);
                }
            }).subscribe(new Action1<GetBucketConfigResponse>() {
                @Override
                public void call(GetBucketConfigResponse res) {
                    provider().proposeBucketConfig(res.bucket(), res.content().toString(CharsetUtil.UTF_8));
                }
             });
    }
}
