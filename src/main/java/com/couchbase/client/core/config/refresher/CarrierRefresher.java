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
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import com.couchbase.client.core.utils.Buffers;
import io.netty.util.CharsetUtil;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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

    private final Set<String> subscriptions;
    private final CoreEnvironment environment;

    /**
     * Creates a new {@link CarrierRefresher}.
     *
     * @param environment the environment to use.
     * @param cluster the cluster reference.
     */
    public CarrierRefresher(final CoreEnvironment environment, final ClusterFacade cluster) {
        super(cluster);
        subscriptions = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        this.environment = environment;
    }

    @Override
    public Observable<Boolean> shutdown() {
        return Observable.just(true);
    }

    @Override
    public void markTainted(final BucketConfig config) {
        final String bucketName = config.name();
        if (subscriptions.contains(bucketName)) {
            return;
        }

        LOGGER.debug("Config for bucket \"" + bucketName + "\" marked as tainted, starting polling.");
        subscriptions.add(bucketName);
        Observable<Long> pollSequence = Observable
            .interval(1, TimeUnit.SECONDS)
            .takeWhile(new Func1<Long, Boolean>() {
                @Override
                public Boolean call(Long aLong) {
                    return subscriptions.contains(bucketName);
                }
            });


        Observable<String> refreshSequence = null;
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>(config.nodes());
        Collections.shuffle(nodeInfos);
        for (final NodeInfo nodeInfo : nodeInfos) {
            if (refreshSequence == null) {
                refreshSequence = pollSequence.flatMap(new Func1<Long, Observable<String>>() {
                    @Override
                    public Observable<String> call(Long aLong) {
                        return refreshAgainstNode(bucketName, nodeInfo.hostname());
                    }
                });
            } else {
                refreshSequence = refreshSequence.onErrorResumeNext(
                    refreshAgainstNode(bucketName, nodeInfo.hostname())
                );
            }
        }

        if (refreshSequence == null) {
            LOGGER.debug("Cannot poll bucket, because node list contains no nodes.");
            return;
        }

        refreshSequence.subscribe(new Subscriber<String>() {
            @Override
            public void onCompleted() {
                LOGGER.debug("Completed polling for bucket \"{}\".", bucketName);
            }

            @Override
            public void onError(Throwable e) {
                LOGGER.debug("Error while polling bucket config, ignoring.", e);
            }

            @Override
            public void onNext(String rawConfig) {
                if (rawConfig.startsWith("{")) {
                    provider().proposeBucketConfig(bucketName, rawConfig);
                }
            }
        });
    }

    @Override
    public void markUntainted(final BucketConfig config) {
        if (subscriptions.contains(config.name())) {
            LOGGER.debug("Config for bucket \"" + config.name() + "\" marked as untainted, stopping polling.");
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
                    final String bucketName = config.name();
                    Observable<String> refreshSequence = null;

                    List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>(config.nodes());
                    Collections.shuffle(nodeInfos);
                    for (NodeInfo nodeInfo : nodeInfos) {
                        if (refreshSequence == null) {
                            refreshSequence = refreshAgainstNode(bucketName, nodeInfo.hostname());
                        } else {
                            refreshSequence = refreshSequence
                                .onErrorResumeNext(refreshAgainstNode(bucketName, nodeInfo.hostname()));
                        }
                    }

                    if (refreshSequence == null) {
                        LOGGER.debug("No node registered in the current configuration, skipping to refresh.");
                        return;
                    }

                    refreshSequence.subscribe(new Subscriber<String>() {
                        @Override
                        public void onCompleted() {
                            LOGGER.debug("Completed refreshing config for bucket \"{}\"", bucketName);
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

    /**
     * Helper method to fetch a config from a specific node of the cluster.
     *
     * @param bucketName the name of the bucket.
     * @param hostname the hostname of the node to fetch from.
     * @return a raw configuration or an error.
     */
    private Observable<String> refreshAgainstNode(final String bucketName, final InetAddress hostname) {
        return Buffers.wrapColdWithAutoRelease(
            cluster().<GetBucketConfigResponse>send(new GetBucketConfigRequest(bucketName, hostname))
        )
        .doOnNext(new Action1<GetBucketConfigResponse>() {
            @Override
            public void call(GetBucketConfigResponse response) {
                if (!response.status().isSuccess()) {
                    if (response.content() != null && response.content().refCnt() > 0) {
                        response.content().release();
                    }
                    throw new ConfigurationException("Could not fetch config from node: " + response);
                }
            }
        })
        .map(new Func1<GetBucketConfigResponse, String>() {
            @Override
            public String call(GetBucketConfigResponse response) {
                String raw = response.content().toString(CharsetUtil.UTF_8).trim();
                if (response.content().refCnt() > 0) {
                    response.content().release();
                }
                return raw.replace("$HOST", response.hostname().getHostName());
            }
        })
        .doOnError(new Action1<Throwable>() {
            @Override
            public void call(Throwable ex) {
                LOGGER.debug("Could not fetch config from bucket \"" + bucketName + "\" against \""
                    + hostname + "\".", ex);
            }
        });
    }

}
