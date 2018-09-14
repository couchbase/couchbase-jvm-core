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
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.Buffers;
import com.couchbase.client.core.utils.NetworkAddress;
import io.netty.util.CharsetUtil;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    private volatile Subscription pollerSubscription;

    /**
     * Stores an ever-incrementing offset that gets used to always pick a different relative node
     * to poll.
     */
    private volatile long nodeOffset;

    /**
     * Stores the nanoTime for the last poll time instant.
     */
    private final Map<String, Long> lastPollTimestamps;

    /**
     * The minimum poll interval in nanoseconds.
     */
    private final long pollFloorNs;

    /**
     * Creates a new {@link CarrierRefresher}.
     *
     * @param environment the environment to use.
     * @param cluster the cluster reference.
     */
    public CarrierRefresher(final CoreEnvironment environment, final ClusterFacade cluster) {
        super(environment, cluster);
        subscriptions = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        this.environment = environment;
        this.lastPollTimestamps = new ConcurrentHashMap<String, Long>();
        this.nodeOffset = 0;
        this.pollFloorNs = TimeUnit.MILLISECONDS.toNanos(environment.configPollFloorInterval());

        long pollInterval = environment.configPollInterval();
        if (pollInterval > 0) {
            LOGGER.debug("Starting polling with interval {}ms", pollInterval);
            pollerSubscription = Observable
                    .interval(pollInterval, TimeUnit.MILLISECONDS, environment.scheduler())
                    .subscribe(new Action1<Long>() {
                        @Override
                        public void call(Long aLong) {
                            if (provider() != null) {
                                ClusterConfig config = provider().config();
                                if (config != null && !config.bucketConfigs().isEmpty()) {
                                    refresh(config);
                                } else {
                                    LOGGER.debug("No bucket open to refresh, ignoring outdated signal.");
                                }
                            } else {
                                LOGGER.debug("Provider not yet wired up, ignoring outdated signal.");
                            }
                        }
                    });
        } else {
            LOGGER.info("Proactive config polling disabled based on environment setting.");
        }
    }

    @Override
    public Observable<Boolean> shutdown() {
        LOGGER.debug("Shutting down the CarrierRefresher.");
        return Observable
            .just(true)
            .doOnNext(new Action1<Boolean>() {
                @Override
                public void call(Boolean ignored) {
                    if (pollerSubscription != null && !pollerSubscription.isUnsubscribed()) {
                        pollerSubscription.unsubscribe();
                    }
                }
            });
    }

    @Override
    public void markTainted(final BucketConfig config) {
        final String bucketName = config.name();
        if (subscriptions.contains(bucketName)) {
            return;
        }

        LOGGER.debug("Config for bucket \"" + bucketName + "\" marked as tainted, starting polling.");
        subscriptions.add(bucketName);
        Observable
            .interval(1, TimeUnit.SECONDS)
            .takeWhile(new Func1<Long, Boolean>() {
                @Override
                public Boolean call(Long aLong) {
                    return subscriptions.contains(bucketName);
                }
            })
            .filter(new Func1<Long, Boolean>() {
                @Override
                public Boolean call(Long aLong) {
                    boolean allowed = allowedToPoll(bucketName);
                    if (allowed) {
                        lastPollTimestamps.put(bucketName, System.nanoTime());
                    } else {
                        LOGGER.trace("Ignoring tainted polling attempt because poll interval is too small.");
                    }
                    return allowed;
                }
            }).flatMap(new Func1<Long, Observable<ProposedBucketConfigContext>>() {
                @Override
                public Observable<ProposedBucketConfigContext> call(Long aLong) {
                    List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>(config.nodes());
                    if (nodeInfos.isEmpty()) {
                        LOGGER.debug("Cannot poll bucket, because node list contains no nodes.");
                        return Observable.empty();
                    }
                    shiftNodeList(nodeInfos);
                    return buildRefreshFallbackSequence(nodeInfos, bucketName);
                }
            }).subscribe(new Subscriber<ProposedBucketConfigContext>() {
                @Override
                public void onCompleted() {
                    LOGGER.debug("Completed polling for bucket \"{}\".", bucketName);
                }

                @Override
                public void onError(Throwable e) {
                    LOGGER.debug("Error while polling bucket config, ignoring.", e);
                }

                @Override
                public void onNext(ProposedBucketConfigContext ctx) {
                    if (ctx.config().startsWith("{")) {
                        provider().proposeBucketConfig(ctx);
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
            .filter(new Func1<BucketConfig, Boolean>() {
                @Override
                public Boolean call(BucketConfig config) {
                    String bucketName = config.name();
                    boolean allowed = allowedToPoll(bucketName);
                    if (allowed) {
                        lastPollTimestamps.put(bucketName, System.nanoTime());
                    } else {
                        LOGGER.trace("Ignoring refresh polling attempt because poll interval is too small.");
                    }
                    return allowed;
                }
            })
            .subscribe(new Action1<BucketConfig>() {
                @Override
                public void call(final BucketConfig config) {
                    final String bucketName = config.name();
                    List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>(config.nodes());
                    if (nodeInfos.isEmpty()) {
                        LOGGER.debug("Cannot refresh bucket, because node list contains no nodes.");
                        return;
                    }
                    shiftNodeList(nodeInfos);
                    Observable<ProposedBucketConfigContext> refreshSequence = buildRefreshFallbackSequence(nodeInfos, bucketName);
                    refreshSequence.subscribe(new Subscriber<ProposedBucketConfigContext>() {
                        @Override
                        public void onCompleted() {
                            LOGGER.debug("Completed refreshing config for bucket \"{}\"", bucketName);
                        }

                        @Override
                        public void onError(Throwable e) {
                            LOGGER.debug("Error while refreshing bucket config, ignoring.", e);
                        }

                        @Override
                        public void onNext(ProposedBucketConfigContext ctx) {
                            if (ctx.config().startsWith("{")) {
                                provider().proposeBucketConfig(ctx);
                            }
                        }
                    });
                }
            });
    }

    /**
     * Helper method which builds the refresh fallback sequence based on the node list.
     *
     * @param nodeInfos the list of node infos.
     * @param bucketName the name of the bucket.
     * @return an observable containing flatMapped failback sequences.
     */
    private Observable<ProposedBucketConfigContext> buildRefreshFallbackSequence(List<NodeInfo> nodeInfos, String bucketName) {
        Observable<ProposedBucketConfigContext> failbackSequence = null;
        for (final NodeInfo nodeInfo : nodeInfos) {
            if (!isValidCarrierNode(environment.sslEnabled(), nodeInfo)) {
                continue;
            }

            if (failbackSequence == null) {
                failbackSequence = refreshAgainstNode(bucketName, nodeInfo.hostname());
            } else {
                failbackSequence = failbackSequence.onErrorResumeNext(
                        refreshAgainstNode(bucketName, nodeInfo.hostname())
                );
            }
        }
        if (failbackSequence == null) {
            LOGGER.debug("Could not build refresh sequence, node list is empty - ignoring attempt.");
            return Observable.empty();
        }
        return failbackSequence;
    }

     /**
      * Helper method to transparently rearrange the node list based on the current global offset.
      *
      * @param nodeList the list to shift.
      */
     <T> void shiftNodeList(List<T> nodeList) {
         int shiftBy = (int) (nodeOffset++ % nodeList.size());
         for(int i = 0; i < shiftBy; i++) {
             T element = nodeList.remove(0);
             nodeList.add(element);
         }
     }

    /**
     * Helper method to detect if the given node can actually perform carrier refresh.
     *
     * @param sslEnabled true if ssl enabled, false otherwise.
     * @param nodeInfo the node info for the given node.
     * @return true if it is a valid carrier node, false otherwise.
     */
    private static boolean isValidCarrierNode(final boolean sslEnabled, final NodeInfo nodeInfo) {
        if (sslEnabled && nodeInfo.sslServices().containsKey(ServiceType.BINARY)) {
            return true;
        } else if (nodeInfo.services().containsKey(ServiceType.BINARY)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true if polling is allowed, false if we are below the configured floor poll interval.
     */
    private boolean allowedToPoll(final String bucket) {
        Long bucketLastPollTimestamp = lastPollTimestamps.get(bucket);
        return bucketLastPollTimestamp == null || ((System.nanoTime() - bucketLastPollTimestamp) >= pollFloorNs);
    }

    /**
     * Helper method to fetch a config from a specific node of the cluster.
     *
     * @param bucketName the name of the bucket.
     * @param hostname the hostname of the node to fetch from.
     * @return a raw configuration or an error.
     */
    private Observable<ProposedBucketConfigContext> refreshAgainstNode(final String bucketName, final NetworkAddress hostname) {
        return Buffers.wrapColdWithAutoRelease(Observable.defer(new Func0<Observable<GetBucketConfigResponse>>() {
            @Override
            public Observable<GetBucketConfigResponse> call() {
                return cluster().send(new GetBucketConfigRequest(bucketName, hostname));
            }
        }))
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
        .map(new Func1<GetBucketConfigResponse, ProposedBucketConfigContext>() {
            @Override
            public ProposedBucketConfigContext call(GetBucketConfigResponse response) {
                String raw = response.content().toString(CharsetUtil.UTF_8).trim();
                if (response.content().refCnt() > 0) {
                    response.content().release();
                }

                raw = raw.replace("$HOST", response.hostname().address());
                return new ProposedBucketConfigContext(bucketName, raw, hostname);
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

    /**
     * Helper method to inspect the poller subscription state in tests.
     */
    Subscription pollerSubscription() {
        return pollerSubscription;
    }

}
