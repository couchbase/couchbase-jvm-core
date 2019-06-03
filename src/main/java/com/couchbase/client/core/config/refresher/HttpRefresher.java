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
import com.couchbase.client.core.config.loader.HttpLoader;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.config.BucketStreamingResponse;
import com.couchbase.client.core.service.ServiceType;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    private final CoreEnvironment environment;

    /**
     * Stores an ever-incrementing offset that gets used to always pick a different relative node
     * to poll.
     */
    private final AtomicLong nodeOffset = new AtomicLong(0);

    /**
     * Stores the nanoTime for the last poll time instant.
     */
    private final Map<String, Long> lastPollTimestamps = new ConcurrentHashMap<String, Long>();

    /**
     * The minimum poll interval in nanoseconds.
     */
    private final long pollFloorNs;

    public HttpRefresher(final CoreEnvironment env, final ClusterFacade cluster) {
        super(env, cluster);
        this.environment = env;
        this.pollFloorNs = TimeUnit.MILLISECONDS.toNanos(environment.configPollFloorInterval());
    }

    @Override
    public Observable<Boolean> registerBucket(final String name, final String password) {
        return registerBucket(name, name, password);
    }

        @Override
    public Observable<Boolean> registerBucket(final String name, final String username, final String password) {
        Observable<BucketStreamingResponse> response = super
            .registerBucket(name, username, password)
            .flatMap(new Func1<Boolean, Observable<BucketStreamingResponse>>() {
                @Override
                public Observable<BucketStreamingResponse> call(Boolean aBoolean) {
                    return cluster()
                        .<BucketStreamingResponse>send(new BucketStreamingRequest(TERSE_PATH, name, username, password))
                        .doOnNext(new Action1<BucketStreamingResponse>() {
                            @Override
                            public void call(BucketStreamingResponse response) {
                                if (response.status() == ResponseStatus.NOT_EXISTS) {
                                    throw new TerseConfigDoesNotExistException();
                                } else if (!response.status().isSuccess()) {
                                    throw new ConfigurationException("Could not load terse config.");
                                }
                            }
                        });
                }
            })
            .onErrorResumeNext(new Func1<Throwable, Observable<BucketStreamingResponse>>() {
                @Override
                public Observable<BucketStreamingResponse> call(Throwable throwable) {
                    if (throwable instanceof TerseConfigDoesNotExistException) {
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
                    } else {
                        return Observable.error(throwable);
                    }
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
                            return new ProposedBucketConfigContext(name, raw, response.host());
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
        }).subscribe(new Subscriber<ProposedBucketConfigContext>() {
            @Override
            public void onCompleted() {
                // ignored on purpose
            }

            @Override
            public void onError(Throwable e) {
                LOGGER.error("Error while subscribing to Http refresh stream!", e);
            }

            @Override
            public void onNext(ProposedBucketConfigContext ctx) {
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
            }).subscribe(new Action1<BucketConfig>() {
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
    private Observable<ProposedBucketConfigContext> buildRefreshFallbackSequence(final List<NodeInfo> nodeInfos,
        final String bucketName) {
        Observable<ProposedBucketConfigContext> failbackSequence = null;
        for (final NodeInfo nodeInfo : nodeInfos) {
            if (!isValidConfigNode(environment.sslEnabled(), nodeInfo)) {
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
     * Helper method to fetch a config from a specific node of the cluster.
     *
     * @param bucketName the name of the bucket.
     * @param hostname the hostname of the node to fetch from.
     * @return a raw configuration or an error.
     */
    private Observable<ProposedBucketConfigContext> refreshAgainstNode(final String bucketName,
        final String hostname) {
        final Credential credential = registrations().get(bucketName);
        if (credential == null) {
            LOGGER.debug("Ignoring refresh attempt since it seems the bucket registration is gone (closed).");
            return Observable.empty();
        }

        return Observable.defer(new Func0<Observable<BucketConfigResponse>>() {
            @Override
            public Observable<BucketConfigResponse> call() {
                return cluster()
                    .<BucketConfigResponse>send(new BucketConfigRequest(HttpLoader.TERSE_PATH, hostname, bucketName,
                        credential.username(), credential.password()))
                    .flatMap(new Func1<BucketConfigResponse, Observable<BucketConfigResponse>>() {
                        @Override
                        public Observable<BucketConfigResponse> call(BucketConfigResponse response) {
                            if (response.status().isSuccess()) {
                                LOGGER.debug("Successfully got config refresh from terse bucket remote.");
                                return Observable.just(response);
                            }

                            LOGGER.debug("Terse bucket config refresh failed, falling back to verbose.");
                            return cluster().send(
                                new BucketConfigRequest(HttpLoader.VERBOSE_PATH, hostname, bucketName,
                                    credential.username(), credential.password()));
                        }
                    });
            }
        }).map(new Func1<BucketConfigResponse, ProposedBucketConfigContext>() {
            @Override
            public ProposedBucketConfigContext call(final BucketConfigResponse response) {
                String config = response.config().replace("$HOST", hostname);
                return new ProposedBucketConfigContext(bucketName, config, hostname);
            }
        }).doOnError(new Action1<Throwable>() {
            @Override
            public void call(Throwable ex) {
                LOGGER.debug("Could not fetch config from bucket \"" + bucketName + "\" against \""
                    + hostname + "\".", ex);
            }
        });
    }

    /**
     * Helper method to detect if the given node can actually perform http config refresh.
     *
     * @param sslEnabled true if ssl enabled, false otherwise.
     * @param nodeInfo the node info for the given node.
     * @return true if it is a valid config node, false otherwise.
     */
    private static boolean isValidConfigNode(final boolean sslEnabled, final NodeInfo nodeInfo) {
        if (sslEnabled && nodeInfo.sslServices().containsKey(ServiceType.CONFIG)) {
            return true;
        } else if (nodeInfo.services().containsKey(ServiceType.CONFIG)) {
            return true;
        }
        return false;
    }


    /**
     * Helper method to transparently rearrange the node list based on the current global offset.
     *
     * @param nodeList the list to shift.
     */
    private <T> void shiftNodeList(List<T> nodeList) {
        int shiftBy = (int) (nodeOffset.getAndIncrement() % nodeList.size());
        for(int i = 0; i < shiftBy; i++) {
            T element = nodeList.remove(0);
            nodeList.add(element);
        }
    }

    /**
     * Returns true if polling is allowed, false if we are below the configured floor poll interval.
     */
    private boolean allowedToPoll(final String bucket) {
        Long bucketLastPollTimestamp = lastPollTimestamps.get(bucket);
        return bucketLastPollTimestamp == null || ((System.nanoTime() - bucketLastPollTimestamp) >= pollFloorNs);
    }

    class TerseConfigDoesNotExistException extends ConfigurationException {

    }
}
