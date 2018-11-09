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
package com.couchbase.client.core.config;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.loader.CarrierLoader;
import com.couchbase.client.core.config.loader.HttpLoader;
import com.couchbase.client.core.config.loader.Loader;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.core.config.refresher.CarrierRefresher;
import com.couchbase.client.core.config.refresher.HttpRefresher;
import com.couchbase.client.core.config.refresher.Refresher;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.system.BucketClosedEvent;
import com.couchbase.client.core.event.system.BucketOpenedEvent;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.utils.DefaultObjectMapper;
import com.couchbase.client.core.utils.NetworkAddress;
import com.fasterxml.jackson.databind.JsonNode;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.meta;

/**
 * **The default implementation of a {@link ConfigurationProvider}.**
 *
 * <p>The {@link ConfigurationProvider} is the central orchestrator for configuration management. Observers can observe
 * bucket and cluster configurations from this component. Behind the scenes, it facilitates configuration loaders and
 * configuration refreshers that grab initial configurations and keep them refreshed respectively. The structure
 * looks like this:</p>
 *
 * <pre>
 *
 *     [ConfigurationProvider] --&gt; [Config from REST]
 *     [ConfigurationProvider] --&gt; [Config from Carrier]
 *
 *     package "Config from REST" {
 *         [HttpLoader]
 *         [HttpRefresher]
 *     }
 *
 *     [HttpLoader] --&gt; 8091
 *     [HttpRefresher] --&gt; 8091
 *
 *     package "Config from Carrier" {
 *         [CarrierLoader]
 *         [CarrierRefresher]
 *     }
 *
 *     [CarrierLoader] --&gt; 11210
 *     [CarrierRefresher] --&gt; 11210
 *
 * </pre>
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class DefaultConfigurationProvider implements ConfigurationProvider {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ConfigurationProvider.class);

    /**
     * Reference to the cluster to issue config fetching commands.
     */
    private final ClusterFacade cluster;

    /**
     * The observable which will push out new config changes to interested parties.
     */
    private final Subject<ClusterConfig, ClusterConfig> configObservable;

    private final List<Loader> loaderChain;
    private final Map<LoaderType, Refresher> refreshers;
    private final CoreEnvironment environment;
    private final EventBus eventBus;

    /**
     * Signals if the provider is completely terminated.
     */
    private volatile boolean terminated;

    /**
     * Signals if the provider is bootstrapped and serving configs.
     */
    private volatile boolean bootstrapped;

    /**
     * Represents the current cluster-wide configuration.
     */
    private volatile ClusterConfig currentConfig;

    /**
     * List of initial bootstrap seed hostnames.
     */
    private volatile Set<NetworkAddress> seedHosts;

    /**
     * If null means not decided yet, true and false mean decided.
     *
     * Optional is not available in java 6, go figure.
     */
    private volatile String externalNetwork;

    /**
     * Create a new {@link DefaultConfigurationProvider}.
     *
     * When this constructor is used, the default loader chain is populated (first carrier is tried and the http
     * loader is registered as a fallback).
     *
     * @param cluster the cluster reference.
     * @param environment the environment.
     */
    public DefaultConfigurationProvider(final ClusterFacade cluster, final CoreEnvironment environment) {
        this(
            cluster,
            environment,
            Arrays.asList((Loader) new CarrierLoader(cluster, environment), new HttpLoader(cluster, environment)),
            new HashMap<LoaderType, Refresher>() {
                {
                    put(LoaderType.Carrier, new CarrierRefresher(environment, cluster));
                    put(LoaderType.HTTP, new HttpRefresher(environment, cluster));
                }
            }
        );
    }

    /**
     * Create a new {@link DefaultConfigurationProvider}.
     *
     * @param cluster the cluster reference.
     * @param environment the environment.
     * @param loaderChain the configuration loaders which will be tried in sequence.
     */
    public DefaultConfigurationProvider(final ClusterFacade cluster, final CoreEnvironment environment,
        final List<Loader> loaderChain, final Map<LoaderType, Refresher> refreshers) {
        if (cluster == null) {
            throw new IllegalArgumentException("A cluster reference needs to be provided");
        }
        if (loaderChain == null || loaderChain.isEmpty()) {
            throw new IllegalArgumentException("At least one config loader needs to be provided");
        }
        this.cluster = cluster;
        this.loaderChain = loaderChain;
        this.refreshers = refreshers;
        this.environment = environment;
        this.eventBus = environment.eventBus();

        configObservable = PublishSubject.<ClusterConfig>create().toSerialized();
        seedHosts = null;
        bootstrapped = false;
        terminated = false;
        currentConfig = new DefaultClusterConfig();

        Observable
            .from(refreshers.values())
            .doOnNext(new Action1<Refresher>() {
                @Override
                public void call(Refresher refresher) {
                    refresher.provider(DefaultConfigurationProvider.this);
                }
            })
            .flatMap(new Func1<Refresher, Observable<ProposedBucketConfigContext>>() {
                @Override
                public Observable<ProposedBucketConfigContext> call(Refresher refresher) {
                    return refresher.configs();
                }
            }).subscribe(new Action1<ProposedBucketConfigContext>() {
                @Override
                public void call(ProposedBucketConfigContext ctx) {
                    proposeBucketConfig(ctx);
                }
            });
    }

    @Override
    public Observable<ClusterConfig> configs() {
        return configObservable;
    }

    @Override
    public ClusterConfig config() {
        return currentConfig;
    }

    @Override
    public boolean seedHosts(final Set<NetworkAddress> hosts, boolean shuffle) {
        if (bootstrapped) {
            LOGGER.debug("Seed hosts called with {}, but already bootstrapped.", hosts);
            return false;
        }
        LOGGER.debug("Setting seed hosts to {}", hosts);
        if (shuffle) {
            final List<NetworkAddress> hostsList = new ArrayList<NetworkAddress>(hosts);
            Collections.shuffle(hostsList);
            seedHosts = new LinkedHashSet<NetworkAddress>(hostsList);
        } else {
            seedHosts = new LinkedHashSet<NetworkAddress>(hosts);
        }
        return true;
    }

    @Override
    public Observable<ClusterConfig> openBucket(final String bucket, final String password) {
        return openBucket(bucket, bucket, password);
    }

    @Override
    public Observable<ClusterConfig> openBucket(final String bucket, final String username, final String password) {
        LOGGER.debug("Got instructed to open bucket {}", bucket);
        if (currentConfig != null && currentConfig.hasBucket(bucket)) {
            LOGGER.debug("Bucket {} already opened.", bucket);
            return Observable.just(currentConfig);
        }

        if (seedHosts == null || seedHosts.isEmpty()) {
            return Observable.error(new ConfigurationException("Seed node list not provided or empty."));
        }

        Observable<Tuple2<LoaderType, BucketConfig>> observable = Observable.mergeDelayError(Observable
                .from(seedHosts)
                .map(new Func1<NetworkAddress, Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> call(NetworkAddress seedHost) {
                        Observable<Tuple2<LoaderType, BucketConfig>> node = loaderChain.get(0)
                                .loadConfig(seedHost, bucket, username, password);
                        for (int i = 1; i < loaderChain.size(); i++) {
                            node = node.onErrorResumeNext(loaderChain.get(i)
                                    .loadConfig(seedHost, bucket, username, password));
                        }
                        return node;
                    }
                })
        ).take(1);

        return
                observable
                        .doOnNext(new Action1<Tuple2<LoaderType, BucketConfig>>() {
                            @Override
                            public void call(final Tuple2<LoaderType, BucketConfig> tuple) {
                                registerBucketForRefresh(tuple.value1(), tuple.value2());
                            }
                        })
                        .map(new Func1<Tuple2<LoaderType, BucketConfig>, ClusterConfig>() {
                            @Override
                            public ClusterConfig call(final Tuple2<LoaderType, BucketConfig> tuple) {
                                upsertBucketConfig(tuple.value2());
                                return currentConfig;
                            }
                        })
                        .doOnNext(new Action1<ClusterConfig>() {
                            @Override
                            public void call(ClusterConfig clusterConfig) {
                                LOGGER.info("Opened bucket {}", meta(bucket));
                                if (eventBus != null && eventBus.hasSubscribers()) {
                                    eventBus.publish(new BucketOpenedEvent(bucket));
                                }
                                bootstrapped = true;
                            }
                        })
                        .doOnError(new Action1<Throwable>() {
                            @Override
                            public void call(Throwable throwable) {
                                LOGGER.debug("Explicitly closing bucket {} after failed open attempt to clean resources.", bucket);
                                removeBucketConfig(bucket);
                            }
                        })
                        .onErrorResumeNext(new Func1<Throwable, Observable<ClusterConfig>>() {
                            @Override
                            public Observable<ClusterConfig> call(final Throwable throwable) {
                                return Observable.error(new ConfigurationException("Could not open bucket.", throwable));
                            }
                        });
    }

    @Override
    public Observable<ClusterConfig> closeBucket(String name) {
        LOGGER.debug("Closing bucket {}", name);
        return Observable.just(name).map(new Func1<String, ClusterConfig>() {
            @Override
            public ClusterConfig call(String bucket) {
                removeBucketConfig(bucket);
                LOGGER.info("Closed bucket {}", meta(bucket));
                if (eventBus != null && eventBus.hasSubscribers()) {
                    eventBus.publish(new BucketClosedEvent(bucket));
                }
                return currentConfig;
            }
        });
    }

    @Override
    public Observable<Boolean> closeBuckets() {
        LOGGER.debug("Closing all open buckets");
        if (currentConfig == null || currentConfig.bucketConfigs().isEmpty()) {
            return Observable.just(true);
        }

        Set<String> configs = new HashSet<String>(currentConfig.bucketConfigs().keySet());
        return Observable
            .from(configs)
            .observeOn(environment.scheduler())
            .flatMap(new Func1<String, Observable<? extends ClusterConfig>>() {
                @Override
                public Observable<? extends ClusterConfig> call(String bucketName) {
                    return closeBucket(bucketName);
                }
            })
            .last()
            .map(new Func1<ClusterConfig, Boolean>() {
                @Override
                public Boolean call(ClusterConfig clusterConfig) {
                    return true;
                }
            });
    }

    @Override
    public void proposeBucketConfig(ProposedBucketConfigContext ctx) {
        try {
            LOGGER.debug("New Bucket {} config proposed.", ctx.bucketName());
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Proposed raw config is {}", ctx.config());
            }

            JsonNode configNodes = DefaultObjectMapper.readTree(ctx.config());
            String bucketName = ctx.bucketName() == null ? configNodes.get("name").textValue() : ctx.bucketName();

            JsonNode revNode = configNodes.get("rev");
            long newRev = revNode == null ? 0 : revNode.asLong();

            BucketConfig oldConfig = currentConfig.bucketConfig(bucketName);
            if (newRev > 0 && oldConfig != null && newRev <= oldConfig.rev()) {
                LOGGER.trace("Not applying new configuration, older or same rev ID.");
                return;
            }

            BucketConfig config = BucketConfigParser.parse(ctx.config(), environment, ctx.origin());
            upsertBucketConfig(config);
        } catch (Exception ex) {
            LOGGER.warn("Could not read proposed configuration, ignoring. Message: {}", ex.getMessage());
        }
    }


    @Override
    public void signalOutdated() {
        LOGGER.debug("Received signal to proactively refresh (a maybe outdated) configuration.");

        if (currentConfig.bucketConfigs().isEmpty()) {
            LOGGER.debug("Ignoring outdated signal, since no buckets are open.");
            return;
        }

        for (Refresher refresher : refreshers.values()) {
            refresher.refresh(currentConfig);
        }
    }

    @Override
    public synchronized Observable<Boolean> shutdown() {
        if (terminated) {
            LOGGER.debug("ConfigurationProvider already shut down, ignoring.");
            return Observable.just(true);
        } else {
            LOGGER.debug("Shutting down ConfigurationProvider.");
            terminated = true;

            return Observable
                .just(true)
                .doOnNext(new Action1<Boolean>() {
                    @Override
                    public void call(Boolean ignored) {
                        if (configObservable != null) {
                            LOGGER.trace("Completing ConfigObservable for termination.");
                            configObservable.onCompleted();
                        }
                    }
                })
                .flatMap(new Func1<Boolean, Observable<Boolean>>() {
                    @Override
                    public Observable<Boolean> call(Boolean aBoolean) {
                        Observable<Boolean> shutdownObs = Observable.just(true);
                        for (final Refresher refresher : refreshers.values()) {
                            shutdownObs = shutdownObs.flatMap(new Func1<Boolean, Observable<Boolean>>() {
                                @Override
                                public Observable<Boolean> call(Boolean ignored) {
                                    LOGGER.trace("Initiating {} shutdown.",
                                        refresher.getClass().getSimpleName());
                                    return refresher.shutdown();
                                }
                            });
                        }
                        return shutdownObs;
                    }
                });
        }
    }

    /**
     * Helper method which registers (after a {@link #openBucket(String, String)} call) the bucket for config
     * refreshes.
     *
     * @param loaderType the type of the lader used to determine the proper refresher.
     * @param bucketConfig the config itself.
     */
    private void registerBucketForRefresh(final LoaderType loaderType, final BucketConfig bucketConfig) {
        LOGGER.debug("Registering Bucket {} to refresh at Loader {}", bucketConfig.name(), loaderType);
        Refresher refresher = refreshers.get(loaderType);
        if (refresher == null) {
            throw new IllegalStateException("Could not find refresher for loader type: " + loaderType);
        }

        refresher.registerBucket(bucketConfig.name(), bucketConfig.username(), bucketConfig.password()).subscribe();
    }

    /**
     * Helper method which takes the given bucket config and applies it to the cluster config.
     *
     * This method also sends out an update to the subject afterwards, so that observers are notified.
     *
     * @param newConfig the configuration of the bucket.
     */
    private void upsertBucketConfig(final BucketConfig newConfig) {
        ClusterConfig cluster = currentConfig;
        BucketConfig oldConfig = cluster.bucketConfig(newConfig.name());

        if (newConfig.rev() > 0 && oldConfig != null && newConfig.rev() <= oldConfig.rev()) {
            LOGGER.trace("Not applying new configuration, older or same rev ID.");
            return;
        }

        // If the current password of the config is empty and an old config exists
        // make sure to transfer the password over to the new config. Otherwise it
        // is possible that authentication errors because of a null password arise.
        // See JVMCBC-185
        if (newConfig.password() == null && oldConfig != null) {
            newConfig.password(oldConfig.password());
        }

        //copy the username as well
        if (oldConfig != null) {
            newConfig.username(oldConfig.username());
        }

        // this is the first config, decide on external networking
        if (oldConfig == null) {
            externalNetwork = determineNetworkResolution(newConfig, environment.networkResolution(), seedHosts);
            LOGGER.info("Selected network configuration: {}", externalNetwork != null ? externalNetwork : "default");
        }

        if (externalNetwork != null) {
            newConfig.useAlternateNetwork(externalNetwork);
        }

        cluster.setBucketConfig(newConfig.name(), newConfig);
        LOGGER.debug("Applying new configuration {}", newConfig);

        currentConfig = cluster;

        boolean tainted = newConfig.tainted();
        for (Refresher refresher : refreshers.values()) {
            if (tainted) {
                refresher.markTainted(newConfig);
            } else {
                refresher.markUntainted(newConfig);
            }
        }

        configObservable.onNext(currentConfig);
    }

    /**
     * Helper method to figure out which network resolution should be used.
     *
     * if DEFAULT is selected, then null is returned which is equal to the "internal" or default
     * config mode. If AUTO is used then we perform the select heuristic based off of the seed
     * hosts given. All other resolution settings (i.e. EXTERNAL) are returned directly and are
     * considered to be part of the alternate address configs.
     *
     * @param config the config to check against
     * @param nr the network resolution setting from the environment
     * @param seedHosts the seed hosts from bootstrap for autoconfig.
     * @return the found setting if external is used, null if internal/default is used.
     */
    public static String determineNetworkResolution(final BucketConfig config, final NetworkResolution nr,
        final Set<NetworkAddress> seedHosts) {
        if (nr.equals(NetworkResolution.DEFAULT)) {
            return null;
        } else if (nr.equals(NetworkResolution.AUTO)) {
            for (NodeInfo info : config.nodes()) {
                if (seedHosts.contains(info.hostname())) {
                    return null;
                }

                Map<String, AlternateAddress> aa = info.alternateAddresses();
                if (aa != null && !aa.isEmpty()) {
                    for (Map.Entry<String, AlternateAddress> entry : aa.entrySet()) {
                        AlternateAddress alternateAddress = entry.getValue();
                        if (alternateAddress != null && seedHosts.contains(alternateAddress.hostname())) {
                            return entry.getKey();
                        }
                    }
                }
            }
            return null;
        } else {
            return nr.name();
        }
    }

    /**
     * Remove a bucket config (closing it).
     *
     * @param name the name of the bucket.
     */
    private void removeBucketConfig(final String name) {
        LOGGER.debug("Removing bucket {} configuration from known configs.", name);
        ClusterConfig cluster = currentConfig;
        cluster.deleteBucketConfig(name);
        currentConfig = cluster;
        configObservable.onNext(currentConfig);
    }
}
