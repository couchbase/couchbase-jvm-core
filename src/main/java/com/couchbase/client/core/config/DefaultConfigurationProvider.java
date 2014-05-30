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
package com.couchbase.client.core.config;

import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.config.loader.CarrierLoader;
import com.couchbase.client.core.config.loader.HttpLoader;
import com.couchbase.client.core.config.loader.Loader;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.core.config.refresher.CarrierRefresher;
import com.couchbase.client.core.config.refresher.HttpRefresher;
import com.couchbase.client.core.config.refresher.Refresher;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.lang.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * **The default implementation of a {@link ConfigurationProvider}.**
 *
 * The {@link ConfigurationProvider} is the central orchestrator for configuration management. Observers can subscribe
 * bucket and cluster configurations from this component. Behind the scenes, it facilitates configuration loaders and
 * configuration refreshers that grab initial configurations and keep them refreshed respectively. The structure
 * looks like this:
 *
 * ![Configuration Provider Architecture](architecture.png)
 *
 * @startuml architecture.png
 *
 * [ConfigurationProvider] --> [Config from REST]
 * [ConfigurationProvider] --> [Config from Carrier]
 *
 * package "Config from REST" {
 *   [HttpLoader]
 *   [HttpRefresher]
 * }
 *
 * [HttpLoader] --> 8091
 * [HttpRefresher] --> 8091
 *
 * package "Config from Carrier" {
 *     [CarrierLoader]
 *     [CarrierRefresher]
 * }
 *
 * [CarrierLoader] --> 11210
 * [CarrierRefresher] --> 11210
 *
 *
 * @enduml
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class DefaultConfigurationProvider implements ConfigurationProvider {

    /**
     * The logger used.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationProvider.class);

    /**
     * Reference to the cluster to issue config fetching commands.
     */
    private final Cluster cluster;

    /**
     * The observable which will push out new config changes to interested parties.
     */
    private final PublishSubject<ClusterConfig> configObservable;

    /**
     * Represents the current cluster-wide configuration.
     */
    private final AtomicReference<ClusterConfig> currentConfig;

    /**
     * List of initial bootstrap seed hostnames.
     */
    private final AtomicReference<Set<InetAddress>> seedHosts;

    private final List<Loader> loaderChain;
    private final Map<LoaderType, Refresher> refreshers;

    /**
     * Signals if the provider is bootstrapped and serving configs.
     */
    private volatile boolean bootstrapped;

    /**
     * Create a new {@link DefaultConfigurationProvider}.
     *
     * When this constructor is used, the default loader chain is populated (first carrier is tried and the http
     * loader is registered as a fallback).
     *
     * @param cluster the cluster reference.
     * @param environment the environment.
     */
    public DefaultConfigurationProvider(final Cluster cluster, final Environment environment) {
        this(
            cluster,
            environment,
            Arrays.asList((Loader) new CarrierLoader(cluster, environment), new HttpLoader(cluster, environment)),
            new HashMap<LoaderType, Refresher>() {{
                put(LoaderType.Carrier, new CarrierRefresher(cluster));
                put(LoaderType.HTTP, new HttpRefresher(cluster));
            }}
        );
    }

    /**
     * Create a new {@link DefaultConfigurationProvider}.
     *
     * @param cluster the cluster reference.
     * @param environment the environment.
     * @param loaderChain the configuration loaders which will be tried in sequence.
     */
    public DefaultConfigurationProvider(final Cluster cluster, final Environment environment,
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

        configObservable = PublishSubject.create();
        seedHosts = new AtomicReference<Set<InetAddress>>();
        bootstrapped = false;
        currentConfig = new AtomicReference<ClusterConfig>(new DefaultClusterConfig());

        Observable.from(refreshers.values()).flatMap(new Func1<Refresher, Observable<BucketConfig>>() {
            @Override
            public Observable<BucketConfig> call(Refresher refresher) {
                return refresher.configs();
            }
        }).subscribe(new Action1<BucketConfig>() {
            @Override
            public void call(BucketConfig bucketConfig) {
                upsertBucketConfig(bucketConfig);
            }
        });
    }

    @Override
    public Observable<ClusterConfig> configs() {
        return configObservable;
    }

    @Override
    public boolean seedHosts(final Set<InetAddress> hosts) {
        if (bootstrapped) {
            LOGGER.debug("Seed hosts called with {}, but already bootstrapped.", hosts);
            return false;
        }
        LOGGER.debug("Setting seed hosts to {}", hosts);
        this.seedHosts.set(hosts);
        return true;
    }

    @Override
    public Observable<ClusterConfig> openBucket(final String bucket, final String password) {
        if (currentConfig.get() != null && currentConfig.get().hasBucket(bucket)) {
            return Observable.from(currentConfig.get());
        }

        Observable<Tuple2<LoaderType, BucketConfig>> observable = loaderChain.get(0).loadConfig(seedHosts.get(),
            bucket, password);
        for (int i = 1; i < loaderChain.size(); i++) {
            observable = observable.onErrorResumeNext(
                loaderChain.get(i).loadConfig(seedHosts.get(), bucket, password)
            );
        }

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
                    return currentConfig.get();
                }
            }).onErrorResumeNext(new Func1<Throwable, Observable<ClusterConfig>>() {
                @Override
                public Observable<ClusterConfig> call(Throwable throwable) {
                    return Observable.error(new ConfigurationException("Could not open bucket.", throwable));
                }
            });
    }

    @Override
    public Observable<ClusterConfig> closeBucket(String name) {
        return Observable.from(name).map(new Func1<String, ClusterConfig>() {
            @Override
            public ClusterConfig call(String bucket) {
                removeBucketConfig(bucket);
                return currentConfig.get();
            }
        });
    }

    @Override
    public Observable<ClusterConfig> closeBuckets() {
        return Observable
            .from(currentConfig.get().bucketConfigs().keySet())
            .flatMap(new Func1<String, Observable<? extends ClusterConfig>>() {
                @Override
                public Observable<? extends ClusterConfig> call(String bucketName) {
                    return closeBucket(bucketName);
                }
            }).last();
    }

    @Override
    public void proposeBucketConfig(String bucket, String rawConfig) {
        BucketConfig config = BucketConfigParser.parse(rawConfig);
        config.password(currentConfig.get().bucketConfig(bucket).password());
        upsertBucketConfig(config);
    }

    /**
     * Helper method which registers (after a {@link #openBucket(String, String)} call) the bucket for config
     * refreshes.
     *
     * @param loaderType the type of the lader used to determine the proper refresher.
     * @param bucketConfig the config itself.
     */
    private void registerBucketForRefresh(final LoaderType loaderType, final BucketConfig bucketConfig) {
        Refresher refresher = refreshers.get(loaderType);
        if (refresher == null) {
            throw new IllegalStateException("Could not find refresher for loader type: " + loaderType);
        }

        refresher.registerBucket(bucketConfig.name(), bucketConfig.password()).subscribe();
    }

    /**
     * Helper method which takes the given bucket config and applies it to the cluster config.
     *
     * This method also sends out an update to the subject afterwards, so that observers are notified.
     *
     * @param config the configuration of the bucket.
     */
    private void upsertBucketConfig(final BucketConfig config) {
        ClusterConfig cluster = currentConfig.get();
        cluster.setBucketConfig(config.name(), config);
        currentConfig.set(cluster);
        configObservable.onNext(currentConfig.get());
    }

    private void removeBucketConfig(final String name) {
        ClusterConfig cluster = currentConfig.get();
        cluster.deleteBucketConfig(name);
        currentConfig.set(cluster);
        configObservable.onNext(currentConfig.get());
    }
}
