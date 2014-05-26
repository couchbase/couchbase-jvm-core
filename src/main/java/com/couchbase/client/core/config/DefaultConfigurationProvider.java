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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.config.loader.CarrierLoader;
import com.couchbase.client.core.config.loader.HttpLoader;
import com.couchbase.client.core.env.Environment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The default implementation of a {@link ConfigurationProvider}.
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

    private final CarrierLoader carrierLoader;
    private final HttpLoader httpLoader;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Signals if the provider is bootstrapped and serving configs.
     */
    private volatile boolean bootstrapped;

    /**
     * Create a new {@link DefaultConfigurationProvider}.
     *
     * @param cluster the cluster reference.
     */
    public DefaultConfigurationProvider(final Cluster cluster, final Environment environment) {
        this.cluster = cluster;
        configObservable = PublishSubject.create();
        seedHosts = new AtomicReference<Set<InetAddress>>();
        bootstrapped = false;
        currentConfig = new AtomicReference<ClusterConfig>(new DefaultClusterConfig());
        carrierLoader = new CarrierLoader(cluster, environment);
        httpLoader = new HttpLoader(cluster, environment);
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

        return carrierLoader
            .loadConfig(seedHosts.get(), bucket, password)
            .onExceptionResumeNext(httpLoader.loadConfig(seedHosts.get(), bucket, password))
            .map(new Func1<BucketConfig, ClusterConfig>() {
                @Override
                public ClusterConfig call(BucketConfig bucketConfig) {
                    upsertBucketConfig(bucket, bucketConfig);
                    return currentConfig.get();
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
        try {
            BucketConfig config = OBJECT_MAPPER.readValue(rawConfig, BucketConfig.class);
            config.password(currentConfig.get().bucketConfig(bucket).password());
            upsertBucketConfig(bucket, config);
        } catch (Exception ex) {
            throw new CouchbaseException("Could not parse configuration", ex);
        }
    }

    /**
     * Helper method which takes the given bucket config and applies it to the cluster config.
     *
     * This method also sends out an update to the subject afterwards, so that observers are notified.
     *
     * @param name the name of the bucket.
     * @param config the configuration of the bucket.
     */
    private void upsertBucketConfig(final String name, final BucketConfig config) {
        ClusterConfig cluster = currentConfig.get();
        cluster.setBucketConfig(name, config);
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
