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
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Common implementation for all refreshers.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public abstract  class AbstractRefresher implements Refresher {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Refresher.class);

    /**
     * The config stream where the provider subscribes to.
     */
    private final PublishSubject<BucketConfig> configStream;

    /**
     * Cluster reference so that implementations can call requests.
     */
    private final ClusterFacade cluster;

    private ConfigurationProvider provider;

    private final Map<String, String> registrations;

    /**
     * Creates a new {@link AbstractRefresher}.
     *
     * @param cluster the cluster reference.
     */
    protected AbstractRefresher(final ClusterFacade cluster) {
        this.configStream = PublishSubject.create();
        this.cluster = cluster;
        registrations = new ConcurrentHashMap<String, String>();
    }

    @Override
    public Observable<Boolean> deregisterBucket(String name) {
        if (registrations.containsKey(name)) {
            registrations.remove(name);
            return Observable.just(true);
        }
        return Observable.just(false);
    }

    @Override
    public Observable<Boolean> registerBucket(String name, String password) {
        if (registrations.containsKey(name)) {
            return Observable.just(false);
        }

        registrations.put(name, password);
        return Observable.just(true);
    }

    @Override
    public Observable<BucketConfig> configs() {
        return configStream;
    }

    /**
     * Push a {@link BucketConfig} into the config stream.
     *
     * @param config the config to push.
     */
    protected void pushConfig(final String config) {
        try {
            configStream.onNext(BucketConfigParser.parse(config));
        } catch (CouchbaseException e) {
            LOGGER.warn("Exception while pushing new configuration - ignoring.", e);
        }
    }

    /**
     * Returns the cluster reference.
     *
     * @return the cluster reference.
     */
    protected ClusterFacade cluster() {
        return cluster;
    }

    protected ConfigurationProvider provider() {
        return provider;
    }

    protected Map<String, String> registrations() {
        return registrations;
    }

    @Override
    public void provider(ConfigurationProvider provider) {
        this.provider = provider;
    }

}
