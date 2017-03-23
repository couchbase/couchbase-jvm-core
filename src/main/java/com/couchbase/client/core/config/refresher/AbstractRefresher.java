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
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

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
    private final Subject<BucketConfig, BucketConfig> configStream;

    /**
     * Cluster reference so that implementations can call requests.
     */
    private final ClusterFacade cluster;

    private ConfigurationProvider provider;

    private final Map<String, Credential> registrations;

    private final CoreEnvironment env;

    /**
     * Creates a new {@link AbstractRefresher}.
     *
     * @param env the environment
     * @param cluster the cluster reference.
     */
    protected AbstractRefresher(final CoreEnvironment env, final ClusterFacade cluster) {
        this.env = env;
        this.configStream = PublishSubject.<BucketConfig>create().toSerialized();
        this.cluster = cluster;
        registrations = new ConcurrentHashMap<String, Credential>();
    }

    @Override
    public Observable<Boolean> deregisterBucket(String name) {
        LOGGER.debug("Deregistering Bucket {} from refresh.", name);
        if (registrations.containsKey(name)) {
            registrations.remove(name);
            return Observable.just(true);
        }
        return Observable.just(false);
    }

    @Override
    public Observable<Boolean> registerBucket(String name, String password) {
        return registerBucket(name, name, password);
    }

    @Override
    public Observable<Boolean> registerBucket(String name, String username, String password) {
        LOGGER.debug("Registering Bucket {} for refresh.", name);
        if (registrations.containsKey(name)) {
            return Observable.just(false);
        }

        registrations.put(name, new Credential(username, password));
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
            configStream.onNext(BucketConfigParser.parse(config, env));
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

    @Override
    public void provider(ConfigurationProvider provider) {
        this.provider = provider;
    }

    protected Map<String, Credential> registrations() {
        return registrations;
    }

    static class Credential {
        final private String username;
        final private String password;

        public Credential(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public String username() {
            return this.username;
        }

        public String password() {
            return this.password;
        }
    }
}
