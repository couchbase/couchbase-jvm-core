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
package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.config.LoaderType;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import rx.Observable;
import rx.functions.Func1;

/**
 * Loads a raw bucket configuration through the Couchbase Server HTTP config interface.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class HttpLoader extends AbstractLoader {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(HttpLoader.class);

    public static final String TERSE_PATH = "/pools/default/b/";
    public static final String VERBOSE_PATH = "/pools/default/buckets/";

    /**
     * Creates a new {@link HttpLoader}.
     *
     * @param cluster the cluster reference.
     * @param environment the environment to use.
     */
    public HttpLoader(ClusterFacade cluster, CoreEnvironment environment) {
        super(LoaderType.HTTP, ServiceType.CONFIG, cluster, environment);
    }

    @Override
    protected int port(String hostname) {
        Integer portFromConfig = tryLoadingPortFromConfig(hostname);
        if (portFromConfig != null) {
            return portFromConfig;
        }

        int port = env().sslEnabled() ? env().bootstrapHttpSslPort() : env().bootstrapHttpDirectPort();
        LOGGER.trace("Picked (default) port " + port + " for " + hostname);
        return port;
    }

    @Override
    protected Observable<String> discoverConfig(final String bucket, final String username, final String password,
        final String hostname) {
        if (!env().bootstrapHttpEnabled()) {
            LOGGER.info("HTTP Bootstrap manually disabled.");
            return Observable.error(new ConfigurationException("HTTP Bootstrap disabled through configuration."));
        }
        LOGGER.debug("Starting to discover config through HTTP Bootstrap");

        return cluster()
            .<BucketConfigResponse>send(new BucketConfigRequest(TERSE_PATH, hostname, bucket, username, password))
            .flatMap(new Func1<BucketConfigResponse, Observable<BucketConfigResponse>>() {
                @Override
                public Observable<BucketConfigResponse> call(BucketConfigResponse response) {
                    if (response.status().isSuccess()) {
                        LOGGER.debug("Successfully got config from terse bucket remote.");
                        return Observable.just(response);
                    }

                    if (response.status() == ResponseStatus.NOT_EXISTS) {
                        LOGGER.debug("Terse bucket config failed (not found), falling back to verbose.");
                        return cluster().send(new BucketConfigRequest(VERBOSE_PATH, hostname, bucket, username, password));
                    } else {
                        LOGGER.debug("Terse bucket config failed, propagating failed response");
                        return Observable.just(response);
                    }
                }
            })
            .map(new Func1<BucketConfigResponse, String>() {
                @Override
                public String call(BucketConfigResponse response) {
                    if (!response.status().isSuccess()) {
                        throw new IllegalStateException("Could not load bucket configuration: "
                            + response.status() + "(" + response.config() + ")");
                    }

                    LOGGER.debug("Successfully loaded config through HTTP.");
                    return replaceHostWildcard(response.config(), hostname);
                }
            });
    }
}
