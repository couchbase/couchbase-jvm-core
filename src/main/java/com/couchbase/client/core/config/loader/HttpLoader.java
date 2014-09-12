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
package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.config.LoaderType;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import rx.Observable;
import rx.functions.Func1;

import java.net.InetAddress;

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

    private static final String TERSE_PATH = "/pools/default/b/";
    private static final String VERBOSE_PATH = "/pools/default/buckets/";

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
    protected int port() {
        return env().sslEnabled() ? env().bootstrapHttpSslPort() : env().bootstrapHttpDirectPort();
    }

    @Override
    protected Observable<String> discoverConfig(final String bucket, final String password, final InetAddress hostname) {
        if (!env().bootstrapHttpEnabled()) {
            LOGGER.info("HTTP Bootstrap manually disabled.");
            return Observable.error(new ConfigurationException("HTTP Bootstrap disabled through configuration."));
        }

        return cluster()
            .<BucketConfigResponse>send(new BucketConfigRequest(TERSE_PATH, hostname, bucket, password))
            .flatMap(new Func1<BucketConfigResponse, Observable<BucketConfigResponse>>() {
                @Override
                public Observable<BucketConfigResponse> call(BucketConfigResponse response) {
                    if (response.status().isSuccess()) {
                        LOGGER.debug("Successfully got config from terse bucket remote.");
                        return Observable.just(response);
                    }

                    LOGGER.debug("Terse bucket config failed, falling back to verbose.");
                    return cluster().send(new BucketConfigRequest(VERBOSE_PATH, hostname, bucket, password));
                }
            }).map(new Func1<BucketConfigResponse, String>() {
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
