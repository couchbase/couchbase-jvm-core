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
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import io.netty.util.CharsetUtil;
import rx.Observable;
import rx.functions.Func1;

import java.net.InetAddress;

/**
 * Loads a raw bucket configuration through the carrier mechanism (also commonly referred to as CCCP).
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CarrierLoader extends AbstractLoader {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CarrierLoader.class);

    /**
     * Creates a new {@link CarrierLoader}.
     *
     * @param cluster the cluster reference.
     * @param environment the environment to use.
     */
    public CarrierLoader(ClusterFacade cluster, CoreEnvironment environment) {
        super(LoaderType.Carrier, ServiceType.BINARY, cluster, environment);
    }

    @Override
    protected int port() {
        return env().sslEnabled() ? env().bootstrapCarrierSslPort() : env().bootstrapCarrierDirectPort();
    }

    @Override
    protected Observable<String> discoverConfig(final String bucket, final String password, final InetAddress hostname) {
        if (!env().bootstrapCarrierEnabled()) {
            LOGGER.info("Carrier Bootstrap manually disabled.");
            return Observable.error(new ConfigurationException("Carrier Bootstrap disabled through configuration."));
        }
        return cluster()
            .<GetBucketConfigResponse>send(new GetBucketConfigRequest(bucket, hostname))
            .map(new Func1<GetBucketConfigResponse, String>() {
                @Override
                public String call(GetBucketConfigResponse response) {
                    if (!response.status().isSuccess()) {
                        throw new IllegalStateException("Bucket config response did not return with success.");
                    }

                    LOGGER.debug("Successfully loaded config through carrier.");
                    return replaceHostWildcard(response.content().toString(CharsetUtil.UTF_8), hostname);
                }
            });
    }

}
