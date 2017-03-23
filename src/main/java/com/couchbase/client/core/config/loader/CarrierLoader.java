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
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.Buffers;
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
    protected Observable<String> discoverConfig(final String bucket, final String username, final String password,
        final InetAddress hostname) {
        if (!env().bootstrapCarrierEnabled()) {
            LOGGER.info("Carrier Bootstrap manually disabled.");
            return Observable.error(new ConfigurationException("Carrier Bootstrap disabled through configuration."));
        }
        LOGGER.debug("Starting to discover config through Carrier Bootstrap");

        return Buffers.wrapColdWithAutoRelease(
           cluster().<GetBucketConfigResponse>send(new GetBucketConfigRequest(bucket, hostname))
        )
        .map(new Func1<GetBucketConfigResponse, String>() {
            @Override
            public String call(GetBucketConfigResponse response) {
                if (!response.status().isSuccess()) {
                    response.content().release();
                    throw new IllegalStateException("Bucket config response did not return with success.");
                }

                LOGGER.debug("Successfully loaded config through carrier.");
                String content = response.content().toString(CharsetUtil.UTF_8);
                response.content().release();
                return replaceHostWildcard(content, hostname);
            }
        });
    }

}
