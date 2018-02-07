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
package com.couchbase.client.core.service;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.lmax.disruptor.RingBuffer;

public class ServiceFactory {

    public static final ServiceFactory INSTANCE = new ServiceFactory();

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ServiceFactory.class);

    private static final boolean FORCE_OLD_SERVICES;

    static {
        FORCE_OLD_SERVICES = Boolean.parseBoolean(
                System.getProperty("com.couchbase.forceOldServices", "false")
        );

        if (FORCE_OLD_SERVICES) {
            LOGGER.info("Old Service override is enabled!");
        } else {
            LOGGER.debug("New Services are enabled");
        }
    }

    private ServiceFactory() { }

    public Service create(String hostname, String bucket, String username, String password,
        int port, CoreContext ctx, ServiceType type) {

        if (FORCE_OLD_SERVICES) {
            switch (type) {
                case BINARY:
                    return new OldKeyValueService(hostname, bucket, username, password, port, ctx);
                case VIEW:
                    return new OldViewService(hostname, bucket, username, password, port, ctx);
                case CONFIG:
                    return new ConfigService(hostname, bucket, username, password, port, ctx);
                case QUERY:
                    return new OldQueryService(hostname, bucket, username, password, port, ctx);
                case SEARCH:
                    return new OldSearchService(hostname, bucket, username, password, port, ctx);
                case ANALYTICS:
                    return new OldAnalyticsService(hostname, bucket, username, password, port, ctx);
                default:
                    throw new IllegalArgumentException("Unknown Service Type: " + type);
            }
        } else {
            switch (type) {
                case BINARY:
                    return new KeyValueService(hostname, bucket, username, password, port, ctx);
                case VIEW:
                    return new ViewService(hostname, bucket, username, password, port, ctx);
                case CONFIG:
                    return new ConfigService(hostname, bucket, username, password, port, ctx);
                case QUERY:
                    return new QueryService(hostname, bucket, username, password, port, ctx);
                case SEARCH:
                    return new SearchService(hostname, bucket, username, password, port, ctx);
                case ANALYTICS:
                    return new AnalyticsService(hostname, bucket, username, password, port, ctx);
                default:
                    throw new IllegalArgumentException("Unknown Service Type: " + type);
            }
        }


    }

}
