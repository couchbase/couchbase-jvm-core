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
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.config.ConfigEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.lmax.disruptor.RingBuffer;

public class ConfigService extends AbstractOnDemandService {

    /**
     * The endpoint factory.
     */
    private static final EndpointFactory FACTORY = new ConfigEndpointFactory();

    @Deprecated
    public ConfigService(String hostname, String bucket, String password, int port, CoreContext ctx) {
        this(hostname, bucket, bucket, password, port, ctx);
    }

    public ConfigService(String hostname, String bucket, String username, String password, int port,
        CoreContext ctx) {
        super(hostname, bucket, username, password, port, ctx, FACTORY);
    }

    @Override
    public ServiceType type() {
        return ServiceType.CONFIG;
    }

    static class ConfigEndpointFactory implements EndpointFactory {
        public Endpoint create(String hostname, String bucket, String username, String password, int port, CoreContext ctx) {
            return new ConfigEndpoint(hostname, bucket, username, password, port, ctx.environment(), ctx.responseRingBuffer());
        }
    }

}
