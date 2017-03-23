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

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.kv.KeyValueEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.service.strategies.PartitionSelectionStrategy;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public class KeyValueService extends PooledService {

    /**
     * The endpoint selection strategy.
     */
    private static final SelectionStrategy STRATEGY = PartitionSelectionStrategy.INSTANCE;

    /**
     * The endpoint factory.
     */
    private static final EndpointFactory FACTORY = new KeyValueEndpointFactory();

    /**
     * Creates a new {@link KeyValueService}.
     *
     * @param hostname the hostname of the service.
     * @param bucket the name of the bucket.
     * @param password the password of the bucket.
     * @param port the port of the service.
     * @param env the shared environment.
     * @param responseBuffer the shared response buffer.
     */
    @Deprecated
    public KeyValueService(final String hostname, final String bucket, final String password, final int port,
                           final CoreEnvironment env, final RingBuffer<ResponseEvent> responseBuffer) {
        this(hostname, bucket, bucket, password, port, env, responseBuffer);
    }

    /**
     * Creates a new {@link KeyValueService}.
     *
     * @param hostname the hostname of the service.
     * @param bucket the name of the bucket.
     * @param username the user authorized for bucket access.
     * @param password the password of the user.
     * @param port the port of the service.
     * @param env the shared environment.
     * @param responseBuffer the shared response buffer.
     */
    public KeyValueService(final String hostname, final String bucket, final String username, final String password, final int port,
        final CoreEnvironment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, username, password, port, env, env.kvServiceConfig(), responseBuffer, FACTORY, STRATEGY);
    }

    @Override
    public ServiceType type() {
        return ServiceType.BINARY;
    }

    /**
     * The factory for {@link KeyValueEndpoint}s.
     */
    static class KeyValueEndpointFactory implements EndpointFactory {
        @Override
        public Endpoint create(String hostname, String bucket, String username, String password, int port, CoreEnvironment env,
            RingBuffer<ResponseEvent> responseBuffer) {
            return new KeyValueEndpoint(hostname, bucket, username, password, port, env, responseBuffer);
        }
    }
}
