/*
 * Copyright (c) 2015 Couchbase, Inc.
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

package com.couchbase.client.core.service;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.kv.KeyValueEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.service.strategies.PartitionSelectionStrategy;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.lmax.disruptor.RingBuffer;

public class KeyValueService extends AbstractPoolingService {

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
    public KeyValueService(final String hostname, final String bucket, final String password, final int port,
        final CoreEnvironment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, env.kvEndpoints(), env.kvEndpoints(), STRATEGY, responseBuffer,
            FACTORY);
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
        public Endpoint create(String hostname, String bucket, String password, int port, CoreEnvironment env,
            RingBuffer<ResponseEvent> responseBuffer) {
            return new KeyValueEndpoint(hostname, bucket, password, port, env, responseBuffer);
        }
    }
}
