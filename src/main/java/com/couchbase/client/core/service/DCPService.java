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

package com.couchbase.client.core.service;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.dcp.DCPEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.service.strategies.FirstConnectedSelectionStrategy;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.lmax.disruptor.RingBuffer;

/**
 * The {@link DCPService} is composed of and manages {@link DCPEndpoint}s.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
public class DCPService extends AbstractService {
    /**
     * The endpoint selection strategy.
     */
    private static final SelectionStrategy STRATEGY = new FirstConnectedSelectionStrategy();

    /**
     * The endpoint factory.
     */
    private static final EndpointFactory FACTORY = new DCPEndpointFactory();

    private static final int NUM_ENDPOINTS = 1;

    public DCPService(String hostname, String bucket, String password, int port,
                      CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, password, port, env, NUM_ENDPOINTS, STRATEGY, responseBuffer, FACTORY);
    }

    @Override
    public ServiceType type() {
        return ServiceType.DCP;
    }

    /**
     * The factory for {@link DCPEndpoint}s.
     */
    static class DCPEndpointFactory implements EndpointFactory {
        @Override
        public Endpoint create(final String hostname, final String bucket, final String password, final int port,
                               final CoreEnvironment env, final RingBuffer<ResponseEvent> responseBuffer) {
            return new DCPEndpoint(hostname, bucket, password, port, env, responseBuffer);
        }
    }
}
