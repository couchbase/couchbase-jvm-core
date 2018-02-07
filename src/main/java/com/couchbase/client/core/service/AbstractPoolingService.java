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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.lmax.disruptor.RingBuffer;

/**
 * Abstract implementation of a (fixed size) pooling Service.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public abstract class AbstractPoolingService extends AbstractDynamicService {

    private final int maxEndpoints;
    private final RingBuffer<ResponseEvent> responseBuffer;
    private final SelectionStrategy strategy;
    private final CoreEnvironment env;

    protected AbstractPoolingService(String hostname, String bucket, String username, String password, int port,
        CoreContext ctx, int minEndpoints, int maxEndpoints, SelectionStrategy strategy, EndpointFactory endpointFactory) {
        super(hostname, bucket, username, password, port, ctx, minEndpoints, endpointFactory);
        this.maxEndpoints = maxEndpoints;
        this.responseBuffer = ctx.responseRingBuffer();
        this.strategy = strategy;
        this.env = ctx.environment();
    }

    @Override
    protected void dispatch(final CouchbaseRequest request) {
        if (endpoints().size() == maxEndpoints) {
            Endpoint endpoint = strategy.select(request, endpoints());
            if (endpoint == null) {
                RetryHelper.retryOrCancel(env, request, responseBuffer);

            } else {
                endpoint.send(request);
            }
        } else {
            throw new UnsupportedOperationException("Dynamic endpoint scaling is currently not supported.");
        }
    }

}
