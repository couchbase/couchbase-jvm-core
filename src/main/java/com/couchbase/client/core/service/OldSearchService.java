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
import com.couchbase.client.core.endpoint.search.SearchEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.service.strategies.RandomSelectionStrategy;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.lmax.disruptor.RingBuffer;

/**
 * @author Sergey Avseyev
 */
public class OldSearchService extends AbstractPoolingService {
    /**
     * The endpoint selection strategy.
     */
    private static final SelectionStrategy STRATEGY = new RandomSelectionStrategy();
    /**
     * The endpoint factory.
     */
    private static final EndpointFactory FACTORY = new SearchEndpointFactory();

    /**
     * Creates a new {@link ViewService}.
     *
     * @param hostname       the hostname of the service.
     * @param bucket         the name of the bucket.
     * @param password       the password of the bucket.
     * @param port           the port of the service.
     * @param env            the shared environment.
     * @param responseBuffer the shared response buffer.
     */
    @Deprecated
    public OldSearchService(final String hostname, final String bucket, final String password, final int port,
                            final CoreEnvironment env, final RingBuffer<ResponseEvent> responseBuffer) {
        this(hostname, bucket, bucket, password, port, env, responseBuffer);
    }

    /**
     * Creates a new {@link ViewService}.
     *
     * @param hostname       the hostname of the service.
     * @param bucket         the name of the bucket.
     * @param username       the user authorized for bucket access.
     * @param password       the password of the user.
     * @param port           the port of the service.
     * @param env            the shared environment.
     * @param responseBuffer the shared response buffer.
     */
    public OldSearchService(final String hostname, final String bucket, final String username, final String password, final int port,
                         final CoreEnvironment env, final RingBuffer<ResponseEvent> responseBuffer) {
        super(hostname, bucket, username, password, port, env, env.searchEndpoints(), env.searchEndpoints(), STRATEGY,
                responseBuffer, FACTORY);
    }

    @Override
    public ServiceType type() {
        return ServiceType.SEARCH;
    }

    /**
     * The factory for {@link SearchEndpoint}s.
     */
    static class SearchEndpointFactory implements EndpointFactory {
        @Override
        public Endpoint create(final String hostname, final String bucket, final String username, final String password, final int port,
                               final CoreEnvironment env, final RingBuffer<ResponseEvent> responseBuffer) {
            return new SearchEndpoint(hostname, bucket, username, password, port, env, responseBuffer);
        }
    }

}