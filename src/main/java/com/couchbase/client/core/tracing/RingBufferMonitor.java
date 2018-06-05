/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.tracing;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.analytics.AnalyticsRequest;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.kv.AbstractKeyValueRequest;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.search.SearchRequest;
import com.couchbase.client.core.message.view.ViewRequest;
import com.couchbase.client.core.service.ServiceType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stores diagnostics on the main ringbuffer to provide additional context to BackpressureExceptions.
 *
 * @author Graham Pople
 * @since 1.6.0
 */
@InterfaceAudience.Private
@InterfaceStability.Experimental
public class RingBufferMonitor {
    private final AtomicInteger countKeyValue = new AtomicInteger();
    private final AtomicInteger countQuery = new AtomicInteger();
    private final AtomicInteger countConfig = new AtomicInteger();
    private final AtomicInteger countSearch = new AtomicInteger();
    private final AtomicInteger countView = new AtomicInteger();
    private final AtomicInteger countAnalytics = new AtomicInteger();
    private final AtomicInteger countNonService = new AtomicInteger();
    // If adding a new counter, add it to reset()

    public void addRequest(CouchbaseRequest request) {
        AtomicInteger count = counterFor(request);
        count.incrementAndGet();
    }

    public void removeRequest(CouchbaseRequest request) {
        AtomicInteger count = counterFor(request);
        count.decrementAndGet();
    }

    public void reset() {
        countKeyValue.set(0);
        countQuery.set(0);
        countConfig.set(0);
        countSearch.set(0);
        countView.set(0);
        countAnalytics.set(0);
        countNonService.set(0);
    }

    public static RingBufferMonitor instance() {
        return instance;
    }

    private static final RingBufferMonitor instance = new RingBufferMonitor();

    private RingBufferMonitor() {
    }

    public BackpressureException createException() {
        // There's a race window here, but don't want to add the overhead of locking for simple diagnostics
        Map<ServiceType, Integer> counts = new HashMap<ServiceType, Integer>(6);

        counts.put(ServiceType.BINARY, countKeyValue.get());
        counts.put(ServiceType.QUERY, countQuery.get());
        counts.put(ServiceType.CONFIG, countConfig.get());
        counts.put(ServiceType.SEARCH, countSearch.get());
        counts.put(ServiceType.VIEW, countView.get());
        counts.put(ServiceType.ANALYTICS, countAnalytics.get());
        // If adding a service type, bump up initialCapacity of counts

        RingBufferDiagnostics diag = new RingBufferDiagnostics(counts, countNonService.get());

        return new BackpressureException(diag);
    }

    private AtomicInteger counterFor(CouchbaseRequest request) {
        // instanceof is used instead of a more generic Map-based solution purely to provide lock-free performance
        if (request instanceof GenericQueryRequest) {
            return countQuery;
        }
        else if (request instanceof ConfigRequest) {
            return countConfig;
        }
        else if (request instanceof BinaryRequest) {
            // AbstractKeyValueRequest implements BinaryRequest
            return countKeyValue;
        }
        else if (request instanceof SearchRequest) {
            return countSearch;
        }
        else if (request instanceof ViewRequest) {
            return countView;
        }
        else if (request instanceof AnalyticsRequest) {
            return countAnalytics;
        }
        else {
            return countNonService;
        }
    }
}
