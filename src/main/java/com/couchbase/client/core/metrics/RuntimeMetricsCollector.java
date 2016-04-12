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
package com.couchbase.client.core.metrics;

import com.couchbase.client.core.env.Diagnostics;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.metrics.RuntimeMetricsEvent;
import rx.Scheduler;

import java.util.Map;
import java.util.TreeMap;

/**
 * A {@link MetricsCollector} which collects and emits system information like gc, memory or thread usage.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class RuntimeMetricsCollector extends AbstractMetricsCollector {

    public RuntimeMetricsCollector(final EventBus eventBus, Scheduler scheduler, MetricsCollectorConfig config) {
        super(eventBus, scheduler, config);
    }

    @Override
    protected CouchbaseEvent generateCouchbaseEvent() {
        Map<String, Object> metrics = new TreeMap<String, Object>();

        Diagnostics.gcInfo(metrics);
        Diagnostics.memInfo(metrics);
        Diagnostics.threadInfo(metrics);

        return new RuntimeMetricsEvent(metrics);
    }

}
