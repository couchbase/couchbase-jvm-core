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

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.metrics.LatencyMetricsEvent;
import org.LatencyUtils.LatencyStats;
import org.LatencyUtils.PauseDetector;
import org.LatencyUtils.SimplePauseDetector;
import rx.Scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The default abstract implementation for a latency metrics collector.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public abstract class AbstractLatencyMetricsCollector<I extends LatencyMetricsIdentifier, E extends LatencyMetricsEvent>
    extends AbstractMetricsCollector
    implements LatencyMetricsCollector<I> {

    private static final PauseDetector PAUSE_DETECTOR = new SimplePauseDetector(
        TimeUnit.MILLISECONDS.toNanos(10),
        TimeUnit.MILLISECONDS.toNanos(10),
        3
    );

    static {
        LatencyStats.setDefaultPauseDetector(PAUSE_DETECTOR);
        Runtime.getRuntime().addShutdownHook(new Thread("cb-shutdown-pd") {
            @Override
            public void run() {
                PAUSE_DETECTOR.shutdown();
            }
        });
    }

    private final Map<I, LatencyStats> latencyMetrics;
    private final LatencyMetricsCollectorConfig config;

    protected AbstractLatencyMetricsCollector(EventBus eventBus, Scheduler scheduler, LatencyMetricsCollectorConfig config) {
        super(eventBus, scheduler, config);
        this.config = config;
        latencyMetrics = new ConcurrentHashMap<I, LatencyStats>();

    }

    protected abstract E generateLatencyMetricsEvent(Map<I, LatencyStats> latencyMetrics);

    @Override
    protected CouchbaseEvent generateCouchbaseEvent() {
        return generateLatencyMetricsEvent(new HashMap<I, LatencyStats>(latencyMetrics));
    }

    @Override
    public void record(I identifier, long latency) {
        if (config.emitFrequency() <= 0) {
            return;
        }

        LatencyStats metric = latencyMetrics.get(identifier);
        if (metric == null) {
            metric = new LatencyStats();
            latencyMetrics.put(identifier, metric);
        }
        metric.recordLatency(latency);
    }

    @Override
    public LatencyMetricsCollectorConfig config() {
        return config;
    }

    /**
     * Helper method to remove an item out of the stored metrics.
     */
    protected void remove(I identifier) {
        latencyMetrics.remove(identifier);
    }

}
