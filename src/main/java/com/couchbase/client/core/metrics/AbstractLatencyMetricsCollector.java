/**
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
