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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The default abstract implementation for a latency metrics collector.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public abstract class AbstractLatencyMetricsCollector<I extends LatencyMetricsIdentifier, E extends LatencyMetricsEvent>
    extends AbstractMetricsCollector
    implements LatencyMetricsCollector<I> {

    private final static Object PAUSE_DETECTOR_LOCK = new Object();
    private static int pauseDetectorCount = 0;
    private static PauseDetector staticPauseDetector;

    private static PauseDetector acquirePauseDetector() {
        synchronized (PAUSE_DETECTOR_LOCK) {
            if (pauseDetectorCount++ == 0) {
                staticPauseDetector = new SimplePauseDetector(
                    TimeUnit.MILLISECONDS.toNanos(10),
                    TimeUnit.MILLISECONDS.toNanos(10),
                    3
                );
            }
            return staticPauseDetector;
        }
    }

    private static void releasePauseDetector() {
        synchronized (PAUSE_DETECTOR_LOCK) {
            if (--pauseDetectorCount == 0) {
                staticPauseDetector.shutdown();
                staticPauseDetector = null; // help GC
            }
        }
    }

    private final PauseDetector pauseDetector;
    private final AtomicBoolean pauseDetectorHeld;

    private final Map<I, LatencyStats> latencyMetrics;
    private final LatencyMetricsCollectorConfig config;

    protected AbstractLatencyMetricsCollector(EventBus eventBus, Scheduler scheduler, LatencyMetricsCollectorConfig config) {
        super(eventBus, scheduler, config);
        this.config = config;
        latencyMetrics = new ConcurrentHashMap<I, LatencyStats>();
        pauseDetector = acquirePauseDetector();
        pauseDetectorHeld = new AtomicBoolean(true);
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
            metric = LatencyStats.Builder.create().pauseDetector(pauseDetector).build();
            latencyMetrics.put(identifier, metric);
        }
        metric.recordLatency(latency);
    }

    @Override
    public boolean shutdown() {
        if (pauseDetectorHeld.compareAndSet(true, false)) {
            releasePauseDetector();
        }
        return super.shutdown();
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
