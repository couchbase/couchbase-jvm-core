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

import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.metrics.LatencyMetric;
import com.couchbase.client.core.event.metrics.NetworkLatencyMetricsEvent;
import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import rx.Scheduler;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * The collector for core network latencies.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class NetworkLatencyMetricsCollector
    extends AbstractLatencyMetricsCollector<NetworkLatencyMetricsIdentifier, NetworkLatencyMetricsEvent> {

    private final TimeUnit targetUnit;
    private final Double[] targetPercentiles;

    public NetworkLatencyMetricsCollector(EventBus eventBus, Scheduler scheduler, LatencyMetricsCollectorConfig config) {
        super(eventBus, scheduler, config);

        targetUnit = config.targetUnit();
        targetPercentiles = config.targetPercentiles();
    }

    @Override
    protected NetworkLatencyMetricsEvent generateLatencyMetricsEvent(
        final Map<NetworkLatencyMetricsIdentifier, LatencyStats> latencyMetrics) {

        Map<NetworkLatencyMetricsIdentifier, LatencyMetric> sortedMetrics =
            new TreeMap<NetworkLatencyMetricsIdentifier, LatencyMetric>();


        for (Map.Entry<NetworkLatencyMetricsIdentifier, LatencyStats> entry : latencyMetrics.entrySet()) {
            Histogram histogram = entry.getValue().getIntervalHistogram();

            if (histogram.getTotalCount() == 0) {
                // no events have been found on this identifier in the last interval, so remove it and
                // do not include it in the output.
                remove(entry.getKey());
                continue;
            }

            Map<Double, Long> percentiles = new TreeMap<Double, Long>();
            for (double targetPercentile : targetPercentiles) {
                percentiles.put(targetPercentile, targetUnit.convert(
                    histogram.getValueAtPercentile(targetPercentile), TimeUnit.NANOSECONDS)
                );
            }

            sortedMetrics.put(entry.getKey(), new LatencyMetric(
                targetUnit.convert(histogram.getMinValue(), TimeUnit.NANOSECONDS),
                targetUnit.convert(histogram.getMaxValue(), TimeUnit.NANOSECONDS),
                histogram.getTotalCount(),
                percentiles,
                targetUnit
            ));
        }

        return new NetworkLatencyMetricsEvent(sortedMetrics);
    }
}
