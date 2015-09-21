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
