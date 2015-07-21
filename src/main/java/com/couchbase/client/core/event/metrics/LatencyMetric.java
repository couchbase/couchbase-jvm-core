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
package com.couchbase.client.core.event.metrics;

import java.util.Map;

/**
 * A latency metric value object which contains percentile and other related information.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class LatencyMetric {

    private final long min;
    private final long max;
    private final long count;
    private final Map<Double, Long> percentiles;

    public LatencyMetric(long min, long max, long count, Map<Double, Long> percentiles) {
        this.min = min;
        this.max = max;
        this.count = count;
        this.percentiles = percentiles;
    }

    /**
     * The minimum latency recorded in the interval.
     */
    public long min() {
        return min;
    }

    /**
     * The maximum latency recorded in the interval.
     */
    public long max() {
        return max;
    }

    /**
     * The number of latency entries recorded in the interval.
     */
    public long count() {
        return count;
    }

    /**
     * Configured latencies with their values recorded in the interval.
     */
    public Map<Double, Long> percentiles() {
        return percentiles;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LatencyMetric{");
        sb.append("min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", count=").append(count);
        sb.append(", percentiles=").append(percentiles);
        sb.append('}');
        return sb.toString();
    }
}
