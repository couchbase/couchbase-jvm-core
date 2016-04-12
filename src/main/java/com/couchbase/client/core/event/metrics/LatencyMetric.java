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
package com.couchbase.client.core.event.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    private final TimeUnit unit;

    public LatencyMetric(long min, long max, long count, Map<Double, Long> percentiles, TimeUnit unit) {
        this.min = min;
        this.max = max;
        this.count = count;
        this.percentiles = percentiles;
        this.unit = unit;
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

    /**
     * Returns the time unit for the percentiles, min and max values.
     */
    public TimeUnit timeUnit() {
        return unit;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LatencyMetric{");
        sb.append("min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", count=").append(count);
        sb.append(", percentiles=").append(percentiles);
        sb.append(", timeUnit=").append(unit);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Exports this object to a plain map structure which can be easily converted into other
     * target formats.
     *
     * @return the exported structure.
     */
    public Map<String, Object> export() {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("min", min());
        result.put("max", max());
        result.put("count", count());
        result.put("percentiles", percentiles());
        result.put("timeUnit", timeUnit().toString());
        return result;
    }
}
