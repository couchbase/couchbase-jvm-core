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

import java.util.concurrent.TimeUnit;

/**
 * The default configuration for the latency metrics collectors.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class DefaultLatencyMetricsCollectorConfig
    extends DefaultMetricsCollectorConfig
    implements LatencyMetricsCollectorConfig {

    public static final TimeUnit TARGET_UNIT = TimeUnit.MICROSECONDS;
    public static final Double[] TARGET_PERCENTILES = new Double[] { 50.0, 90.0, 95.0, 99.0, 99.9 };

    private final TimeUnit targetUnit;
    private final Double[] targetPercentiles;

    public static DefaultLatencyMetricsCollectorConfig create() {
        return new DefaultLatencyMetricsCollectorConfig(builder());
    }

    public static DefaultLatencyMetricsCollectorConfig create(long emitFrequency, TimeUnit emitFrequencyUnit) {
        Builder builder = builder();
        builder.emitFrequency(emitFrequency);
        builder.emitFrequencyUnit(emitFrequencyUnit);
        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private DefaultLatencyMetricsCollectorConfig(Builder builder) {
        super(builder);

        this.targetUnit = builder.targetUnit;
        this.targetPercentiles = builder.targetPercentiles;
    }

    @Override
    public TimeUnit targetUnit() {
        return targetUnit;
    }

    @Override
    public Double[] targetPercentiles() {
        return targetPercentiles;
    }

    public static class Builder extends DefaultMetricsCollectorConfig.Builder {

        private TimeUnit targetUnit = TARGET_UNIT;
        private Double[] targetPercentiles = TARGET_PERCENTILES;

        protected Builder() {
        }

        /**
         * Overrides the target unit for the latencies recorded.
         *
         * Note that latencies are always recorded with nanosecond precision, but are converted before the
         * event is emitted based on the time unit provided here.
         *
         * @param targetUnit the target unit.
         */
        public Builder targetUnit(TimeUnit targetUnit) {
            this.targetUnit = targetUnit;
            return this;
        }

        /**
         * Overrides the emitted percentiles.
         *
         * Note that all kinds of percentiles between 0.1 and 99.999* can be provided, since all values are recorded
         * as part of the internal histogram.
         *
         * @param targetPercentiles the percentiles which should be emitted.
         */
        public Builder targetPercentiles(Double[] targetPercentiles) {
            this.targetPercentiles = targetPercentiles;
            return this;
        }

        @Override
        public Builder emitFrequency(long emitFrequency) {
            super.emitFrequency(emitFrequency);
            return this;
        }

        @Override
        public Builder emitFrequencyUnit(TimeUnit emitFrequencyUnit) {
            super.emitFrequencyUnit(emitFrequencyUnit);
            return this;
        }

        public DefaultLatencyMetricsCollectorConfig build() {
            return new DefaultLatencyMetricsCollectorConfig(this);
        }
    }

}
