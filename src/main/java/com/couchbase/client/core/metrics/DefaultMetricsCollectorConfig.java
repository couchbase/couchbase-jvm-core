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
 * The default implementation with a builder for the {@link MetricsCollectorConfig}.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class DefaultMetricsCollectorConfig implements MetricsCollectorConfig {

    public static final long EMIT_FREQUENCY = 1;
    public static final TimeUnit EMIT_FREQUENCY_UNIT = TimeUnit.HOURS;

    private final long emitFrequency;
    private final TimeUnit emitFrequencyUnit;

    public static DefaultMetricsCollectorConfig create() {
        return new DefaultMetricsCollectorConfig(builder());
    }

    public static DefaultMetricsCollectorConfig create(long emitFrequency, TimeUnit emitFrequencyUnit) {
        Builder builder = builder();
        builder.emitFrequency(emitFrequency);
        builder.emitFrequencyUnit(emitFrequencyUnit);
        return builder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    protected DefaultMetricsCollectorConfig(Builder builder) {
        emitFrequency = builder.emitFrequency;
        emitFrequencyUnit = builder.emitFrequencyUnit;
    }

    @Override
    public long emitFrequency() {
        return emitFrequency;
    }

    @Override
    public TimeUnit emitFrequencyUnit() {
        return emitFrequencyUnit;
    }

    public static class Builder {

        private long emitFrequency = EMIT_FREQUENCY;
        private TimeUnit emitFrequencyUnit = EMIT_FREQUENCY_UNIT;

        protected Builder() {
        }

        /**
         * Overrides the default emit frequency for this metric collector.
         *
         * @param emitFrequency the custom emit frequency.
         */
        public Builder emitFrequency(long emitFrequency) {
            this.emitFrequency = emitFrequency;
            return this;
        }

        /**
         * Overrides the default emit frequency unit for this metric collector.
         *
         * @param emitFrequencyUnit the custom emit frequency unit.
         */
        public Builder emitFrequencyUnit(TimeUnit emitFrequencyUnit) {
            this.emitFrequencyUnit = emitFrequencyUnit;
            return this;
        }

        public DefaultMetricsCollectorConfig build() {
            return new DefaultMetricsCollectorConfig(this);
        }

    }

}
