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

    public static DefaultMetricsCollectorConfig disabled() {
        return create(0, TimeUnit.SECONDS);
    }

    public static DefaultMetricsCollectorConfig create() {
        return new DefaultMetricsCollectorConfig(builder());
    }

    public static DefaultMetricsCollectorConfig create(long emitFrequency, TimeUnit emitFrequencyUnit) {
        Builder builder = builder();
        builder.emitFrequency(emitFrequency);
        builder.emitFrequencyUnit(emitFrequencyUnit);
        return builder.build();
    }

    private static Builder builder() {
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
