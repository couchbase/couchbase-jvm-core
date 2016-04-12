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
 * A generic configuration for a metrics collector.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public interface MetricsCollectorConfig {

    /**
     * The interval frequency when to emit the metrics.
     *
     * Defaults to {@link DefaultMetricsCollectorConfig#EMIT_FREQUENCY}.
     */
    long emitFrequency();

    /**
     * The time unit for the emit frequency.
     *
     * Defaults to {@link DefaultMetricsCollectorConfig#EMIT_FREQUENCY_UNIT}.
     */
    TimeUnit emitFrequencyUnit();

}
