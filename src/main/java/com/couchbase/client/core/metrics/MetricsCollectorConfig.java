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
