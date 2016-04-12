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

/**
 * A generic metrics collector.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public interface MetricsCollector {

    /**
     * Its high-level configuration.
     */
    MetricsCollectorConfig config();

    /**
     * Shuts down the collector (non-reversible) and frees bound resources.
     */
    boolean shutdown();

    /**
     * True if this collector actually emits something.
     */
    boolean isEnabled();

    /**
     * Triggers the immediate emission of whatever is currently collected. Useful for testing.
     */
    void triggerEmit();

}
