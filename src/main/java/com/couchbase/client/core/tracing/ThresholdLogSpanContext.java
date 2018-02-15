/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.tracing;

import io.opentracing.SpanContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds baggage items for the slow operation spans.
 *
 * This class is thread safe!
 *
 * @author Michael Nitschinger
 * @since 1.6.0
 */
public class ThresholdLogSpanContext implements SpanContext {

    /**
     * Internal structure to hold the baggage items.
     */
    private final Map<String, String> baggageItems;

    /**
     * Creates a new {@link ThresholdLogSpanContext} with empty baggage.
     */
    public ThresholdLogSpanContext() {
        this.baggageItems = new ConcurrentHashMap<String, String>();
    }

    @Override
    public Iterable<Map.Entry<String, String>> baggageItems() {
        return new HashMap<String, String>(baggageItems).entrySet();
    }

    /**
     * Stores the given baggage item/value.
     *
     * If an item already exists, it will be overridden.
     *
     * @param item the item to store.
     * @param value the value to store.
     * @return this {@link ThresholdLogSpanContext} for chaining purposes.
     */
    public ThresholdLogSpanContext baggageItem(final String item, final String value) {
        baggageItems.put(item, value);
        return this;
    }

    /**
     * Retrieve the baggage value by item.
     *
     * @param item the item to look up.
     * @return the value if found, null otherwise.
     */
    public String baggageItem(final String item) {
        return baggageItems.get(item);
    }

}
