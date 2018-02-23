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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import io.opentracing.Span;

import java.util.Collections;
import java.util.Map;

public class ThresholdLogSpan implements Span, Comparable<ThresholdLogSpan> {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ThresholdLogSpan.class);

    private final ThresholdLogTracer tracer;
    private final Map<String, Object> tags;
    private final ThresholdLogSpanContext context;
    private final long startTimeMicroseconds;
    private long endTimeMicroseconds;
    private String operationName;
    private boolean finished;

    /**
     * If set, associates this span with a couchbase request.
     */
    private volatile CouchbaseRequest request;

    ThresholdLogSpan(final ThresholdLogTracer tracer, final ThresholdLogSpanContext context,
        final String operationName, final Map<String, Object> tags, final long startTimeMicroseconds) {
        this.context = context;
        this.tracer = tracer;
        this.operationName = operationName;
        this.tags = tags;
        this.startTimeMicroseconds = startTimeMicroseconds;
        finished = false;
    }

    @Override
    public ThresholdLogSpanContext context() {
        return context;
    }

    @Override
    public synchronized ThresholdLogSpan setTag(String key, String value) {
        tags.put(key, value);
        return this;
    }

    @Override
    public synchronized ThresholdLogSpan setTag(String key, boolean value) {
        tags.put(key, value);
        return this;
    }

    @Override
    public synchronized ThresholdLogSpan setTag(String key, Number value) {
        tags.put(key, value);
        return this;
    }

    @Override
    public ThresholdLogSpan log(Map<String, ?> fields) {
        // this tracer doesn't need log functionality...
        return this;
    }

    @Override
    public ThresholdLogSpan log(String event) {
        // this tracer doesn't need log functionality...
        return this;
    }

    @Override
    public ThresholdLogSpan log(long timestampMicroseconds, Map<String, ?> fields) {
        // this tracer doesn't need log functionality...
        return this;
    }

    @Override
    public ThresholdLogSpan log(long timestampMicroseconds, String event) {
        // this tracer doesn't need log functionality...
        return this;
    }

    @Override
    public ThresholdLogSpan setBaggageItem(String key, String value) {
        context.baggageItem(key, value);
        return this;
    }

    @Override
    public String getBaggageItem(String key) {
        return context.baggageItem(key);
    }

    @Override
    public synchronized ThresholdLogSpan setOperationName(String operationName) {
        this.operationName = operationName;
        return this;
    }

    @Override
    public int compareTo(final ThresholdLogSpan o) {
        return ((Long) durationMicros()).compareTo(o.durationMicros());
    }

    @Override
    public void finish() {
        finish(ThresholdLogTracer.currentTimeMicros());
    }

    /**
     * Returns the relative duration in microseconds.
     */
    public synchronized long durationMicros() {
        return endTimeMicroseconds - startTimeMicroseconds;
    }

    /**
     * Returns the operation name.
     */
    public synchronized String operationName() {
        return operationName;
    }

    /**
     * Returns the set tags.
     */
    public Map<String, Object> tags() {
        return Collections.unmodifiableMap(tags);
    }

    /**
     * Returns a tag if set, null otherwise.
     *
     * @param key the key to check.
     * @return returns the value or null if not set.
     */
    public synchronized Object tag(final String key) {
        return tags.get(key);
    }

    public CouchbaseRequest request() {
        return request;
    }

    public void request(CouchbaseRequest request) {
        this.request = request;
    }

    @Override
    public void finish(long finishMicros) {
        synchronized (this) {
            if (finished) {
                LOGGER.warn("Span has already been finished; will not be reported again.");
                return;
            }
            endTimeMicroseconds = finishMicros;
            finished = true;
        }
        tracer.reportSpan(this);
    }

    @Override
    public String toString() {
        return "ThresholdLogSpan{" +
            ", tags=" + tags +
            ", context=" + context +
            ", startTimeMicroseconds=" + startTimeMicroseconds +
            ", endTimeMicroseconds=" + endTimeMicroseconds +
            ", operationName='" + operationName + '\'' +
            ", finished=" + finished +
            '}';
    }
}
