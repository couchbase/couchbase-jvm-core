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

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.util.ThreadLocalScopeManager;

/**
 * This {@link Tracer} implementation logs operations over a certain threshold based
 * on the given configuration.
 *
 * @author Michael Nitschinger
 * @since 1.6.0
 */
public class ThresholdLogTracer implements Tracer {

    /**
     * The {@link ScopeManager} used.
     */
    private final ScopeManager scopeManager;

    /**
     * The Reporter used.
     */
    private final ThresholdLogReporter reporter;

    /**
     * Creates a new tracer with default settings.
     */
    public static ThresholdLogTracer create() {
        return create(ThresholdLogReporter.create());
    }

    /**
     * Creates a new tracer with a custom reporter.
     *
     * @param reporter the reporter to use.
     */
    public static ThresholdLogTracer create(final ThresholdLogReporter reporter) {
        return new ThresholdLogTracer(reporter);
    }

    /**
     * Simple constructor, useful for testing. Disables the log reporter.
     */
    ThresholdLogTracer() {
        this(ThresholdLogReporter.disabled());
    }

    /**
     * Creates a new {@link ThresholdLogTracer}.
     *
     * @param reporter the log reporter to use.
     */
    private ThresholdLogTracer(final ThresholdLogReporter reporter) {
        this.scopeManager = new ThreadLocalScopeManager();
        this.reporter = reporter;
    }

    @Override
    public ScopeManager scopeManager() {
        return scopeManager;
    }

    @Override
    public Span activeSpan() {
        return scopeManager.activeSpan();
    }

    @Override
    public Scope activateSpan(Span span) {
        return scopeManager.activate(span);
    }

    @Override
    public SpanBuilder buildSpan(String operationName) {
        return new ThresholdLogSpanBuilder(this, operationName, scopeManager);
    }

    @Override
    public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
        throw new UnsupportedOperationException("Not supported by the " + getClass().getSimpleName());
    }

    @Override
    public <C> SpanContext extract(Format<C> format, C carrier) {
        throw new UnsupportedOperationException("Not supported by the " + getClass().getSimpleName());
    }

    @Override
    public void close() {

    }

    /**
     * Feeds the span into the reporter for further processing.
     *
     * @param span the param to report.
     */
    public void reportSpan(final ThresholdLogSpan span) {
        reporter.report(span);
    }

    public void shutdown() {
        reporter.shutdown();
    }

}
