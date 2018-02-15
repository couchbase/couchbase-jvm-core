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
import io.opentracing.Span;

/**
 * {@link ThresholdLogScope} is a simple {@link Scope} implementation that relies on Java's
 * thread-local storage primitive, very similar to the one shipped with "opentracing-util".
 *
 * @author Michael Nitschinger
 * @since 1.6.0
 */
public class ThresholdLogScope implements Scope {

    private final ThresholdLogScopeManager scopeManager;
    private final Span wrapped;
    private final boolean finishOnClose;
    private final ThresholdLogScope toRestore;

    /**
     * Creates a new {@link ThresholdLogScope}.
     *
     * @param scopeManager the scope manager to reference.
     * @param wrapped the wrapped span.
     * @param finishOnClose if close is called, finish the span.
     */
    ThresholdLogScope(final ThresholdLogScopeManager scopeManager, final Span wrapped,
                      final boolean finishOnClose) {
        this.scopeManager = scopeManager;
        this.wrapped = wrapped;
        this.finishOnClose = finishOnClose;
        this.toRestore = scopeManager.scope.get();
        scopeManager.scope.set(this);
    }

    @Override
    public void close() {
        if (scopeManager.scope.get() != this) {
            return;
        }
        if (finishOnClose) {
            wrapped.finish();
        }
        scopeManager.scope.set(toRestore);
    }

    @Override
    public Span span() {
        return wrapped;
    }
}
