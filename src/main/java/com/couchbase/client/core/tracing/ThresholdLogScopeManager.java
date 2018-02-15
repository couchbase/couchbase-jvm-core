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

/**
 * A {@link ScopeManager} implementation built on top of Java's thread-local storage primitive,
 * very similar to the one shipped with "opentracing-util".
 *
 * @author Michael Nitschinger
 * @since 1.6.0
 */
public class ThresholdLogScopeManager implements ScopeManager {

    final ThreadLocal<ThresholdLogScope> scope = new ThreadLocal<ThresholdLogScope>();

    @Override
    public Scope activate(Span span, boolean finishSpanOnClose) {
        return new ThresholdLogScope(this, span, finishSpanOnClose);
    }

    @Override
    public Scope active() {
        return scope.get();
    }

}
