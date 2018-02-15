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

import io.opentracing.References;

/**
 * Represents a reference from one span to another one.
 *
 * @author Michael Nitschinger
 * @since 1.6.0
 */
public class ThresholdLogReference {

    private final ThresholdLogSpanContext spanContext;
    private final String type;

    /**
     * Creates a reference which represents the child of the given context.
     *
     * @param spanContext the context.
     * @return a reference representing this relationship.
     */
    public static ThresholdLogReference childOf(final ThresholdLogSpanContext spanContext) {
        return new ThresholdLogReference(spanContext, References.CHILD_OF);
    }

    /**
     * Creates a reference which represents the follower of the given context.
     *
     * @param spanContext the context.
     * @return a reference representing this relationship.
     */
    public static ThresholdLogReference followsFrom(final ThresholdLogSpanContext spanContext) {
        return new ThresholdLogReference(spanContext, References.FOLLOWS_FROM);
    }

    /**
     * Creates a new reference for the given context.
     *
     * @param spanContext the context to create.
     * @param type the reference type.
     */
    private ThresholdLogReference(final ThresholdLogSpanContext spanContext, final String type) {
        this.spanContext = spanContext;
        this.type = type;
    }

    /**
     * Returns the current span context.
     */
    public ThresholdLogSpanContext spanContext() {
        return spanContext;
    }

    /**
     * Returns the type of reference.
     */
    public String type() {
        return type;
    }

}
