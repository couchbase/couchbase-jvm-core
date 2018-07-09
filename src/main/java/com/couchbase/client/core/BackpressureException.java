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
package com.couchbase.client.core;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.env.Diagnostics;
import com.couchbase.client.core.tracing.RingBufferDiagnostics;

/**
 * Identifies the need to back off on the supplier side when using a service, because the consumer is overloaded.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class BackpressureException extends CouchbaseException {
    private RingBufferDiagnostics diagnostics;

    public BackpressureException() {}

    public BackpressureException(RingBufferDiagnostics diagnostics) {
        super(makeString(diagnostics));
        this.diagnostics = diagnostics;
    }

    /**
     * Returns a {@link RingBufferDiagnostics} which, if non-null, gives a granular breakdown of the contents of the
     * ringbuffer at the time of this exception
     */
    @InterfaceAudience.Public
    @InterfaceStability.Experimental
    public RingBufferDiagnostics diagnostics() {
        return diagnostics;
    }

    @Override
    public String toString() {
        return makeString(diagnostics);
    }

    private static String makeString(RingBufferDiagnostics diag) {
        StringBuilder sb = new StringBuilder("Backpressure, ringbuffer contains ");
        sb.append(diag == null ? "unavailable" : diag.toString());
        return sb.toString();
    }
}
