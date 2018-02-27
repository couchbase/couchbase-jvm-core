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

package com.couchbase.client.core;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.env.CoreEnvironment;
import com.lmax.disruptor.RingBuffer;

/**
 * The {@link CoreContext} contains required assets which are shared on
 * a per-core basis.
 *
 * @author Michael Nitschinger
 * @since 1.5.6
 */
@InterfaceAudience.Private
@InterfaceStability.Uncommitted
public class CoreContext {

    private final CoreEnvironment environment;
    private final RingBuffer<ResponseEvent> responseRingBuffer;
    private final long coreId;

    /**
     * Creates a new {@link CoreContext} with no core id.
     *
     * @param environment the environment to share.
     * @param responseRingBuffer the response ring buffer to share.
     */
    public CoreContext(final CoreEnvironment environment,
        final RingBuffer<ResponseEvent> responseRingBuffer) {
        this(environment, responseRingBuffer, 0);
    }

    /**
     * Creates a new {@link CoreContext} with no a core id.
     *
     * @param environment the environment to share.
     * @param responseRingBuffer the response ring buffer to share.
     * @param coreId the core id to use.
     */
    public CoreContext(final CoreEnvironment environment,
        final RingBuffer<ResponseEvent> responseRingBuffer,
        final long coreId) {
        this.environment = environment;
        this.responseRingBuffer = responseRingBuffer;
        this.coreId = coreId;
    }

    /**
     * Returns the current environment.
     */
    public CoreEnvironment environment() {
        return environment;
    }

    /**
     * Returns the response ring buffer.
     */
    public RingBuffer<ResponseEvent> responseRingBuffer() {
        return responseRingBuffer;
    }

    /**
     * The core it, 0 if not set.
     */
    public long coreId() {
        return coreId;
    }
}
