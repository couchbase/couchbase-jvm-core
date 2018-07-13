/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import com.couchbase.client.core.service.Service;

/**
 * Defines a general config for a {@link Service}.
 *
 * @author Michael Nitschinger
 * @since 1.4.2
 */
public abstract class AbstractServiceConfig {

    /**
     * Constant to use if no idle time should be used.
     */
    public static final int NO_IDLE_TIME = 0;

    /**
     * The default idle time for pooled services.
     */
    public static final int DEFAULT_IDLE_TIME = 300;

    /**
     * The minimum number of endpoints to be used for this service.
     */
    private final int minEndpoints;

    /**
     * The maximum number of endpoints to be used for this service.
     */
    private final int maxEndpoints;

    /**
     * If this is a pipelined service.
     */
    private final boolean pipelined;

    /**
     * The configured idle time for this service.
     */
    private final int idleTime;

    /**
     * Creates a new service config.
     *
     * @param minEndpoints minimum number of endpoints to be used
     * @param maxEndpoints maximum number of endpoints to be used
     * @param pipelined if this is a pipelined service.
     * @param idleTime the configured idle time
     */
    protected AbstractServiceConfig(int minEndpoints, int maxEndpoints, boolean pipelined, int idleTime) {
        if (minEndpoints < 0 || maxEndpoints < 0) {
            throw new IllegalArgumentException("The minEndpoints and maxEndpoints must not be negative");
        }
        if (maxEndpoints == 0) {
            throw new IllegalArgumentException("The maxEndpoints must be greater than 0");
        }
        if (maxEndpoints < minEndpoints) {
            throw new IllegalArgumentException("The maxEndpoints must not be smaller than mindEndpoints");
        }

        // temporary limitation:
        if (pipelined && (minEndpoints != maxEndpoints)) {
            throw new IllegalArgumentException("Pipelining and non-fixed size of endpoints is "
                + "currently not supported.");
        }

        this.minEndpoints = minEndpoints;
        this.maxEndpoints = maxEndpoints;
        this.pipelined = pipelined;
        this.idleTime = idleTime;
    }

    /**
     * Helper method to check if the idle time is within proper range.
     *
     * This method is refactored out so it can be overridden in test cases.
     *
     * @param idleTime the idle time to check.
     */
    protected void checkIdleTime(final int idleTime) {
        if (idleTime > 0 && idleTime < 10) {
            throw new IllegalArgumentException("Idle time must either be 0 (disabled) or greater than 9 seconds");
        }
    }

    /**
     * The minimum endpoints per node which will always be established.
     */
    public int minEndpoints() {
        return minEndpoints;
    }

    /**
     * The maximum endpoints per node which will be established.
     */
    public int maxEndpoints() {
        return maxEndpoints;
    }

    /**
     * If this service is pipelined (more than one request at the same time on the
     * same socket).
     */
    public boolean isPipelined() {
        return pipelined;
    }

    /**
     * The time in seconds (minimum, approx) after when idle the socket will be closed.
     */
    public int idleTime() {
        return idleTime;
    }

    @Override
    public String toString() {
        return "AbstractServiceConfig{" +
            "minEndpoints=" + minEndpoints +
            ", maxEndpoints=" + maxEndpoints +
            ", pipelined=" + pipelined +
            ", idleTime=" + idleTime +
            '}';
    }

}
