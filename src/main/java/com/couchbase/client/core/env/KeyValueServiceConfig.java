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

/**
 * Allows to configure a KV Service on a per-node basis.
 *
 * @author Michael Nitschinger
 * @since 1.4.2
 */
public final class KeyValueServiceConfig extends AbstractServiceConfig {

    /**
     * Internal constructor to create a KV service config.
     *
     * @param minEndpoints minimum number of endpoints to be used
     * @param maxEndpoints maximum number of endpoints to be used
     */
    private KeyValueServiceConfig(final int minEndpoints, final int maxEndpoints) {
        super(minEndpoints, maxEndpoints, true, NO_IDLE_TIME);
    }

    /**
     * Creates a new configuration for the KV service.
     *
     * Note that because the KV service does not support dynamic pooling, only a fixed
     * number of endpoints per node can be provided. KV connections are expensive to
     * create and should be reused as much as possible.
     *
     * As a rule of thumb, the default of {@link DefaultCoreEnvironment#KEYVALUE_ENDPOINTS}
     * provides the best performance. If the load is very spiky and comes in batches, then
     * increasing the number of endpoints can help to "drain the pipe" faster but comes
     * at the cost of keeping more connections open.
     *
     * @param endpoints the number of endpoints (sockets) per node, fixed.
     * @return the created {@link KeyValueServiceConfig}.
     */
    public static KeyValueServiceConfig create(final int endpoints) {
        return new KeyValueServiceConfig(endpoints, endpoints);
    }

    @Override
    public String toString() {
        return "KeyValueServiceConfig{" +
            "minEndpoints=" + minEndpoints() +
            ", maxEndpoints=" + maxEndpoints() +
            ", pipelined=" + isPipelined() +
            ", idleTime=" + idleTime() +
            '}';
    }

}
