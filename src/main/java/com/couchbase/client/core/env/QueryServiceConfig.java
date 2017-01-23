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

public final class QueryServiceConfig extends AbstractServiceConfig {

    private QueryServiceConfig(int minEndpoints, int maxEndpoints, int idleTime) {
        super(minEndpoints, maxEndpoints, false, idleTime);
    }

    public static QueryServiceConfig create(int minEndpoints, int maxEndpoints) {
        return create(minEndpoints, maxEndpoints, DEFAULT_IDLE_TIME);
    }

    public static QueryServiceConfig create(int minEndpoints, int maxEndpoints, int idleTime) {
        if (idleTime > 0 && idleTime < 10) {
            throw new IllegalArgumentException("Idle time must either be 0 (disabled) or greater than 9 seconds");
        }

        return new QueryServiceConfig(minEndpoints, maxEndpoints, idleTime);
    }

    @Override
    public String toString() {
        return "QueryServiceConfig{" +
                "minEndpoints=" + minEndpoints() +
                ", maxEndpoints=" + maxEndpoints() +
                ", pipelined=" + isPipelined() +
                ", idleTime=" + idleTime() +
                '}';
    }

}
