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

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.service.ServiceType;

import java.util.Iterator;
import java.util.Map;

/**
 * Provides a granular breakdown of the queries in the ringbuffer
 *
 * @author Graham Pople
 * @since 2.6.0
 */
@InterfaceAudience.Public
@InterfaceStability.Experimental
public class RingBufferDiagnostics {
    private final Map<ServiceType, Integer> counts;
    private final int countNonService;

    /**
     * Returns a map of the type of each request, along with the current count of each in the ringbuffer
     */
    @InterfaceAudience.Public
    @InterfaceStability.Experimental
    public Map<ServiceType, Integer> counts() {
        return counts;
    }

    /**
     * Returns the count of all requests not associated with a particular service
     */
    @InterfaceAudience.Public
    @InterfaceStability.Experimental
    public int countNonService() {
        return countNonService;
    }

    /**
     * Returns the count of all requests in the ringbuffer
     */
    @InterfaceAudience.Public
    @InterfaceStability.Experimental
    public int totalCount() {
        int total = countNonService;
        for (Map.Entry<ServiceType, Integer> entry : counts.entrySet()) {
            total += entry.getValue();
        }
        return total;
    }

    @InterfaceAudience.Public
    @InterfaceStability.Experimental
    public RingBufferDiagnostics(Map<ServiceType, Integer> counts, int countNonService) {
        this.counts = counts;
        this.countNonService = countNonService;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int total = countNonService;

        for (Map.Entry<ServiceType, Integer> entry : counts.entrySet()) {
            total += entry.getValue();
            sb.append(entry.getKey().name())
                    .append('=')
                    .append(entry.getValue())
                    .append(' ');
        }

        sb.append("other=")
                .append(countNonService)
                .append(" total=")
                .append(total);
        return sb.toString();
    }
}
