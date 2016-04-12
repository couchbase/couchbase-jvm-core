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
package com.couchbase.client.core.event.metrics;

import com.couchbase.client.core.metrics.NetworkLatencyMetricsIdentifier;
import com.couchbase.client.core.utils.Events;

import java.util.HashMap;
import java.util.Map;

/**
 * This event represents network latencies captured in the core layer.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class NetworkLatencyMetricsEvent extends LatencyMetricsEvent<NetworkLatencyMetricsIdentifier> {

    public NetworkLatencyMetricsEvent(Map<NetworkLatencyMetricsIdentifier, LatencyMetric> latencies) {
        super(latencies);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = Events.identityMap(this);

        for (Map.Entry<NetworkLatencyMetricsIdentifier, LatencyMetric> metric : latencies().entrySet()) {
            NetworkLatencyMetricsIdentifier ident = metric.getKey();
            Map<String, Object> host = getOrCreate(ident.host(), result);
            Map<String, Object> service = getOrCreate(ident.service(), host);
            Map<String, Object> request = getOrCreate(ident.request(), service);
            Map<String, Object> status = getOrCreate(ident.status(), request);
            status.put("metrics", metric.getValue().export());
        }
        return result;
    }

    @SuppressWarnings({"unchecked"})
    private Map<String, Object> getOrCreate(String key, Map<String, Object> source) {
        Map<String, Object> found = (Map<String, Object>) source.get(key);
        if (found == null) {
            found = new HashMap<String, Object>();
            source.put(key, found);
        }
        return found;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NetworkLatencyMetricsEvent");
        sb.append(toMap().toString());
        return sb.toString();
    }

}
