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
package com.couchbase.client.core.service.strategies;

import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.state.LifecycleState;

/**
 * Selects the {@link Endpoint} based on the information enclosed in the {@link CouchbaseRequest}.
 *
 * This strategy can be used to "pin" certain requests to specific endpoints based on the supplied information. The
 * current implementation uses this technique to tie ID-based {@link BinaryRequest}s to the same endpoint to enforce
 * at least some amount of ordering guarantees.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class PartitionSelectionStrategy implements SelectionStrategy {

    public static final PartitionSelectionStrategy INSTANCE = new PartitionSelectionStrategy();

    private PartitionSelectionStrategy() {
        // singleton.
    }

    @Override
    public Endpoint select(final CouchbaseRequest request, final Endpoint[] endpoints) {
        if (endpoints.length == 0) {
            return null;
        }

        if (request instanceof BinaryRequest) {
            if (request instanceof GetBucketConfigRequest) {
                return selectFirstConnected(endpoints);
            } else {
                return selectByPartition(endpoints, ((BinaryRequest) request).partition());
            }
        } else {
            throw new IllegalStateException("The PartitionSelectionStrategy does not understand: " + request);
        }
    }

    /**
     * Helper method to select the proper target endpoint by partition.
     *
     * @param endpoints the list of currently available endpoints.
     * @param partition the partition of the incoming request.
     * @return the selected endpoint, or null if no acceptable one found.
     */
    private static Endpoint selectByPartition(final Endpoint[] endpoints, final short partition) {
        if (partition >= 0) {
            int numEndpoints = endpoints.length;
            Endpoint endpoint = numEndpoints == 1 ? endpoints[0] : endpoints[partition % numEndpoints];
            if (endpoint != null && endpoint.isState(LifecycleState.CONNECTED)) {
                return endpoint;
            }
            return null;
        } else {
            return selectFirstConnected(endpoints);
        }
    }

    /**
     * Helper method to select the first connected endpoint if no particular pinning is needed.
     *
     * @param endpoints the list of endpoints.
     * @return the first connected or null if none found.
     */
    private static Endpoint selectFirstConnected(final Endpoint[] endpoints) {
        for (Endpoint endpoint : endpoints) {
            if (endpoint.isState(LifecycleState.CONNECTED)) {
                return endpoint;
            }
        }
        return null;
    }
}
