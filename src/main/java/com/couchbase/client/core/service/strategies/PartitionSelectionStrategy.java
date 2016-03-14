/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
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
