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
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.state.LifecycleState;

/**
 * Selects the {@link Endpoint} based on a round-robin selection of connected {@link Endpoint}s.
 *
 * @author Simon Baslé
 * @since 1.3
 */
public class RoundRobinSelectionStrategy implements SelectionStrategy {

    protected volatile int skip = 0;

    /**
     * Selects an {@link Endpoint} for the given {@link CouchbaseRequest}.
     * <p/>
     * If null is returned, it means that no endpoint could be selected and it is up to the calling party
     * to decide what to do next.
     *
     * @param request   the input request.
     * @param endpoints all the available endpoints.
     * @return the selected endpoint.
     */
    @Override
    public Endpoint select(CouchbaseRequest request, Endpoint[] endpoints) {
        int endpointSize = endpoints.length;
        //increments skip and prevents it to overflow to a negative value
        skip = Math.max(0, skip+1);
        int offset = skip % endpointSize;

        //attempt to find a CONNECTED endpoint at the offset, or try following ones
        for (int i = offset; i < endpointSize; i++) {
            Endpoint endpoint = endpoints[i];
            if (endpoint.isState(LifecycleState.CONNECTED)) {
                return endpoint;
            }
        }

        //arriving here means the offset endpoint wasn't CONNECTED and none of the endpoints after it were.
        //wrap around and try from the beginning of the array
        for (int i = 0; i < offset; i++) {
            Endpoint endpoint = endpoints[i];
            if (endpoint.isState(LifecycleState.CONNECTED)) {
                return endpoint;
            }
        }

        //lastly, really no eligible endpoint was found, return null
        return null;
    }

    /**
     * Force a value to the skip counter, mainly for testing purposes.
     *
     * @param newValue the new skip value to apply.
     */
    protected void setSkip(int newValue) {
        if (newValue < 0)
            skip = 0;
        else
            skip = newValue;
    }
}
