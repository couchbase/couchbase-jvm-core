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
import com.couchbase.client.core.state.LifecycleState;

import java.util.Random;

/**
 * Selects the {@link Endpoint} based on a random selection of connected {@link Endpoint}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class RandomSelectionStrategy implements SelectionStrategy {

    /**
     * Random number generator, statically initialized and designed to be reused.
     */
    private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };

    /**
     * The number of times to try to find a suitable endpoint before returning without success.
     */
    private static final int MAX_TRIES = 100;

    @Override
    public Endpoint select(final CouchbaseRequest request, final Endpoint[] endpoints) {
        if (endpoints.length == 0) {
            return null;
        }

        for (int i = 0; i < MAX_TRIES; i++) {
            Endpoint endpoint = endpoints[RANDOM.get().nextInt(endpoints.length)];
            if (endpoint.isState(LifecycleState.CONNECTED)) {
                return endpoint;
            }
        }

        return null;
    }
}
