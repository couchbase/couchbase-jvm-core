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
package com.couchbase.client.core.retry;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;

import java.util.concurrent.TimeUnit;

/**
 * A {@link RetryStrategy} that will retry until the max request lifetime is reached.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class BestEffortRetryStrategy implements RetryStrategy {

    /**
     * A reusable instance of this strategy.
     */
    public static final BestEffortRetryStrategy INSTANCE = new BestEffortRetryStrategy();

    private BestEffortRetryStrategy() {
    }

    @Override
    public boolean shouldRetry(final CouchbaseRequest request, final CoreEnvironment env) {
        return TimeUnit.MILLISECONDS.toNanos(env.maxRequestLifetime()) > System.nanoTime() - request.creationTime();
    }

    @Override
    public boolean shouldRetryObserve() {
        return true;
    }

    @Override
    public String toString() {
        return "BestEffort";
    }
}
