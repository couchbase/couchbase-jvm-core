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
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link BestEffortRetryStrategy}.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class BestEffortRetryStrategyTest {

    @Test
    public void shouldRetryWhileUnderMaxTime() {
        BestEffortRetryStrategy strategy = BestEffortRetryStrategy.INSTANCE;

        CouchbaseRequest request = mock(CouchbaseRequest.class);
        when(request.creationTime()).thenReturn(System.nanoTime());
        CoreEnvironment env = mock(CoreEnvironment.class);
        when(env.maxRequestLifetime()).thenReturn(10000L);

        assertTrue(strategy.shouldRetry(request, env));
    }

    @Test
    public void shouldCancelWhenOverMaxTime() {
        BestEffortRetryStrategy strategy = BestEffortRetryStrategy.INSTANCE;

        CouchbaseRequest request = mock(CouchbaseRequest.class);
        long backInTime = TimeUnit.SECONDS.toNanos(3);
        when(request.creationTime()).thenReturn(System.nanoTime() - backInTime);
        CoreEnvironment env = mock(CoreEnvironment.class);
        when(env.maxRequestLifetime()).thenReturn(1000L);

        assertFalse(strategy.shouldRetry(request, env));
    }
}