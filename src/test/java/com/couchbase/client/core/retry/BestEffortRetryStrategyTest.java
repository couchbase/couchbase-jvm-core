/**
 * Copyright (c) 2015 Couchbase, Inc.
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