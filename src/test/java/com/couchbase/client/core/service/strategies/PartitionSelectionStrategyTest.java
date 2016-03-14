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
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.state.LifecycleState;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link PartitionSelectionStrategy}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class PartitionSelectionStrategyTest {

    @Test
    public void shouldSelectFirstConnectedForBucketConfigRequest() throws Exception {
        SelectionStrategy strategy = PartitionSelectionStrategy.INSTANCE;

        Endpoint endpoint1 = mock(Endpoint.class);
        when(endpoint1.isState(LifecycleState.CONNECTED)).thenReturn(false);
        Endpoint endpoint2 = mock(Endpoint.class);
        when(endpoint2.isState(LifecycleState.CONNECTED)).thenReturn(true);
        Endpoint endpoint3 = mock(Endpoint.class);
        when(endpoint3.isState(LifecycleState.CONNECTED)).thenReturn(true);
        Endpoint[] endpoints = new Endpoint[] {endpoint1, endpoint2, endpoint3};

        GetBucketConfigRequest request = mock(GetBucketConfigRequest.class);
        Endpoint selected = strategy.select(request, endpoints);

        assertNotNull(selected);
        assertTrue(selected.equals(endpoint2));
    }

    @Test
    public void shouldSelectFirstConnectedForRequestWithoutPartition() throws Exception {
        SelectionStrategy strategy = PartitionSelectionStrategy.INSTANCE;

        Endpoint endpoint1 = mock(Endpoint.class);
        when(endpoint1.isState(LifecycleState.CONNECTED)).thenReturn(false);
        Endpoint endpoint2 = mock(Endpoint.class);
        when(endpoint2.isState(LifecycleState.CONNECTED)).thenReturn(false);
        Endpoint endpoint3 = mock(Endpoint.class);
        when(endpoint3.isState(LifecycleState.CONNECTED)).thenReturn(true);
        Endpoint[] endpoints = new Endpoint[] {endpoint1, endpoint2, endpoint3};

        GetRequest request = mock(GetRequest.class);
        when(request.partition()).thenReturn((short) -1);
        Endpoint selected = strategy.select(request, endpoints);

        assertNotNull(selected);
        assertTrue(selected.equals(endpoint3));
    }

    @Test
    public void shouldSelectPinnedForBinaryWithKey() throws Exception {
        SelectionStrategy strategy = PartitionSelectionStrategy.INSTANCE;

        Endpoint endpoint1 = mock(Endpoint.class);
        when(endpoint1.isState(LifecycleState.CONNECTED)).thenReturn(true);
        Endpoint endpoint2 = mock(Endpoint.class);
        when(endpoint2.isState(LifecycleState.CONNECTED)).thenReturn(true);
        Endpoint endpoint3 = mock(Endpoint.class);
        when(endpoint3.isState(LifecycleState.CONNECTED)).thenReturn(true);
        Endpoint[] endpoints = new Endpoint[] {endpoint1, endpoint2, endpoint3};

        GetRequest request = mock(GetRequest.class);
        when(request.partition()).thenReturn((short) 12);
        Endpoint selected = strategy.select(request, endpoints);

        for (int i = 0; i < 1000; i++) {
            assertNotNull(selected);
            assertTrue(selected.equals(endpoint1));
        }
    }

    @Test
    public void shouldSelectNullIfPinedIsNotConnected() throws Exception {
        SelectionStrategy strategy = PartitionSelectionStrategy.INSTANCE;

        Endpoint endpoint1 = mock(Endpoint.class);
        when(endpoint1.isState(LifecycleState.CONNECTED)).thenReturn(false);
        Endpoint endpoint2 = mock(Endpoint.class);
        when(endpoint2.isState(LifecycleState.CONNECTED)).thenReturn(true);
        Endpoint endpoint3 = mock(Endpoint.class);
        when(endpoint3.isState(LifecycleState.CONNECTED)).thenReturn(true);
        Endpoint[] endpoints = new Endpoint[] {endpoint1, endpoint2, endpoint3};

        GetRequest request = mock(GetRequest.class);
        when(request.partition()).thenReturn((short) 12);
        Endpoint selected = strategy.select(request, endpoints);

        for (int i = 0; i < 1000; i++) {
            assertNull(selected);
        }
    }

    @Test
    public void shouldReturnIfEmptyArrayPassedIn() {
        SelectionStrategy strategy = PartitionSelectionStrategy.INSTANCE;

        Endpoint selected = strategy.select(mock(CouchbaseRequest.class),  new Endpoint[] {});
        assertNull(selected);
    }
}