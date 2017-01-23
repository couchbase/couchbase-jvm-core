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
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.state.LifecycleState;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        when(endpoint1.isFree()).thenReturn(true);
        Endpoint endpoint2 = mock(Endpoint.class);
        when(endpoint2.isState(LifecycleState.CONNECTED)).thenReturn(true);
        when(endpoint2.isFree()).thenReturn(true);
        Endpoint endpoint3 = mock(Endpoint.class);
        when(endpoint3.isState(LifecycleState.CONNECTED)).thenReturn(true);
        when(endpoint3.isFree()).thenReturn(true);
        List<Endpoint> endpoints = Arrays.asList(endpoint1, endpoint2, endpoint3);

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
        when(endpoint1.isFree()).thenReturn(true);
        Endpoint endpoint2 = mock(Endpoint.class);
        when(endpoint2.isState(LifecycleState.CONNECTED)).thenReturn(false);
        when(endpoint2.isFree()).thenReturn(true);
        Endpoint endpoint3 = mock(Endpoint.class);
        when(endpoint3.isState(LifecycleState.CONNECTED)).thenReturn(true);
        when(endpoint3.isFree()).thenReturn(true);
        List<Endpoint> endpoints = Arrays.asList(endpoint1, endpoint2, endpoint3);

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
        when(endpoint1.isFree()).thenReturn(true);
        Endpoint endpoint2 = mock(Endpoint.class);
        when(endpoint2.isState(LifecycleState.CONNECTED)).thenReturn(true);
        when(endpoint2.isFree()).thenReturn(true);
        Endpoint endpoint3 = mock(Endpoint.class);
        when(endpoint3.isState(LifecycleState.CONNECTED)).thenReturn(true);
        when(endpoint3.isFree()).thenReturn(true);
        List<Endpoint> endpoints = Arrays.asList(endpoint1, endpoint2, endpoint3);

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
        when(endpoint1.isFree()).thenReturn(true);
        Endpoint endpoint2 = mock(Endpoint.class);
        when(endpoint2.isState(LifecycleState.CONNECTED)).thenReturn(true);
        when(endpoint2.isFree()).thenReturn(true);
        Endpoint endpoint3 = mock(Endpoint.class);
        when(endpoint3.isState(LifecycleState.CONNECTED)).thenReturn(true);
        when(endpoint3.isFree()).thenReturn(true);
        List<Endpoint> endpoints = Arrays.asList(endpoint1, endpoint2, endpoint3);

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

        Endpoint selected = strategy.select(mock(CouchbaseRequest.class),  Collections.<Endpoint>emptyList());
        assertNull(selected);
    }
}