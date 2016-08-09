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
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class RoundRobinSelectionStrategyTest {

    @Test
    public void testRoundRobinSelectOverIntegerMaxValue() throws Exception {
        RoundRobinSelectionStrategy strategy = new RoundRobinSelectionStrategy();
        Endpoint a = Mockito.mock(Endpoint.class);
        Endpoint b = Mockito.mock(Endpoint.class);
        Endpoint c = Mockito.mock(Endpoint.class);
        Endpoint d = Mockito.mock(Endpoint.class);
        Endpoint e = Mockito.mock(Endpoint.class);
        when(a.isState(any(LifecycleState.class))).thenReturn(true);
        when(b.isState(any(LifecycleState.class))).thenReturn(true);
        when(c.isState(any(LifecycleState.class))).thenReturn(true);
        when(d.isState(any(LifecycleState.class))).thenReturn(true);
        when(e.isState(any(LifecycleState.class))).thenReturn(true);
        Endpoint[] endpoints = new Endpoint[] { a, b, c, d, e };
        CouchbaseRequest request = Mockito.mock(CouchbaseRequest.class);

        strategy.setSkip(Integer.MAX_VALUE - 2);

        //selecting brings skip to max-value - 1
        Endpoint selected = strategy.select(request, endpoints);
        int skipStart = strategy.skip;
        assertThat(skipStart).isGreaterThan(1000);

        //max-value
        selected = strategy.select(request, endpoints);
        assertThat(strategy.skip)
                .isEqualTo(skipStart + 1)
                .isPositive();

        //max-value + 1: wrapping
        selected = strategy.select(request, endpoints);
        assertThat(strategy.skip).isZero();
        assertThat(selected).isEqualTo(a);

        //following selects will select B, C, D, E, A and increment skip to 5
        selected = strategy.select(request, endpoints);
        assertThat(strategy.skip).isEqualTo(1);
        assertThat(selected).isEqualTo(b);

        selected = strategy.select(request, endpoints);
        assertThat(strategy.skip).isEqualTo(2);
        assertThat(selected).isEqualTo(c);

        selected = strategy.select(request, endpoints);
        assertThat(strategy.skip).isEqualTo(3);
        assertThat(selected).isEqualTo(d);

        selected = strategy.select(request, endpoints);
        assertThat(strategy.skip).isEqualTo(4);
        assertThat(selected).isEqualTo(e);

        selected = strategy.select(request, endpoints);
        assertThat(strategy.skip).isEqualTo(5);
        assertThat(selected).isEqualTo(a);
    }
}