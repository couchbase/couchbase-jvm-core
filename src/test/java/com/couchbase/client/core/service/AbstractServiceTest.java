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
package com.couchbase.client.core.service;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Verifies the correct functionality of a {@link AbstractService}.
 */
public class AbstractServiceTest {

    private final String hostname = "127.0.0.1";
    private final Environment environment = new CouchbaseEnvironment();

    @Test
    public void shouldBeDisconnectAfterInit() {
        Service service = new DummyService(hostname, environment, 1, mock(SelectionStrategy.class));
        assertEquals(LifecycleState.DISCONNECTED, service.state());
    }

    @Test
    public void shouldBeDisconnectedIfNoEndpointConfigured() {
        Service service = new DummyService(hostname, environment, 0, mock(SelectionStrategy.class));
        service.connect().toBlockingObservable().single();
        assertEquals(LifecycleState.DISCONNECTED, service.state());
    }

    @Test
    public void shouldConnectToOneEndpoint() {
        Service service = new DummyService(hostname, environment, 1, mock(SelectionStrategy.class));
        assertEquals(LifecycleState.DISCONNECTED, service.state());

        service.connect().toBlockingObservable().single();
        assertEquals(LifecycleState.CONNECTED, service.state());
    }

    @Test
    public void shouldConnectToThreeEndpoints() {

    }

    @Test
    public void shouldBeDegradedIfNotAllEndpointsConnected() {

    }

    @Test
    public void shouldDisconnectAllEndpointsOnDisconnect() {

    }

    @Test
    public void shouldBeDegradedAndConnectedIfOneEndpointReconnects() {

    }

    @Test
    public void shouldSwallowDuplicateConnectAttempt() {

    }

    @Test
    public void shouldSwallowConnectWhenConnected() {

    }

    @Test
    public void shouldSwallowDuplicateDisconnect() {

    }

    @Test
    public void shouldSwallowDisconnectIfDisconnected() {

    }

    static class DummyService extends AbstractService {

        DummyService(String hostname, Environment env, int numEndpoints, SelectionStrategy strategy) {
            super(hostname, env, numEndpoints, strategy, null);
        }

        @Override
        protected Endpoint newEndpoint(RingBuffer<ResponseEvent> responseBuffer) {
            return mock(Endpoint.class);
        }

        @Override
        public ServiceType type() {
            return ServiceType.BINARY;
        }
    }
}
