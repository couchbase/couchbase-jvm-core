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

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import org.junit.Test;
import rx.Observable;
import rx.subjects.BehaviorSubject;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of a {@link AbstractService}.
 */
public class AbstractServiceTest {

    private final String hostname = "127.0.0.1";
    private final String bucket = "default";
    private final String password = "";
    private final int port = 0;
    private static final CoreEnvironment environment = DefaultCoreEnvironment.create();

    @Test
    public void shouldBeDisconnectAfterInit() {
        Endpoint endpoint = mock(Endpoint.class);
        when(endpoint.states()).thenReturn(Observable.from(LifecycleState.DISCONNECTED));
        List<Endpoint> endpoints = Arrays.asList(endpoint);
        Service.EndpointFactory factory = new DummyService.DummyEndpointFactory(endpoints.iterator());

        Service service = new DummyService(hostname, bucket, password, port, environment, 1,
            mock(SelectionStrategy.class), factory);
        assertEquals(LifecycleState.DISCONNECTED, service.state());
    }

    @Test
    public void shouldBeDisconnectedIfNoEndpointConfigured() {
        List<Endpoint> endpoints = Collections.emptyList();
        Service.EndpointFactory factory = new DummyService.DummyEndpointFactory(endpoints.iterator());
        Service service = new DummyService(hostname, bucket, password, port, environment, 0,
            mock(SelectionStrategy.class), factory);
        service.connect().toBlocking().single();
        assertEquals(LifecycleState.DISCONNECTED, service.state());
    }

    @Test
    public void shouldConnectToOneEndpoint() {
        Endpoint endpoint = mock(Endpoint.class);
        BehaviorSubject<LifecycleState> endpointStates = BehaviorSubject.create(LifecycleState.DISCONNECTED);
        when(endpoint.states()).thenReturn(endpointStates);
        when(endpoint.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));
        List<Endpoint> endpoints = Arrays.asList(endpoint);
        Service.EndpointFactory factory = new DummyService.DummyEndpointFactory(endpoints.iterator());
        Service service = new DummyService(hostname, bucket, password, port, environment, 1,
            mock(SelectionStrategy.class), factory);
        service.connect().toBlocking().single();
        endpointStates.onNext(LifecycleState.CONNECTED);
        assertEquals(LifecycleState.CONNECTED, service.state());
    }

    @Test
    public void shouldConnectToThreeEndpoints() {
        Endpoint endpoint1 = mock(Endpoint.class);
        BehaviorSubject<LifecycleState> endpoint1States = BehaviorSubject.create(LifecycleState.DISCONNECTED);
        when(endpoint1.states()).thenReturn(endpoint1States);
        when(endpoint1.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));
        Endpoint endpoint2 = mock(Endpoint.class);
        BehaviorSubject<LifecycleState> endpoint2States = BehaviorSubject.create(LifecycleState.DISCONNECTED);
        when(endpoint2.states()).thenReturn(endpoint2States);
        when(endpoint2.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));
        Endpoint endpoint3 = mock(Endpoint.class);
        BehaviorSubject<LifecycleState> endpoint3States = BehaviorSubject.create(LifecycleState.DISCONNECTED);
        when(endpoint3.states()).thenReturn(endpoint3States);
        when(endpoint3.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));

        List<Endpoint> endpoints = Arrays.asList(endpoint1, endpoint2, endpoint3);
        Service.EndpointFactory factory = new DummyService.DummyEndpointFactory(endpoints.iterator());
        Service service = new DummyService(hostname, bucket, password, port, environment, 3,
            mock(SelectionStrategy.class), factory);
        service.connect().toBlocking().single();
        endpoint1States.onNext(LifecycleState.CONNECTED);
        endpoint2States.onNext(LifecycleState.CONNECTED);
        endpoint3States.onNext(LifecycleState.CONNECTED);
        assertEquals(LifecycleState.CONNECTED, service.state());
    }

    @Test
    public void shouldBeDegradedIfNotAllEndpointsConnected() {
        Endpoint endpoint1 = mock(Endpoint.class);
        BehaviorSubject<LifecycleState> endpoint1States = BehaviorSubject.create(LifecycleState.DISCONNECTED);
        when(endpoint1.states()).thenReturn(endpoint1States);
        when(endpoint1.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));
        Endpoint endpoint2 = mock(Endpoint.class);
        BehaviorSubject<LifecycleState> endpoint2States = BehaviorSubject.create(LifecycleState.DISCONNECTED);
        when(endpoint2.states()).thenReturn(endpoint2States);
        when(endpoint2.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));
        Endpoint endpoint3 = mock(Endpoint.class);
        BehaviorSubject<LifecycleState> endpoint3States = BehaviorSubject.create(LifecycleState.DISCONNECTED);
        when(endpoint3.states()).thenReturn(endpoint3States);
        when(endpoint3.connect()).thenReturn(Observable.from(LifecycleState.CONNECTED));

        List<Endpoint> endpoints = Arrays.asList(endpoint1, endpoint2, endpoint3);
        Service.EndpointFactory factory = new DummyService.DummyEndpointFactory(endpoints.iterator());
        Service service = new DummyService(hostname, bucket, password, port, environment, 3,
            mock(SelectionStrategy.class), factory);
        service.connect().toBlocking().single();
        endpoint1States.onNext(LifecycleState.CONNECTED);
        endpoint2States.onNext(LifecycleState.CONNECTING);
        endpoint3States.onNext(LifecycleState.CONNECTING);
        assertEquals(LifecycleState.DEGRADED, service.state());
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

        DummyService(String hostname, String bucket, String password, int port, CoreEnvironment env, int numEndpoints,
            SelectionStrategy strategy, EndpointFactory factory) {
            super(hostname, bucket, password, port, env, numEndpoints, strategy, null, factory);
        }

        @Override
        public ServiceType type() {
            return ServiceType.BINARY;
        }

        public static class DummyEndpointFactory implements EndpointFactory {

            Iterator<Endpoint> endpoints;
            public DummyEndpointFactory(Iterator<Endpoint> endpoints) {
                this.endpoints = endpoints;
            }

            @Override
            public Endpoint create(String hostname, String bucket, String password, int port, CoreEnvironment env,
                RingBuffer<ResponseEvent> responseBuffer) {
                return endpoints.next();
            }

        }

    }
}
