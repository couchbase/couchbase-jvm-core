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
package com.couchbase.client.core.service;

import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.subjects.AsyncSubject;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link AbstractPoolingService}.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class AbstractPoolingServiceTest {

    private final String host = "hostname";
    private final String bucket = "bucket";
    private final String password = "";
    private final int port = 1234;

    private CoreEnvironment env;
    private Service.EndpointFactory factory;

    @Before
    public void setup() {
        env = mock(CoreEnvironment.class);
        factory = mock(Service.EndpointFactory.class);
    }

    @Test
    public void shouldConnectNumberOfEndpoints() {
        Endpoint endpoint1 = mock(Endpoint.class);
        EndpointStates e1s = new EndpointStates(LifecycleState.DISCONNECTED);
        when(endpoint1.states()).thenReturn(e1s.states());
        when(endpoint1.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        Endpoint endpoint2 = mock(Endpoint.class);
        EndpointStates e2s = new EndpointStates(LifecycleState.DISCONNECTED);
        when(endpoint2.connect()).thenReturn(Observable.just(LifecycleState.CONNECTING));
        when(endpoint2.states()).thenReturn(e2s.states());
        when(factory.create(host, bucket, password, port, env, null)).thenReturn(endpoint1, endpoint2);

        int endpoints = 2;
        InstrumentedService service = new InstrumentedService(host, bucket, password, port, env, endpoints,
            endpoints, null, null, factory);

        assertEquals(LifecycleState.DISCONNECTED, service.state());

        Observable<LifecycleState> connect = service.connect();

        e1s.transitionState(LifecycleState.CONNECTED);
        e2s.transitionState(LifecycleState.CONNECTING);

        assertEquals(LifecycleState.DEGRADED, connect.toBlocking().single());

        verify(endpoint1, times(1)).connect();
        verify(endpoint2, times(1)).connect();
        assertTrue(service.endpoints().length == 2);
        assertEquals(service.endpoints()[0], endpoint1);
        assertEquals(service.endpoints()[1], endpoint2);
    }

    @Test
    public void shouldDispatchEndpointsOnRequest() {
        Endpoint e1 = mock(Endpoint.class);
        EndpointStates e1s = new EndpointStates(LifecycleState.CONNECTED);
        when(e1.states()).thenReturn(e1s.states());
        Endpoint e2 = mock(Endpoint.class);
        EndpointStates e2s = new EndpointStates(LifecycleState.CONNECTED);
        when(e2.states()).thenReturn(e2s.states());
        Endpoint e3 = mock(Endpoint.class);
        EndpointStates e3s = new EndpointStates(LifecycleState.CONNECTED);
        when(e3.states()).thenReturn(e3s.states());
        Endpoint e4 = mock(Endpoint.class);
        EndpointStates e4s = new EndpointStates(LifecycleState.CONNECTED);
        when(e4.states()).thenReturn(e4s.states());
        when(factory.create(host, bucket, password, port, env, null)).thenReturn(e1, e2, e3, e4);

        final AtomicReference<List> foundEndpoints = new AtomicReference<List>();
        SelectionStrategy strategy = new SelectionStrategy() {
            @Override
            public Endpoint select(CouchbaseRequest request, Endpoint[] endpoints) {
                foundEndpoints.set(Arrays.asList(endpoints));
                return endpoints[0];
            }
        };

        int endpoints = 4;
        InstrumentedService service = new InstrumentedService(host, bucket, password, port, env, endpoints,
            endpoints, strategy, null, factory);

        service.connect().toBlocking().single();

        CouchbaseRequest request = mock(CouchbaseRequest.class);
        service.send(request);

        verify(e1, times(1)).send(request);
        assertEquals(endpoints, foundEndpoints.get().size());
    }

    @Test
    public void shouldDisconnectAllEndpoints() {
        Endpoint endpoint1 = mock(Endpoint.class);
        EndpointStates e1s = new EndpointStates(LifecycleState.DISCONNECTED);
        when(endpoint1.states()).thenReturn(e1s.states());
        when(endpoint1.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(endpoint1.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTING));
        Endpoint endpoint2 = mock(Endpoint.class);
        EndpointStates e2s = new EndpointStates(LifecycleState.DISCONNECTED);
        when(endpoint2.connect()).thenReturn(Observable.just(LifecycleState.CONNECTING));
        when(endpoint2.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTING));
        when(endpoint2.states()).thenReturn(e2s.states());
        when(factory.create(host, bucket, password, port, env, null)).thenReturn(endpoint1, endpoint2);

        int endpoints = 2;
        InstrumentedService service = new InstrumentedService(host, bucket, password, port, env, endpoints,
            endpoints, null, null, factory);

        assertEquals(LifecycleState.DISCONNECTED, service.state());

        Observable<LifecycleState> connect = service.connect();

        e1s.transitionState(LifecycleState.CONNECTED);
        e2s.transitionState(LifecycleState.CONNECTING);

        assertEquals(LifecycleState.DEGRADED, connect.toBlocking().single());

        assertEquals(LifecycleState.DISCONNECTED, service.disconnect().toBlocking().single());
        verify(endpoint1, times(1)).disconnect();
        verify(endpoint2, times(1)).disconnect();
    }

    @Test(expected = RequestCancelledException.class)
    public void shouldCancelRequestOnFailFastStrategy() {
        Endpoint endpoint1 = mock(Endpoint.class);
        EndpointStates e1s = new EndpointStates(LifecycleState.DISCONNECTED);
        when(endpoint1.states()).thenReturn(e1s.states());
        when(endpoint1.connect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        when(endpoint1.disconnect()).thenReturn(Observable.just(LifecycleState.DISCONNECTING));
        CoreEnvironment env = mock(CoreEnvironment.class);
        when(env.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);

        int endpoints = 1;
        SelectionStrategy strategy = mock(SelectionStrategy.class);
        when(strategy.select(any(CouchbaseRequest.class), any(Endpoint[].class))).thenReturn(null);
        InstrumentedService service = new InstrumentedService(host, bucket, password, port, env, endpoints,
                endpoints, strategy, null, factory);

        CouchbaseRequest request = mock(CouchbaseRequest.class);
        AsyncSubject<CouchbaseResponse> response = AsyncSubject.create();
        when(request.observable()).thenReturn(response);
        service.send(request);

        response.toBlocking().single();
    }

    class InstrumentedService extends AbstractPoolingService {

        public InstrumentedService(String hostname, String bucket, String password, int port, CoreEnvironment env,
            int minEndpoints, int maxEndpoints, SelectionStrategy strategy, RingBuffer<ResponseEvent> responseBuffer,
            EndpointFactory endpointFactory) {
            super(hostname, bucket, password, port, env, minEndpoints, maxEndpoints, strategy, responseBuffer,
                endpointFactory);
        }

        @Override
        public ServiceType type() {
            return ServiceType.BINARY;
        }

        @Override
        public Endpoint[] endpoints() {
            return super.endpoints();
        }
    }

    class EndpointStates extends AbstractStateMachine<LifecycleState> {

        public EndpointStates(LifecycleState initialState) {
            super(initialState);
        }

        @Override
        public void transitionState(LifecycleState newState) {
            super.transitionState(newState);
        }
    }

}