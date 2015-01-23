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
package com.couchbase.client.core.service;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.kv.AuthenticationException;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.lmax.disruptor.RingBuffer;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.subjects.AsyncSubject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link AbstractOnDemandService}.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class AbstractOnDemandServiceTest {

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
    public void shouldDispatchOnDemand() throws Exception {
        InstrumentedService service = new InstrumentedService(host, bucket, password, port, env, null, factory);

        Endpoint endpoint = mock(Endpoint.class);
        final EndpointStates endpointStates = new EndpointStates(LifecycleState.DISCONNECTED);
        when(endpoint.states()).thenReturn(endpointStates.states());
        when(endpoint.connect()).thenReturn(Observable.just(LifecycleState.CONNECTED));
        when(factory.create(host, bucket, password, port, env, null)).thenReturn(endpoint);

        assertEquals(0, service.endpoints().length);
        assertEquals(LifecycleState.IDLE, service.connect().toBlocking().single());
        assertEquals(0, service.endpoints().length);

        CouchbaseRequest req = mock(CouchbaseRequest.class);
        AsyncSubject<CouchbaseResponse> reqObservable = AsyncSubject.create();
        when(req.observable()).thenReturn(reqObservable);
        service.send(req);

        endpointStates.transitionState(LifecycleState.CONNECTING);
        endpointStates.transitionState(LifecycleState.CONNECTED);

        verify(endpoint, times(1)).send(req);
        verify(endpoint, times(1)).send(SignalFlush.INSTANCE);

        endpointStates.transitionState(LifecycleState.DISCONNECTED);

        assertEquals(LifecycleState.DISCONNECTED, endpointStates.state());
    }

    @Test
    public void shouldFailObservableIfCouldNotConnect() {
        InstrumentedService service = new InstrumentedService(host, bucket, password, port, env, null, factory);

        Endpoint endpoint = mock(Endpoint.class);
        final EndpointStates endpointStates = new EndpointStates(LifecycleState.DISCONNECTED);
        when(endpoint.states()).thenReturn(endpointStates.states());
        when(endpoint.connect()).thenReturn(Observable.just(LifecycleState.DISCONNECTED));
        when(factory.create(host, bucket, password, port, env, null)).thenReturn(endpoint);

        CouchbaseRequest req = mock(CouchbaseRequest.class);
        AsyncSubject<CouchbaseResponse> reqObservable = AsyncSubject.create();
        when(req.observable()).thenReturn(reqObservable);

        try {
            service.send(req);
            reqObservable.toBlocking().single();
            assertTrue("Should've failed but did not", false);
        } catch(CouchbaseException ex) {
            assertEquals("Could not connect endpoint.", ex.getMessage());
        } catch(Throwable tr) {
            assertTrue(tr.getMessage(), false);
        }
    }

    @Test
    public void shouldFailObservableIfErrorOnConnect() {
        InstrumentedService service = new InstrumentedService(host, bucket, password, port, env, null, factory);

        Endpoint endpoint = mock(Endpoint.class);
        final EndpointStates endpointStates = new EndpointStates(LifecycleState.DISCONNECTED);
        when(endpoint.states()).thenReturn(endpointStates.states());
        when(endpoint.connect()).thenReturn(Observable.<LifecycleState>error(new AuthenticationException()));
        when(factory.create(host, bucket, password, port, env, null)).thenReturn(endpoint);

        CouchbaseRequest req = mock(CouchbaseRequest.class);
        AsyncSubject<CouchbaseResponse> reqObservable = AsyncSubject.create();
        when(req.observable()).thenReturn(reqObservable);

        try {
            service.send(req);
            reqObservable.toBlocking().single();
            assertTrue("Should've failed but did not", false);
        } catch(AuthenticationException ex) {
            assertTrue(true);
            assertEquals(LifecycleState.IDLE, service.state());
        } catch(Throwable tr) {
            assertTrue(tr.getMessage(), false);
        }
    }

    class InstrumentedService extends AbstractOnDemandService {
        public InstrumentedService(String hostname, String bucket, String password, int port, CoreEnvironment env,
            RingBuffer<ResponseEvent> responseBuffer, EndpointFactory endpointFactory) {
            super(hostname, bucket, password, port, env, responseBuffer, endpointFactory);
        }

        @Override
        public ServiceType type() {
            return ServiceType.CONFIG;
        }

        @Override
        public Endpoint[] endpoints() {
            return super.endpoints();
        }

        @Override
        protected EndpointStateZipper endpointStates() {
            return super.endpointStates();
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