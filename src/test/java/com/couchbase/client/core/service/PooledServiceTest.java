/*
 * Copyright (c) 2017 Couchbase, Inc.
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
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.AbstractServiceConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.service.strategies.SelectionStrategy;
import com.couchbase.client.core.state.LifecycleState;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.Observable;
import rx.functions.Action2;
import rx.functions.Func1;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;
import rx.subjects.BehaviorSubject;

import java.util.List;

import static com.couchbase.client.core.service.PooledServiceTest.SimpleSerivceConfig.ssc;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link PooledService}s.
 *
 * @author Michael Nitschinger
 * @since 1.4.2
 */
public class PooledServiceTest {

    private static volatile CoreEnvironment ENV;

    @BeforeClass
    public static void setup() {
        ENV = mock(CoreEnvironment.class);
        when(ENV.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);
        when(ENV.scheduler()).thenReturn(Schedulers.computation());
    }

    @Test(expected =  IllegalArgumentException.class)
    public void shouldFailIfMinIsNegative() {
        new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), ssc(-1, 2), null);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void shouldFailIfMaxIsNegative() {
        new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), ssc(0, -2), null);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void shouldFailIfMaxIs0() {
        new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), ssc(0, 0), null);
    }

    @Test(expected =  IllegalArgumentException.class)
    public void shouldFailIfMaxGreaterThanMinEndpoints() {
        new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), ssc(3, 2), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailIfPipeliningAndNotFixedEndpoints() {
        new MockedService(ServiceType.BINARY,
                EndpointFactoryMock.simple(null, null), ssc(2, 3, true), null);
    }

    @Test
    public void shouldProperlyExposeMapping() {
        MockedService ms =  new MockedService(ServiceType.BINARY,
                EndpointFactoryMock.simple(null, null), ssc(2, 2), null);
        assertEquals(ServiceType.BINARY, ms.type());
        assertEquals(ServiceType.BINARY.mapping(), ms.mapping());
    }

    @Test
    public void shouldBeIdleOnBootIfMinEndpointsIs0() {
        MockedService ms = new MockedService(ServiceType.CONFIG,
            EndpointFactoryMock.simple(null, null), ssc(0, 10), null);

        assertEquals(ServiceType.CONFIG, ms.type());
        assertEquals(LifecycleState.IDLE, ms.state());

        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.IDLE, afterConnectState);
    }

    @Test
    public void shouldBeDisconnectedOnBootIfMinEndpointsIsGt0() {
        MockedService ms = new MockedService(ServiceType.BINARY,
            EndpointFactoryMock.simple(null, null), ssc(3, 10), null);
        assertEquals(ServiceType.BINARY, ms.type());
        assertEquals(LifecycleState.DISCONNECTED, ms.state());
    }

    @Test
    public void shouldSuccessfullyBootstrapMinEndpoints() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(3, 4), null);

        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);

        assertEquals(LifecycleState.CONNECTING, ms.state());
        ef.advance(0, LifecycleState.CONNECTED);
        assertEquals(LifecycleState.DEGRADED, ms.state());
        ef.advance(1, LifecycleState.CONNECTED);
        assertEquals(LifecycleState.DEGRADED, ms.state());
        ef.advance(2, LifecycleState.CONNECTED);
        assertEquals(LifecycleState.CONNECTED, ms.state());

        assertEquals(3, ef.endpointCount());
        assertEquals(3, ef.endpointConnectCalled());
    }

    @Test
    public void shouldIgnoreConnectIfConnecting() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(1, 1), null);

        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(1, ef.endpointCount());

        afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(1, ef.endpointCount());
        assertEquals(1, ef.endpointConnectCalled());
    }

    @Test
    public void shouldIgnoreConnectIfConnected() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(1, 1), null);

        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(1, ef.endpointCount());

        ef.advance(0, LifecycleState.CONNECTED);

        afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTED, afterConnectState);
        assertEquals(1, ef.endpointCount());
        assertEquals(1, ef.endpointConnectCalled());
    }

    @Test
    public void shouldDisconnectIfInstructed() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        ef.onDisconnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(4, 4), null);
        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(4, ef.endpointCount());

        ef.advanceAll(LifecycleState.CONNECTED);

        LifecycleState stateAfterDisconnect = ms.disconnect().toBlocking().single();
        assertEquals(LifecycleState.DISCONNECTED, stateAfterDisconnect);
        assertEquals(4, ef.endpointDisconnectCalled());
    }

    @Test
    public void shouldIgnoreDisconnectIfDisconnecting() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        ef.onDisconnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(4, 4), null);
        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(4, ef.endpointCount());

        ef.advanceAll(LifecycleState.CONNECTED);

        ef.advance(0, LifecycleState.DISCONNECTING);
        ef.advance(1, LifecycleState.DISCONNECTING);
        ef.advance(2, LifecycleState.DISCONNECTING);
        ef.advance(3, LifecycleState.DISCONNECTING);

        assertEquals(LifecycleState.DISCONNECTING, ms.state());

        LifecycleState stateAfterDisconnect = ms.disconnect().toBlocking().single();
        assertEquals(LifecycleState.DISCONNECTING, stateAfterDisconnect);

        assertEquals(0, ef.endpointDisconnectCalled());

        stateAfterDisconnect = ms.disconnect().toBlocking().single();
        assertEquals(LifecycleState.DISCONNECTING, stateAfterDisconnect);
        assertEquals(0, ef.endpointDisconnectCalled());
    }

    @Test
    public void shouldIgnoreDisconnectIfDisconnected() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(null, null);
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(1, 1), null);
        assertEquals(ServiceType.BINARY, ms.type());
        assertEquals(LifecycleState.DISCONNECTED, ms.state());

        LifecycleState afterDisconnectState = ms.disconnect().toBlocking().single();
        assertEquals(LifecycleState.DISCONNECTED, afterDisconnectState);
        assertEquals(0, ef.endpointDisconnectCalled());
    }

    @Test
    public void shouldGenerateIdentityLogLine() {
        MockedService ms = new MockedService(ServiceType.CONFIG,
            EndpointFactoryMock.simple(ENV, null), ssc(0, 10), null);
        String actual = PooledService.logIdent("hostname", ms);
        assertEquals("[hostname][MockedService]: ", actual);

        actual = PooledService.logIdent(null, ms);
        assertEquals("[null][MockedService]: ", actual);
    }

    @Test
    public void shouldSendToEndpointWhenFixed() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(2, 2), ss);
        ms.connect().toBlocking().single();
        ef.advanceAll(LifecycleState.CONNECTED);

        CouchbaseRequest request = mock(CouchbaseRequest.class);
        when(ss.select(same(request), any(List.class))).thenReturn(ef.endpoints().get(0));
        when(request.isActive()).thenReturn(true);

        ms.send(request);
        assertEquals(1, ef.endpointSendCalled());
    }

    @Test
    public void shouldBePutIntoRetryWhenFixed() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(2, 2), ss);
        ms.connect().toBlocking().single();
        ef.advanceAll(LifecycleState.CONNECTED);

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr = mockRequest();
        CouchbaseRequest request = mr.value1();
        TestSubscriber<CouchbaseResponse> subscriber = mr.value2();

        when(ss.select(same(request), any(List.class))).thenReturn(null);
        ms.send(request);

        assertEquals(0, ef.endpointSendCalled());
        subscriber.assertError(RequestCancelledException.class);
    }

    @Test
    public void shouldPropagateFlushToAllEndpoints() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);

        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(3, 3), null);
        ms.connect().toBlocking().single();
        ef.advanceAll(LifecycleState.CONNECTED);

        ms.send(SignalFlush.INSTANCE);
        assertEquals(3, ef.endpointSendCalled());
    }

    @Test
    public void shouldSendFromIdleToOne() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        ef.onDisconnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(4, 4), null);
        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(4, ef.endpointCount());

        ef.advanceAll(LifecycleState.CONNECTED);
    }

    @Test
    public void shouldSendFromMinPlusOne() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });

        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(0, 3), ss);
        ms.connect().toBlocking().single();
        assertEquals(LifecycleState.IDLE, ms.state());

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr = mockRequest();
        CouchbaseRequest request = mr.value1();

        when(ss.select(same(request), any(List.class))).thenReturn(null);
        ms.send(request);

        assertEquals(0, ef.endpointSendCalled());
        assertEquals(0, ms.endpoints().size());
        ef.advanceAll(LifecycleState.CONNECTED);
        assertEquals(2, ef.endpointSendCalled()); // request & flush
        assertEquals(1, ms.endpoints().size());
    }

    @Test
    public void shouldRetryWhenMinEqMax() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });

        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.QUERY, ef, ssc(0, 2), ss);
        ms.connect().toBlocking().single();

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr1 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr2 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr3 = mockRequest();


        when(ss.select(any(CouchbaseRequest.class), any(List.class))).thenReturn(null);
        ms.send(mr1.value1());
        ef.advanceAll(LifecycleState.CONNECTED);
        ms.send(mr2.value1());
        ef.advanceAll(LifecycleState.CONNECTED);
        ms.send(mr3.value1());
        ef.advanceAll(LifecycleState.CONNECTED);

        assertEquals(4, ef.endpointSendCalled());

        mr1.value2().assertNoTerminalEvent();
        mr2.value2().assertNoTerminalEvent();
        mr3.value2().assertError(RequestCancelledException.class);
    }

    @Test
    public void shouldRetryAndAccountForPending() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });

        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.QUERY, ef, ssc(0, 2), ss);
        ms.connect().toBlocking().single();

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr1 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr2 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr3 = mockRequest();


        when(ss.select(any(CouchbaseRequest.class), any(List.class))).thenReturn(null);
        ms.send(mr1.value1());
        ms.send(mr2.value1());
        ms.send(mr3.value1());
        ef.advanceAll(LifecycleState.CONNECTED);

        assertEquals(4, ef.endpointSendCalled());

        mr1.value2().assertNoTerminalEvent();
        mr2.value2().assertNoTerminalEvent();
        mr3.value2().assertError(RequestCancelledException.class);
    }

    @Test
    public void shouldRetryWhenSocketOpenFailedOnSend() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onCreate(new Action2<Endpoint, BehaviorSubject<LifecycleState>>() {
            @Override
            public void call(final Endpoint endpoint, final BehaviorSubject<LifecycleState> states) {
                when(endpoint.connect()).then(new Answer<Observable<LifecycleState>>() {
                    @Override
                    public Observable<LifecycleState> answer(InvocationOnMock invocation) throws Throwable {
                        states.onNext(LifecycleState.DISCONNECTED);
                        return Observable.error(new Exception("something happened"));
                    }
                });
            }
        });

        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.QUERY, ef, ssc(0, 2), ss);
        ms.connect().toBlocking().single();

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr1 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr2 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr3 = mockRequest();

        when(ss.select(any(CouchbaseRequest.class), any(List.class))).thenReturn(null);
        ms.send(mr1.value1());
        ms.send(mr2.value1());
        ms.send(mr3.value1());

        assertEquals(0, ef.endpointSendCalled());

        mr1.value2().assertError(RequestCancelledException.class);
        mr2.value2().assertError(RequestCancelledException.class);
        mr3.value2().assertError(RequestCancelledException.class);
    }

    @Test
    public void shouldRetryWhenSocketOpenResultIsDisconnected() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTED;
            }
        });

        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.QUERY, ef, ssc(0, 2), ss);
        ms.connect().toBlocking().single();

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr1 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr2 = mockRequest();


        when(ss.select(any(CouchbaseRequest.class), any(List.class))).thenReturn(null);
        ms.send(mr1.value1());
        ms.send(mr2.value1());

        assertEquals(0, ef.endpointSendCalled());

        mr1.value2().assertError(RequestCancelledException.class);
        mr2.value2().assertError(RequestCancelledException.class);
    }

    @Test
    public void shouldRetryWhenSocketOpenResultIsDisconnecting() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });

        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.QUERY, ef, ssc(0, 2), ss);
        ms.connect().toBlocking().single();

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr1 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr2 = mockRequest();


        when(ss.select(any(CouchbaseRequest.class), any(List.class))).thenReturn(null);
        ms.send(mr1.value1());
        ms.send(mr2.value1());

        assertEquals(0, ef.endpointSendCalled());

        mr1.value2().assertError(RequestCancelledException.class);
        mr2.value2().assertError(RequestCancelledException.class);
    }

    @Test
    public void selectionStrategyShouldNotBeCalledWithEmptyEndpoints() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });

        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.QUERY, ef, ssc(0, 2), ss);
        ms.connect().toBlocking().single();

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr1 = mockRequest();
        ms.send(mr1.value1());

        verify(ss, never()).select(any(CouchbaseRequest.class), any(List.class));
    }

    @Test
    public void shouldCleanPendingRequestsWhenDisconnectConcurrently() {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        ef.onDisconnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });

        SelectionStrategy ss = mock(SelectionStrategy.class);

        MockedService ms = new MockedService(ServiceType.QUERY, ef, ssc(0, 3), ss);
        ms.connect().toBlocking().single();
        assertEquals(LifecycleState.IDLE, ms.state());

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr = mockRequest();
        CouchbaseRequest request = mr.value1();

        when(ss.select(same(request), any(List.class))).thenReturn(null);
        ms.send(request);

        LifecycleState afterDisconnectState = ms.disconnect().toBlocking().single();
        assertEquals(LifecycleState.IDLE, afterDisconnectState);

        ef.advanceAll(LifecycleState.CONNECTED);
        mr.value2().assertError(RequestCancelledException.class);
        assertEquals(0, ms.endpoints().size());
    }

    @Test
    public void shouldCloseIdleEndpointsIfTimerFires() throws Exception {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        ef.onDisconnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(0, 4, 1), null);
        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.IDLE, afterConnectState);
        assertEquals(0, ef.endpointCount());

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr1 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr2 = mockRequest();

        ms.send(mr1.value1());
        ms.send(mr2.value1());

        ef.advanceAll(LifecycleState.CONNECTED);
        assertEquals(2, ms.endpoints().size());

        // Simulate responses are done and proper.
        long now = System.nanoTime();
        for (Endpoint ep : ef.endpoints()) {
            when(ep.isFree()).thenReturn(true);
            when(ep.lastResponse()).thenReturn(now);
        }

        Thread.sleep(3000);

        assertEquals(0, ms.endpoints().size());
    }

    @Test
    public void shouldNotDisconnectIfQueryIsProcessingOnTimer() throws Exception {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        ef.onDisconnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(0, 4, 1), null);
        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.IDLE, afterConnectState);
        assertEquals(0, ef.endpointCount());

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr1 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr2 = mockRequest();

        ms.send(mr1.value1());
        ms.send(mr2.value1());

        ef.advanceAll(LifecycleState.CONNECTED);
        assertEquals(2, ms.endpoints().size());

        // Simulate responses are done and proper.
        long now = System.nanoTime();

        for (int i = 0; i < ef.endpoints().size(); i++) {
            when(ef.endpoints().get(i).isFree()).thenReturn(i == 0);
            when(ef.endpoints().get(i).lastResponse()).thenReturn(now);
        }

        Thread.sleep(3000);

        assertEquals(1, ms.endpoints().size());
    }

    @Test
    public void shouldRefillSlotsIfBelowMinimumOnIdleTimer() throws Exception {
        EndpointFactoryMock ef = EndpointFactoryMock.simple(ENV, null);
        ef.onConnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.CONNECTING;
            }
        });
        ef.onDisconnectTransition(new Func1<Endpoint, LifecycleState>() {
            @Override
            public LifecycleState call(Endpoint endpoint) {
                return LifecycleState.DISCONNECTING;
            }
        });
        SelectionStrategy ss = mock(SelectionStrategy.class);
        MockedService ms = new MockedService(ServiceType.BINARY, ef, ssc(2, 8, 1), ss);
        when(ss.select(any(CouchbaseRequest.class), any(List.class))).thenReturn(null);

        LifecycleState afterConnectState = ms.connect().toBlocking().single();
        assertEquals(LifecycleState.CONNECTING, afterConnectState);
        assertEquals(2, ms.endpoints().size());
        ef.advanceAll(LifecycleState.CONNECTED);

        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr1 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr2 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr3 = mockRequest();
        Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mr4 = mockRequest();

        ms.send(mr1.value1());
        ms.send(mr2.value1());
        ms.send(mr3.value1());
        ms.send(mr4.value1());

        ef.advanceAll(LifecycleState.CONNECTED);
        assertEquals(6, ms.endpoints().size());

        // Simulate responses are done and proper.
        long now = System.nanoTime();
        for (Endpoint ep : ms.endpoints()) {
            when(ep.isFree()).thenReturn(true);
            when(ep.lastResponse()).thenReturn(now);
        }

        Thread.sleep(3000);

        assertEquals(2, ms.endpoints().size());
    }



    /**
     * A simple service which can be mocked in all kinds of ways to test the functionality of the
     * pooled service.
     */
    static class MockedService extends PooledService {

        private final ServiceType serviceType;

        MockedService(ServiceType st, EndpointFactoryMock ef, AbstractServiceConfig sc,
            SelectionStrategy ss) {
            super(ef.getHostname(), ef.getBucket(), ef.getBucket(), ef.getPassword(), ef.getPort(), ef.getEnv(),
                sc, ef.getResponseBuffer(), ef, ss);
            this.serviceType = st;
        }

        @Override
        public ServiceType type() {
            return serviceType;
        }

        @Override
        public List<Endpoint> endpoints() {
            return super.endpoints();
        }
    }

    static class SimpleSerivceConfig extends AbstractServiceConfig {
        SimpleSerivceConfig(int minEndpoints, int maxEndpoints, boolean pipelined, int idleTime) {
            super(minEndpoints, maxEndpoints, pipelined, idleTime);
        }

        static SimpleSerivceConfig ssc(int minEndpoints, int maxEndpoints) {
            return new SimpleSerivceConfig(minEndpoints, maxEndpoints, false, 0);
        }

        static SimpleSerivceConfig ssc(int minEndpoints, int maxEndpoints, boolean pipelined) {
            return new SimpleSerivceConfig(minEndpoints, maxEndpoints, pipelined, 0);
        }

        static SimpleSerivceConfig ssc(int minEndpoints, int maxEndpoints, int idleTime) {
            return new SimpleSerivceConfig(minEndpoints, maxEndpoints, false, idleTime);
        }
    }

    private Tuple2<CouchbaseRequest, TestSubscriber<CouchbaseResponse>> mockRequest() {
        AsyncSubject<CouchbaseResponse> subject = AsyncSubject.create();
        CouchbaseRequest request = mock(CouchbaseRequest.class);
        when(request.observable()).thenReturn(subject);
        when(request.isActive()).thenReturn(true);
        TestSubscriber<CouchbaseResponse> subscriber = TestSubscriber.create();
        subject.subscribe(subscriber);
        return Tuple.create(request, subscriber);
    }
}