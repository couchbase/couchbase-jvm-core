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
package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.NotConnectedException;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.observers.TestSubscriber;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of {@link AbstractEndpoint}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class AbstractEndpointTest {

    private final String hostname = "127.0.0.1";
    private final CoreEnvironment environment = DefaultCoreEnvironment.create();
    private final EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());

    @Test
    public void shouldBeDisconnectedAfterCreation() {
        Endpoint endpoint = new DummyEndpoint(hostname, environment);
        assertEquals(LifecycleState.DISCONNECTED, endpoint.state());
    }

    @Test
    public void shouldConnectToItsChannel() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        when(bootstrap.connect()).thenReturn(channel.newSucceededFuture());
        Endpoint endpoint = new DummyEndpoint(bootstrap, environment);

        Observable<LifecycleState> observable = endpoint.connect();
        assertEquals(LifecycleState.CONNECTED, observable.toBlocking().single());
    }

    @Test
    public void shouldSwallowDuplicateConnectAttempts() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        final ChannelPromise promise = channel.newPromise();
        when(bootstrap.connect()).thenReturn(promise);
        Endpoint endpoint = new DummyEndpoint(bootstrap, environment);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {}
                promise.setSuccess();
            }
        }).start();

        Observable<LifecycleState> attempt1 = endpoint.connect();
        Observable<LifecycleState> attempt2 = endpoint.connect();
        latch.countDown();

        assertEquals(LifecycleState.CONNECTED, attempt1.toBlocking().single());
        assertEquals(LifecycleState.CONNECTING, attempt2.toBlocking().single());
        verify(bootstrap, times(1)).connect();
    }

    @Test
    public void shouldSwallowConnectIfConnected() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        when(bootstrap.connect()).thenReturn(channel.newSucceededFuture());
        Endpoint endpoint = new DummyEndpoint(bootstrap, environment);

        Observable<LifecycleState> observable = endpoint.connect();
        assertEquals(LifecycleState.CONNECTED, observable.toBlocking().single());
        observable = endpoint.connect();
        assertEquals(LifecycleState.CONNECTED, observable.toBlocking().single());
        verify(bootstrap, times(1)).connect();
    }

    @Test
    public void shouldDisconnectIfInstructed() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        when(bootstrap.connect()).thenReturn(channel.newSucceededFuture());
        Endpoint endpoint = new DummyEndpoint(bootstrap, environment);

        Observable<LifecycleState> observable = endpoint.connect();
        assertEquals(LifecycleState.CONNECTED, observable.toBlocking().single());

        observable = endpoint.disconnect();
        assertEquals(LifecycleState.DISCONNECTED, observable.toBlocking().single());
    }

    @Test
    public void shouldStopConnectIfDisconnectOverrides() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        final ChannelPromise promise = channel.newPromise();
        when(bootstrap.connect()).thenReturn(promise);
        Endpoint endpoint = new DummyEndpoint(bootstrap, environment);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {}
                promise.setSuccess();
            }
        }).start();

        Observable<LifecycleState> connect = endpoint.connect();
        Observable<LifecycleState> disconnect = endpoint.disconnect();
        latch.countDown();

        assertEquals(LifecycleState.DISCONNECTED, connect.toBlocking().single());
        assertEquals(LifecycleState.DISCONNECTED, disconnect.toBlocking().single());
    }

    @Test
    public void shouldSendMessageToChannelIfConnected() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        when(bootstrap.connect()).thenReturn(channel.newSucceededFuture());
        Endpoint endpoint = new DummyEndpoint(bootstrap, environment);

        Observable<LifecycleState> observable = endpoint.connect();
        assertEquals(LifecycleState.CONNECTED, observable.toBlocking().single());

        CouchbaseRequest mockRequest = mock(CouchbaseRequest.class);
        endpoint.send(mockRequest);
        channel.flush();

        assertEquals(1, channel.outboundMessages().size());
        assertTrue(channel.readOutbound() instanceof CouchbaseRequest);
    }

    @Test(expected = NotConnectedException.class)
    public void shouldRejectMessageIfNotConnected() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        Endpoint endpoint = new DummyEndpoint(bootstrap, environment);

        CouchbaseRequest mockRequest = mock(CouchbaseRequest.class);
        Subject<CouchbaseResponse, CouchbaseResponse> subject = AsyncSubject.create();
        when(mockRequest.observable()).thenReturn(subject);
        endpoint.send(mockRequest);
        mockRequest.observable().toBlocking().single();
    }

    @Test
    public void shouldStreamLifecycleToObservers() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        when(bootstrap.connect()).thenReturn(channel.newSucceededFuture());
        Endpoint endpoint = new DummyEndpoint(bootstrap, environment);

        final List<LifecycleState> states = Collections.synchronizedList(new ArrayList<LifecycleState>());
        endpoint.states().subscribe(new Action1<LifecycleState>() {
            @Override
            public void call(LifecycleState state) {
                states.add(state);
            }
        });

        Observable<LifecycleState> observable = endpoint.connect();
        assertEquals(LifecycleState.CONNECTED, observable.toBlocking().single());

        observable = endpoint.disconnect();
        assertEquals(LifecycleState.DISCONNECTED, observable.toBlocking().single());

        assertEquals(LifecycleState.DISCONNECTED, states.get(0));
        assertEquals(LifecycleState.CONNECTING, states.get(1));
        assertEquals(LifecycleState.CONNECTED, states.get(2));
        assertEquals(LifecycleState.DISCONNECTING, states.get(3));
        assertEquals(LifecycleState.DISCONNECTED, states.get(4));
    }

    @Test
    public void shouldForceTimeoutOfSocketConnectDoesNotReturn() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        when(bootstrap.connect()).thenReturn(channel.newPromise()); // this promise never completes
        Endpoint endpoint = new DummyEndpoint(bootstrap, environment);

        Observable<LifecycleState> observable = endpoint.connect();

        TestSubscriber<LifecycleState> testSubscriber = new TestSubscriber<LifecycleState>();
        observable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();

        List<Throwable> errors = testSubscriber.getOnErrorEvents();
        assertEquals(1, errors.size());
        assertEquals(ConnectTimeoutException.class, errors.get(0).getClass());

        endpoint.disconnect().subscribe();
    }

    @Test
    public void shouldAlwaysStartAsFree() {
        Endpoint endpoint = new DummyEndpoint(hostname, environment);
        assertTrue(endpoint.isFree());
    }

    static class DummyEndpoint extends AbstractEndpoint {
        DummyEndpoint(BootstrapAdapter adapter, CoreEnvironment environment) {
            super("default", "default", null, adapter, false, environment, true);
        }

        DummyEndpoint(String hostname, CoreEnvironment environment) {
            super(hostname, "default", "default", null, 0, environment, null, false, environment.ioPool(), true);
        }

        @Override
        protected void customEndpointHandlers(ChannelPipeline pipeline) {

        }
    }

}
