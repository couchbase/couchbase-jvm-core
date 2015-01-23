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
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
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
        Endpoint endpoint = new DummyEndpoint(bootstrap);

        Observable<LifecycleState> observable = endpoint.connect();
        assertEquals(LifecycleState.CONNECTED, observable.toBlocking().single());
    }

    @Test
    public void shouldSwallowDuplicateConnectAttempts() {
        BootstrapAdapter bootstrap = mock(BootstrapAdapter.class);
        final ChannelPromise promise = channel.newPromise();
        when(bootstrap.connect()).thenReturn(promise);
        Endpoint endpoint = new DummyEndpoint(bootstrap);

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
        Endpoint endpoint = new DummyEndpoint(bootstrap);

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
        Endpoint endpoint = new DummyEndpoint(bootstrap);

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
        Endpoint endpoint = new DummyEndpoint(bootstrap);

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
        Endpoint endpoint = new DummyEndpoint(bootstrap);

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
        Endpoint endpoint = new DummyEndpoint(bootstrap);

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
        Endpoint endpoint = new DummyEndpoint(bootstrap);

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

    static class DummyEndpoint extends AbstractEndpoint {
        DummyEndpoint(BootstrapAdapter adapter) {
            super("default", null, adapter, false);
        }

        DummyEndpoint(String hostname, CoreEnvironment environment) {
            super(hostname, "default", null, 0, environment, null, false);
        }

        @Override
        protected void customEndpointHandlers(ChannelPipeline pipeline) {

        }
    }

}
