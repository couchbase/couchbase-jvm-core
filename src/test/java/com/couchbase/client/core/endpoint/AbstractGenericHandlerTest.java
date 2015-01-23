/*
 * Copyright (c) 2014 Couchbase, Inc.
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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.view.GetDesignDocumentRequest;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link AbstractGenericHandler}.
 *
 * @author Simon Baslé
 * @since 1.1
 */
public class AbstractGenericHandlerTest {

    private Disruptor<ResponseEvent> responseBuffer;
    private List<CouchbaseMessage> firedEvents;
    private CountDownLatch latch;
    private RingBuffer<ResponseEvent> responseRingBuffer;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        responseBuffer = new Disruptor<ResponseEvent>(new EventFactory<ResponseEvent>() {
            @Override
            public ResponseEvent newInstance() {
                return new ResponseEvent();
            }
        }, 1024, Executors.newCachedThreadPool());

        firedEvents = Collections.synchronizedList(new ArrayList<CouchbaseMessage>());
        latch = new CountDownLatch(1);
        responseBuffer.handleEventsWith(new EventHandler<ResponseEvent>() {
            @Override
            public void onEvent(ResponseEvent event, long sequence, boolean endOfBatch) throws Exception {
                firedEvents.add(event.getMessage());
                latch.countDown();
            }
        });
        responseRingBuffer = responseBuffer.start();
    }

    @After
    public void clear() {
        responseBuffer.shutdown();
    }

    private static interface FakeHandlerDelegate<R, E, Q extends CouchbaseRequest> {
        public E encodeRequest(ChannelHandlerContext ctx, Q msg) throws Exception;
        public CouchbaseResponse decodeResponse(ChannelHandlerContext ctx, R msg) throws Exception;
    }

    private <R, E, Q extends CouchbaseRequest> AbstractGenericHandler<R, E, Q> createFakeHandler(
            final FakeHandlerDelegate<R, E, Q> delegate) {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.scheduler()).thenReturn(Schedulers.computation());
        AbstractEndpoint endpoint = mock(AbstractEndpoint.class);
        when(endpoint.environment()).thenReturn(environment);
        when(environment.userAgent()).thenReturn("Couchbase Client Mock");

        ArrayDeque<Q> queue = new ArrayDeque<Q>();

        return new AbstractGenericHandler<R, E, Q>(endpoint, responseRingBuffer, queue, false) {
            @Override
            protected E encodeRequest(ChannelHandlerContext ctx, Q msg) throws Exception {
                return delegate.encodeRequest(ctx, msg);
            }

            @Override
            protected CouchbaseResponse decodeResponse(ChannelHandlerContext ctx, R msg) throws Exception {
                return delegate.decodeResponse(ctx, msg);
            }
        };
    }

    @Test(expected = CouchbaseException.class)
    public void whenDecodeFailureShouldOnErrorFailureWrappedInCouchbaseException() {
        AbstractGenericHandler<Object, Object, GetDesignDocumentRequest> handler = createFakeHandler(
            new FakeHandlerDelegate<Object, Object, GetDesignDocumentRequest>() {
                @Override
                public Object encodeRequest(ChannelHandlerContext ctx, GetDesignDocumentRequest msg) throws Exception {
                    return new Object();
                }
                @Override
                public CouchbaseResponse decodeResponse(ChannelHandlerContext ctx, Object msg) throws Exception {
                    throw new IllegalStateException("this is fake");
                }
            }
        );
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        GetDesignDocumentRequest request = new GetDesignDocumentRequest("any", false, "bucket", "password");
        channel.writeOutbound(request);
        channel.writeInbound(new Object());

        try {
            request.observable().timeout(1, TimeUnit.SECONDS).toBlocking().last();
        } catch (CouchbaseException e) {
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof IllegalStateException);
            assertEquals("this is fake", e.getCause().getMessage());
            throw e;
        }
    }
    @Test(expected = CouchbaseException.class)
    public void whenDecodeFailureIsCouchbaseExceptionShouldOnErrorIt() {
        AbstractGenericHandler<Object, Object, GetDesignDocumentRequest> handler = createFakeHandler(
            new FakeHandlerDelegate<Object, Object, GetDesignDocumentRequest>() {
                @Override
                public Object encodeRequest(ChannelHandlerContext ctx, GetDesignDocumentRequest msg) throws Exception {
                    return new Object();
                }
                @Override
                public CouchbaseResponse decodeResponse(ChannelHandlerContext ctx, Object msg) throws Exception {
                    throw new CouchbaseException("this is fake");
                }
            }
        );
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        GetDesignDocumentRequest request = new GetDesignDocumentRequest("any", false, "bucket", "password");
        channel.writeOutbound(request);
        channel.writeInbound(new Object());

        try {
            request.observable().timeout(1, TimeUnit.SECONDS).toBlocking().last();
        } catch (CouchbaseException e) {
            assertNull(e.getCause());
            assertEquals("this is fake", e.getMessage());
            throw e;
        }
    }

}
