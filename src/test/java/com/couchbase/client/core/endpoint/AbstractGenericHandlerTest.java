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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.view.GetDesignDocumentRequest;
import com.couchbase.client.core.service.ServiceType;
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

            @Override
            protected ServiceType serviceType() {
                return null;
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
