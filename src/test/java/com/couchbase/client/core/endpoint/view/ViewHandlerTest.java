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
package com.couchbase.client.core.endpoint.view;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.view.GetDesignDocumentRequest;
import com.couchbase.client.core.message.view.GetDesignDocumentResponse;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.message.view.ViewRequest;
import com.couchbase.client.core.util.Resources;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link ViewHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ViewHandlerTest {

    private ObjectMapper mapper = new ObjectMapper();
    private Queue<ViewRequest> queue;
    private EmbeddedChannel channel;
    private Disruptor<ResponseEvent> responseBuffer;
    private List<CouchbaseMessage> firedEvents;
    private CountDownLatch latch;
    private ViewHandler handler;

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

        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.scheduler()).thenReturn(Schedulers.computation());
        AbstractEndpoint endpoint = mock(AbstractEndpoint.class);
        when(endpoint.environment()).thenReturn(environment);
        when(environment.userAgent()).thenReturn("Couchbase Client Mock");

        queue = new ArrayDeque<ViewRequest>();
        handler = new ViewHandler(endpoint, responseBuffer.start(), queue, false);
        channel = new EmbeddedChannel(handler);
    }

    @After
    public void clear() {
        responseBuffer.shutdown();
    }

    @Test
    public void shouldEncodeGetDesignDocumentRequest() {
        GetDesignDocumentRequest request = new GetDesignDocumentRequest("name", true, "bucket", "password");

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/bucket/_design/dev_name", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
    }

    @Test
    public void shouldDecodeSuccessfulGetDesignDocumentResponse() throws Exception {
        String response = Resources.read("designdoc_success.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GetDesignDocumentRequest requestMock = mock(GetDesignDocumentRequest.class);
        when(requestMock.name()).thenReturn("name");
        when(requestMock.development()).thenReturn(true);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GetDesignDocumentResponse inbound = (GetDesignDocumentResponse) firedEvents.get(0);

        assertTrue(inbound.status().isSuccess());
        assertEquals("name", inbound.name());
        assertEquals(true, inbound.development());
        assertEquals(response, inbound.content().toString(CharsetUtil.UTF_8));

    }

    @Test
    public void shouldEncodeViewQueryRequest() {
        ViewQueryRequest request = new ViewQueryRequest("design", "view", true, "query", "bucket", "password");

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/bucket/_design/dev_design/_view/view?query", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
    }

    @Test
    public void shouldDecodeWithDebugViewQueryResponse() throws Exception {
        String response = Resources.read("query_debug.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        ViewQueryRequest requestMock = mock(ViewQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        ViewQueryResponse inbound = (ViewQueryResponse) firedEvents.get(0);

        assertTrue(inbound.status().isSuccess());
        assertFalse(inbound.rows().toList().toBlocking().single().isEmpty());

        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                try {
                    Map found = mapper.readValue(byteBuf.toString(CharsetUtil.UTF_8), Map.class);
                    assertEquals(2, found.size());
                    assertTrue(found.containsKey("debug_info"));
                    assertTrue(found.containsKey("total_rows"));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertFalse(true);
                }
            }
        });
    }

    @Test
    public void shouldDecodeEmptyViewQueryResponse() throws Exception {
        String response = Resources.read("query_empty.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        ViewQueryRequest requestMock = mock(ViewQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        ViewQueryResponse inbound = (ViewQueryResponse) firedEvents.get(0);

        assertTrue(inbound.status().isSuccess());
        assertTrue(inbound.rows().toList().toBlocking().single().isEmpty());

        final AtomicInteger called = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                called.incrementAndGet();
                assertEquals("{\"total_rows\":7303}", byteBuf.toString(CharsetUtil.UTF_8));
            }
        });
        assertEquals(1, called.get());
    }

    @Test
    public void shouldDecodeOneViewQueryResponse() throws Exception {
        String response = Resources.read("query_one.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        ViewQueryRequest requestMock = mock(ViewQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        ViewQueryResponse inbound = (ViewQueryResponse) firedEvents.get(0);

        assertTrue(inbound.status().isSuccess());
        assertEquals(200, inbound.responseCode());
        assertEquals("OK", inbound.responsePhrase());

        final AtomicInteger calledRow = new AtomicInteger();
        inbound.rows().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                calledRow.incrementAndGet();
                try {
                    Map found = mapper.readValue(byteBuf.toString(CharsetUtil.UTF_8), Map.class);
                    assertEquals(3, found.size());
                    assertTrue(found.containsKey("id"));
                    assertTrue(found.containsKey("key"));
                    assertTrue(found.containsKey("value"));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertFalse(true);
                }
            }
        });
        assertEquals(1, calledRow.get());

        final AtomicInteger called = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                called.incrementAndGet();
                assertEquals("{\"total_rows\":7303}", byteBuf.toString(CharsetUtil.UTF_8));
            }
        });
        assertEquals(1, called.get());
    }

    @Test
    public void shouldDecodeManyViewQueryResponse() throws Exception {
        String response = Resources.read("query_many.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer(response.substring(0, 500), CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultHttpContent(Unpooled.copiedBuffer(response.substring(500, 1234), CharsetUtil.UTF_8));
        HttpContent responseChunk3 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response.substring(1234), CharsetUtil.UTF_8));

        ViewQueryRequest requestMock = mock(ViewQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2, responseChunk3);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        ViewQueryResponse inbound = (ViewQueryResponse) firedEvents.get(0);

        assertTrue(inbound.status().isSuccess());

        final AtomicInteger calledRow = new AtomicInteger();
        inbound.rows().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                calledRow.incrementAndGet();
                try {
                    Map found = mapper.readValue(byteBuf.toString(CharsetUtil.UTF_8), Map.class);
                    assertEquals(3, found.size());
                    assertTrue(found.containsKey("id"));
                    assertTrue(found.containsKey("key"));
                    assertTrue(found.containsKey("value"));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertFalse(true);
                }
            }
        });
        assertEquals(500, calledRow.get());

        final AtomicInteger called = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                called.incrementAndGet();
                assertEquals("{\"total_rows\":7303}", byteBuf.toString(CharsetUtil.UTF_8));
            }
        });
        assertEquals(1, called.get());
    }

    @Test
    public void shoulDecodeNotFoundViewQueryResponse() {

    }
}
