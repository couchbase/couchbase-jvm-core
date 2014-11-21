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
package com.couchbase.client.core.endpoint.query;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.message.query.QueryRequest;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link QueryHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class QueryHandlerTest {

    private ObjectMapper mapper = new ObjectMapper();
    private Queue<QueryRequest> queue;
    private EmbeddedChannel channel;
    private Disruptor<ResponseEvent> responseBuffer;
    private List<CouchbaseMessage> firedEvents;
    private CountDownLatch latch;
    private QueryHandler handler;

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

        queue = new ArrayDeque<QueryRequest>();
        handler = new QueryHandler(endpoint, responseBuffer.start(), queue);
        channel = new EmbeddedChannel(handler);
    }

    @After
    public void clear() {
        responseBuffer.shutdown();
    }

    @Test
    public void shouldEncodeGenericQueryRequest() {
        GenericQueryRequest request = new GenericQueryRequest("query", "bucket", "password");

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.POST, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/query", outbound.getUri());
        assertFalse(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
    }

    @Test
    public void shouldDecodeErrorResponse() throws Exception {
        String response = Resources.read("parse_error.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        assertFalse(inbound.status().isSuccess());
        assertEquals(ResponseStatus.FAILURE, inbound.status());

        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf buf) {
                String response = buf.toString(CharsetUtil.UTF_8);
                try {
                    Map found = mapper.readValue(response, Map.class);
                    assertEquals(5, found.size());
                    assertEquals(new Integer(4100), found.get("code"));
                    assertEquals("Parse Error", found.get("message"));
                } catch (IOException e) {
                    assertFalse(true);
                }
            }
        });
    }

    @Test
    public void shouldDecodeChunkedErrorResponse() throws Exception {
        String response = Resources.read("parse_error.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer(response.substring(0, 100), CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response.substring(100), CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        assertFalse(inbound.status().isSuccess());
        assertEquals(ResponseStatus.FAILURE, inbound.status());

        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf buf) {
                String response = buf.toString(CharsetUtil.UTF_8);
                try {
                    Map found = mapper.readValue(response, Map.class);
                    assertEquals(5, found.size());
                    assertEquals(new Integer(4100), found.get("code"));
                    assertEquals("Parse Error", found.get("message"));
                } catch (IOException e) {
                    assertFalse(true);
                }
            }
        });
    }

    @Test
    public void shouldDecodeEmptySuccessResponse() throws Exception {
        String response = Resources.read("success_0.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        assertTrue(inbound.status().isSuccess());
        assertEquals(ResponseStatus.SUCCESS, inbound.status());

        final AtomicInteger invokeCounter = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf buf) {
                invokeCounter.incrementAndGet();
                String response = buf.toString(CharsetUtil.UTF_8);
                try {
                    Map found = mapper.readValue(response, Map.class);
                    assertEquals(4, found.size());
                    assertTrue(found.containsKey("code"));
                    assertTrue(found.containsKey("key"));
                } catch (IOException e) {
                    assertFalse(true);
                }
            }
        });

        assertEquals(2, invokeCounter.get());
        assertTrue(inbound.rows().toList().toBlocking().single().isEmpty());
    }

    @Test
    public void shouldDecodeOneRowResponse() throws Exception {
        String response = Resources.read("success_1.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        assertTrue(inbound.status().isSuccess());
        assertEquals(ResponseStatus.SUCCESS, inbound.status());

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        inbound.rows().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf buf) {
                invokeCounter1.incrementAndGet();
                String response = buf.toString(CharsetUtil.UTF_8);
                try {
                    Map found = mapper.readValue(response, Map.class);
                    assertEquals(12, found.size());
                    assertEquals("San Francisco", found.get("city"));
                    assertEquals("United States", found.get("country"));
                    Map geo = (Map) found.get("geo");
                    assertNotNull(geo);
                    assertEquals(3, geo.size());
                    assertEquals("ROOFTOP", geo.get("accuracy"));
                } catch (IOException e) {
                    assertFalse(true);
                }
            }
        });

        assertEquals(1, invokeCounter1.get());

        final AtomicInteger invokeCounter2 = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf buf) {
                invokeCounter2.incrementAndGet();
                String response = buf.toString(CharsetUtil.UTF_8);
                try {
                    Map found = mapper.readValue(response, Map.class);
                    assertEquals(4, found.size());
                    assertTrue(found.containsKey("code"));
                    assertTrue(found.containsKey("key"));
                } catch (IOException e) {
                    assertFalse(true);
                }
            }
        });

        assertEquals(2, invokeCounter2.get());
    }

    @Test
    public void shouldDecodeNRowResponse() throws Exception {
        String response = Resources.read("success_5.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        assertTrue(inbound.status().isSuccess());
        assertEquals(ResponseStatus.SUCCESS, inbound.status());

        List<ByteBuf> found = inbound.rows().toList().toBlocking().single();
        assertEquals(5, found.size());
        for (ByteBuf row : found) {
            String content = row.toString(CharsetUtil.UTF_8);
            assertNotNull(content);
            assertTrue(!content.isEmpty());
            try {
                Map decoded = mapper.readValue(content, Map.class);
                assertTrue(decoded.size() > 0);
                assertTrue(decoded.containsKey("name"));
            } catch(Exception e) {
                assertTrue(false);
            }
        }

        final AtomicInteger invokeCounter2 = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf buf) {
                invokeCounter2.incrementAndGet();
                String response = buf.toString(CharsetUtil.UTF_8);
                try {
                    Map found = mapper.readValue(response, Map.class);
                    assertEquals(4, found.size());
                    assertTrue(found.containsKey("code"));
                    assertTrue(found.containsKey("key"));
                } catch (IOException e) {
                    assertFalse(true);
                }
            }
        });

        assertEquals(2, invokeCounter2.get());
    }

    @Test
    public void shouldDecodeNRowResponseChunked() throws Exception {
        String response = Resources.read("success_5.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response.substring(0, 300),
            CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response.substring(300, 950),
            CharsetUtil.UTF_8));
        HttpContent responseChunk3 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response.substring(950, 1345),
            CharsetUtil.UTF_8));
        HttpContent responseChunk4 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response.substring(1345, 3000),
            CharsetUtil.UTF_8));
        HttpContent responseChunk5 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response.substring(3000),
            CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2, responseChunk3, responseChunk4,
            responseChunk5);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        assertTrue(inbound.status().isSuccess());
        assertEquals(ResponseStatus.SUCCESS, inbound.status());

        List<ByteBuf> found = inbound.rows().toList().toBlocking().single();
        assertEquals(5, found.size());
        for (ByteBuf row : found) {
            String content = row.toString(CharsetUtil.UTF_8);
            assertNotNull(content);
            assertTrue(!content.isEmpty());
            try {
                Map decoded = mapper.readValue(content, Map.class);
                assertTrue(decoded.size() > 0);
                assertTrue(decoded.containsKey("name"));
            } catch(Exception e) {
                assertTrue(false);
            }
        }

        final AtomicInteger invokeCounter2 = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf buf) {
                invokeCounter2.incrementAndGet();
                String response = buf.toString(CharsetUtil.UTF_8);
                try {
                    Map found = mapper.readValue(response, Map.class);
                    assertEquals(4, found.size());
                    assertTrue(found.containsKey("code"));
                    assertTrue(found.containsKey("key"));
                } catch (IOException e) {
                    assertFalse(true);
                }
            }
        });

        assertEquals(2, invokeCounter2.get());
    }

}
