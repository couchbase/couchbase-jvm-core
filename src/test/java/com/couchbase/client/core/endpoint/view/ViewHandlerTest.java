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
package com.couchbase.client.core.endpoint.view;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.view.GetDesignDocumentRequest;
import com.couchbase.client.core.message.view.GetDesignDocumentResponse;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.message.view.ViewRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.util.Resources;
import com.couchbase.client.core.utils.DefaultObjectMapper;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
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
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link ViewHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ViewHandlerTest {

    private Queue<ViewRequest> queue;
    private EmbeddedChannel channel;
    private Disruptor<ResponseEvent> responseBuffer;
    private RingBuffer<ResponseEvent> responseRingBuffer;
    private List<CouchbaseMessage> firedEvents;
    private CountDownLatch latch;
    private ViewHandler handler;
    private AbstractEndpoint endpoint;

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

        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.scheduler()).thenReturn(Schedulers.computation());
        when(environment.maxRequestLifetime()).thenReturn(10000L); // 10 seconds
        when(environment.autoreleaseAfter()).thenReturn(2000L);
        when(environment.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);
        endpoint = mock(AbstractEndpoint.class);
        when(endpoint.environment()).thenReturn(environment);
        when(endpoint.context()).thenReturn(new CoreContext(environment, null, 1));
        when(environment.userAgent()).thenReturn("Couchbase Client Mock");

        queue = new ArrayDeque<ViewRequest>();
        handler = new ViewHandler(endpoint, responseRingBuffer, queue, false, false);
        channel = new EmbeddedChannel(handler);
    }

    @After
    public void clear() {
        //triggers the release of the responseContent common buffer
        channel.close().awaitUninterruptibly();
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
        assertTrue(outbound.headers().contains(HttpHeaders.Names.HOST));
        ReferenceCountUtil.releaseLater(outbound); //for consistency, but it uses Unpooled
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
        ReferenceCountUtil.releaseLater(inbound);
    }

    @Test
    public void shouldEncodeViewQueryRequest() {
        ViewQueryRequest request = new ViewQueryRequest("design", "view", true, "query", null, "bucket", "password");

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/bucket/_design/dev_design/_view/view?query", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
        ReferenceCountUtil.releaseLater(outbound); //for consistency, but it uses Unpooled
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
        assertEquals(5, countAndRelease(inbound.rows()));

        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                try {
                    Map<String, Object> found = DefaultObjectMapper.readValueAsMap(byteBuf.toString(CharsetUtil.UTF_8));
                    assertEquals(2, found.size());
                    assertTrue(found.containsKey("debug_info"));
                    assertTrue(found.containsKey("total_rows"));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertFalse(true);
                }
                ReferenceCountUtil.releaseLater(byteBuf);
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
                ReferenceCountUtil.releaseLater(byteBuf);
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
                    Map<String, Object> found = DefaultObjectMapper.readValueAsMap(byteBuf.toString(CharsetUtil.UTF_8));
                    assertEquals(3, found.size());
                    assertTrue(found.containsKey("id"));
                    assertTrue(found.containsKey("key"));
                    assertTrue(found.containsKey("value"));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertFalse(true);
                }
                ReferenceCountUtil.releaseLater(byteBuf);
            }
        });
        assertEquals(1, calledRow.get());

        final AtomicInteger called = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                called.incrementAndGet();
                assertEquals("{\"total_rows\":7303}", byteBuf.toString(CharsetUtil.UTF_8));
                ReferenceCountUtil.releaseLater(byteBuf);
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
                    Map<String, Object> found = DefaultObjectMapper.readValueAsMap(byteBuf.toString(CharsetUtil.UTF_8));
                    assertEquals(3, found.size());
                    assertTrue(found.containsKey("id"));
                    assertTrue(found.containsKey("key"));
                    assertTrue(found.containsKey("value"));
                } catch (IOException e) {
                    e.printStackTrace();
                    assertFalse(true);
                }
                ReferenceCountUtil.releaseLater(byteBuf);

            }
        });
        assertEquals(500, calledRow.get());

        final AtomicInteger called = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                called.incrementAndGet();
                assertEquals("{\"total_rows\":7303}", byteBuf.toString(CharsetUtil.UTF_8));
                ReferenceCountUtil.releaseLater(byteBuf);
            }
        });
        assertEquals(1, called.get());
    }

    @Test
    public void shouldFireKeepAlive() throws Exception {
        final AtomicInteger keepAliveEventCounter = new AtomicInteger();
        final AtomicReference<ChannelHandlerContext> ctxRef = new AtomicReference();

        ViewHandler testHandler = new ViewHandler(endpoint, responseRingBuffer, queue, false, false) {
            @Override
            public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
                super.channelRegistered(ctx);
                ctxRef.compareAndSet(null, ctx);
            }

            @Override
            protected void onKeepAliveFired(ChannelHandlerContext ctx, CouchbaseRequest keepAliveRequest) {
                assertEquals(1, keepAliveEventCounter.incrementAndGet());
            }

            @Override
            protected void onKeepAliveResponse(ChannelHandlerContext ctx, CouchbaseResponse keepAliveResponse) {
                assertEquals(2, keepAliveEventCounter.incrementAndGet());
            }

            @Override
            protected CoreEnvironment env() {
                return DefaultCoreEnvironment.builder()
                        .continuousKeepAliveEnabled(false).build();
            }
        };
        EmbeddedChannel channel = new EmbeddedChannel(testHandler);

        //test idle event triggers a view keepAlive request and hook is called
        testHandler.userEventTriggered(ctxRef.get(), IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT);

        assertEquals(1, keepAliveEventCounter.get());
        assertTrue(queue.peek() instanceof ViewHandler.KeepAliveRequest);
        ViewHandler.KeepAliveRequest keepAliveRequest = (ViewHandler.KeepAliveRequest) queue.peek();

        //test responding to the request with http response is interpreted into a KeepAliveResponse and hook is called
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
        channel.writeInbound(response);
        ViewHandler.KeepAliveResponse keepAliveResponse = keepAliveRequest.observable()
                .cast(ViewHandler.KeepAliveResponse.class)
                .timeout(1, TimeUnit.SECONDS).toBlocking().single();

        assertEquals(2, keepAliveEventCounter.get());
        assertEquals(ResponseStatus.NOT_EXISTS, keepAliveResponse.status());
        //different channel, needs to be closed to release the internal responseContent
        channel.close().awaitUninterruptibly();
    }

    @Test
    public void shouldEncodeLongViewQueryRequestWithPOST() {
        String keys = Resources.read("key_many.json", this.getClass());
        String query = "stale=false&endKey=test";
        ViewQueryRequest request = new ViewQueryRequest("design", "view", true, query, keys, "bucket", "password");
        channel.writeOutbound(request);
        DefaultFullHttpRequest outbound = (DefaultFullHttpRequest) channel.readOutbound();
        assertEquals(HttpMethod.POST, outbound.getMethod());
        assertFalse(outbound.getUri().contains("keys="));
        assertTrue(outbound.getUri().endsWith("?stale=false&endKey=test"));
        String content = outbound.content().toString(CharsetUtil.UTF_8);
        assertTrue(content.startsWith("{\"keys\":["));
        assertTrue(content.endsWith("]}"));
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldUrlEncodeShortKeys() {
        String urlEncodedKeys = "%5B%221%22%2C%222%22%2C%223%22%5D";
        String keys = "[\"1\",\"2\",\"3\"]";
        String query = "stale=false&endKey=test";
        ViewQueryRequest request = new ViewQueryRequest("design", "view", true, query, keys, "bucket", "password");
        channel.writeOutbound(request);
        DefaultFullHttpRequest outbound = (DefaultFullHttpRequest) channel.readOutbound();
        String failMsg = outbound.getUri();
        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertTrue(failMsg, outbound.getUri().contains("keys="));
        assertTrue(failMsg, outbound.getUri().endsWith("?stale=false&endKey=test&keys=" + urlEncodedKeys));
        String content = outbound.content().toString(CharsetUtil.UTF_8);
        assertTrue(content.isEmpty());
        ReferenceCountUtil.releaseLater(outbound); //NO-OP since content is empty but still...
    }

    @Test
    public void shouldProduceValidUrlIfShortKeysAndNoOtherQueryParam() {
        String urlEncodedKeys = "%5B%221%22%2C%222%22%2C%223%22%5D";
        String keys = "[\"1\",\"2\",\"3\"]";
        String query = "";
        ViewQueryRequest request = new ViewQueryRequest("design", "view", true, query, keys, "bucket", "password");
        channel.writeOutbound(request);
        DefaultFullHttpRequest outbound = (DefaultFullHttpRequest) channel.readOutbound();
        String failMsg = outbound.getUri();
        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertTrue(failMsg, outbound.getUri().endsWith("?keys=" + urlEncodedKeys));
        String content = outbound.content().toString(CharsetUtil.UTF_8);
        assertTrue(content.isEmpty());
        ReferenceCountUtil.releaseLater(outbound); //NO-OP since content is empty but still...
    }

    @Test
    public void shouldDoNothingOnNullKeys() {
        String keys = null;
        String query = "stale=false&endKey=test";
        ViewQueryRequest request = new ViewQueryRequest("design", "view", true, query, keys, "bucket", "password");
        channel.writeOutbound(request);
        DefaultFullHttpRequest outbound = (DefaultFullHttpRequest) channel.readOutbound();
        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertFalse(outbound.getUri().contains("keys="));
        assertTrue(outbound.getUri().endsWith("?stale=false&endKey=test"));
        String content = outbound.content().toString(CharsetUtil.UTF_8);
        assertTrue(content.isEmpty());
        ReferenceCountUtil.releaseLater(outbound); //NO-OP since content is empty but still...
    }

    @Test
    public void shouldDoNothingOnEmptyKeys() {
        String keys = "";
        String query = "stale=false&endKey=test";
        ViewQueryRequest request = new ViewQueryRequest("design", "view", true, query, keys, "bucket", "password");
        channel.writeOutbound(request);
        DefaultFullHttpRequest outbound = (DefaultFullHttpRequest) channel.readOutbound();
        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertFalse(outbound.getUri().contains("keys="));
        assertTrue(outbound.getUri().endsWith("?stale=false&endKey=test"));
        String content = outbound.content().toString(CharsetUtil.UTF_8);
        assertTrue(content.isEmpty());
        ReferenceCountUtil.releaseLater(outbound); //NO-OP since content is empty but still...
    }

    @Test
    public void shouldNotBreakLinesOnLongAuth() {
        String longPassword = "thisIsAveryLongPasswordWhichShouldNotContainLineBreaksAfterEncodingOtherwise"
            + "itWillBreakTheRequestResponseFlowWithTheServer";
        GetDesignDocumentRequest request = new GetDesignDocumentRequest("name", true, "bucket", longPassword);

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/bucket/_design/dev_name", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertNotNull(outbound.headers().get(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
        ReferenceCountUtil.releaseLater(outbound); //for consistency, but it uses Unpooled
    }

    @Test
    public void shouldParseErrorWithEmptyRows() throws Exception {
        String response = Resources.read("error_empty_rows.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        ViewQueryRequest requestMock = mock(ViewQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        ViewQueryResponse inbound = (ViewQueryResponse) firedEvents.get(0);
        assertTrue(inbound.status().isSuccess());

        assertEquals(0, countAndRelease(inbound.rows()));

        String error = inbound.error().toBlocking().single();
        Map<String, Object> parsed = DefaultObjectMapper.readValueAsMap(error);
        assertEquals(1, parsed.size());
        assertNotNull(parsed.get("errors"));
    }

    @Test
    public void shouldParseErrorAfterRows() throws Exception {
        String response = Resources.read("error_rows.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        ViewQueryRequest requestMock = mock(ViewQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        ViewQueryResponse inbound = (ViewQueryResponse) firedEvents.get(0);
        assertTrue(inbound.status().isSuccess());

        assertEquals(10, countAndRelease(inbound.rows()));

        String error = inbound.error().toBlocking().single();
        Map<String, Object> parsed = DefaultObjectMapper.readValueAsMap(error);
        assertEquals(1, parsed.size());
        assertNotNull(parsed.get("errors"));
    }

    @Test
    public void shouldParseErrorWithDesignNotFound() throws Exception {
        String response = Resources.read("designdoc_notfound.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(404, "Object Not Found"));
        HttpContent responseChunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        ViewQueryRequest requestMock = mock(ViewQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        ViewQueryResponse inbound = (ViewQueryResponse) firedEvents.get(0);
        assertFalse(inbound.status().isSuccess());
        assertEquals(ResponseStatus.NOT_EXISTS, inbound.status());

        assertEquals(0, countAndRelease(inbound.rows()));

        String error = inbound.error().toBlocking().single();
        assertEquals("{\"errors\":[{\"error\":\"not_found\",\"reason\":\"Design document _design/designdoc not found\"}]}", error);

    }

    @Test
    public void shouldParseRowWithBracketInIdKeysAndValue() throws Exception {
        String response = Resources.read("query_brackets.json", this.getClass());
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

        ByteBuf singleRow = inbound.rows().toBlocking().single(); //single will blow up if not exactly one
        String singleRowData = singleRow.toString(CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(singleRow);
        Map<String, Object> found = null;
        try {
            found = DefaultObjectMapper.readValueAsMap(singleRowData);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed parsing JSON on data " + singleRowData);
        }

        assertEquals(3, found.size());
        assertTrue(found.containsKey("id"));
        assertTrue(found.containsKey("key"));
        assertTrue(found.containsKey("value"));
        assertEquals("IdClosing}BracketId", found.get("id"));
        assertEquals(Arrays.asList("KeyClosing}BracketKey", "KeySquareClosing]SquareBracketKey"), found.get("key"));
        assertEquals("ValueClosing}BracketValue", found.get("value"));

        final AtomicInteger called = new AtomicInteger();
        inbound.info().toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                called.incrementAndGet();
                assertEquals("{\"total_rows\":1}", byteBuf.toString(CharsetUtil.UTF_8));
                ReferenceCountUtil.releaseLater(byteBuf);
            }
        });
        assertEquals(1, called.get());
    }

    private int countAndRelease(Observable<ByteBuf> bufObservable) {
        return bufObservable
                .doOnNext(new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf byteBuf) {
                        byteBuf.release();
                    }
                }).count()
                .toBlocking()
                .singleOrDefault(0);
    }

    @Test
    public void shouldHavePipeliningDisabled() {
        Subject<CouchbaseResponse,CouchbaseResponse> obs1 = AsyncSubject.create();
        ViewQueryRequest requestMock1 = mock(ViewQueryRequest.class);
        when(requestMock1.query()).thenReturn("{...}");
        when(requestMock1.bucket()).thenReturn("foo");
        when(requestMock1.username()).thenReturn("foo");
        when(requestMock1.password()).thenReturn("");
        when(requestMock1.observable()).thenReturn(obs1);
        when(requestMock1.isActive()).thenReturn(true);

        Subject<CouchbaseResponse,CouchbaseResponse> obs2 = AsyncSubject.create();
        ViewQueryRequest requestMock2 = mock(ViewQueryRequest.class);
        when(requestMock2.query()).thenReturn("{...}");
        when(requestMock2.bucket()).thenReturn("foo");
        when(requestMock2.username()).thenReturn("foo");
        when(requestMock2.password()).thenReturn("");
        when(requestMock2.observable()).thenReturn(obs2);
        when(requestMock2.isActive()).thenReturn(true);


        TestSubscriber<CouchbaseResponse> t1 = TestSubscriber.create();
        TestSubscriber<CouchbaseResponse> t2 = TestSubscriber.create();

        obs1.subscribe(t1);
        obs2.subscribe(t2);

        channel.writeOutbound(requestMock1, requestMock2);

        t1.assertNotCompleted();
        t2.assertError(RequestCancelledException.class);
    }

}
