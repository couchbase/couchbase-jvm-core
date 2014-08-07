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
package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.message.config.BucketStreamingRequest;
import com.couchbase.client.core.message.config.BucketStreamingResponse;
import com.couchbase.client.core.message.config.ConfigRequest;
import com.couchbase.client.core.message.config.FlushRequest;
import com.couchbase.client.core.message.config.FlushResponse;
import com.couchbase.client.core.message.config.GetDesignDocumentsRequest;
import com.couchbase.client.core.message.config.GetDesignDocumentsResponse;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
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
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies the functionality of the {@link ConfigHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ConfigHandlerTest {

    private Queue<ConfigRequest> queue;
    private EmbeddedChannel channel;
    private Disruptor<ResponseEvent> responseBuffer;
    private List<CouchbaseMessage> firedEvents;
    private CountDownLatch latch;
    private ConfigHandler handler;

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

        queue = new ArrayDeque<ConfigRequest>();
        handler = new ConfigHandler(mock(AbstractEndpoint.class), responseBuffer.start(), queue);
        channel = new EmbeddedChannel(handler);
    }

    @After
    public void clear() {
        responseBuffer.shutdown();
    }

    @Test
    public void shouldEncodeBucketConfigRequest() throws Exception {
        BucketConfigRequest request = new BucketConfigRequest("/path/", InetAddress.getLocalHost(), "bucket", "password");

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/path/bucket", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Basic YnVja2V0OnBhc3N3b3Jk", outbound.headers().get(HttpHeaders.Names.AUTHORIZATION));
    }

    @Test
    public void shouldDecodeSuccessBucketConfigResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("foo", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultLastHttpContent(Unpooled.copiedBuffer("bar", CharsetUtil.UTF_8));

        BucketConfigRequest requestMock = mock(BucketConfigRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        BucketConfigResponse inbound = (BucketConfigResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals("foobar", inbound.config());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void shouldDecodeAuthFailureBucketConfigResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
            new HttpResponseStatus(401, "Unauthorized"));
        HttpContent responseChunk = LastHttpContent.EMPTY_LAST_CONTENT;

        BucketConfigRequest requestMock = mock(BucketConfigRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        BucketConfigResponse inbound = (BucketConfigResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.FAILURE, inbound.status());
        assertEquals("Unauthorized", inbound.config());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void shouldDecodeNotFoundBucketConfigResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
            new HttpResponseStatus(404, "Object Not Found"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer("Not found.", CharsetUtil.UTF_8));

        BucketConfigRequest requestMock = mock(BucketConfigRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        BucketConfigResponse inbound = (BucketConfigResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.NOT_EXISTS, inbound.status());
        assertEquals("Not found.", inbound.config());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void shouldEncodeFlushRequest() {
        FlushRequest request = new FlushRequest("bucket", "password");

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.POST, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/pools/default/buckets/bucket/controller/doFlush", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Basic YnVja2V0OnBhc3N3b3Jk", outbound.headers().get(HttpHeaders.Names.AUTHORIZATION));
    }

    @Test
    public void shouldDecodeSuccessFlushResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
            new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = LastHttpContent.EMPTY_LAST_CONTENT;

        FlushRequest requestMock = mock(FlushRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        FlushResponse inbound = (FlushResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals("OK", inbound.content());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void shouldDecodeFlushNotEnabledResponse() throws Exception {
        String content = "{\"_\":\"Flush is disabled for the bucket\"}";
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
            new HttpResponseStatus(400, "Bad Request"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(content, CharsetUtil.UTF_8));

        FlushRequest requestMock = mock(FlushRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        FlushResponse inbound = (FlushResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.FAILURE, inbound.status());
        assertEquals("{\"_\":\"Flush is disabled for the bucket\"}", inbound.content());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void shouldDecodeListDesignDocumentResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("foo", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultLastHttpContent(Unpooled.copiedBuffer("bar", CharsetUtil.UTF_8));

        GetDesignDocumentsRequest requestMock = mock(GetDesignDocumentsRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GetDesignDocumentsResponse inbound = (GetDesignDocumentsResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals("foobar", inbound.content());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void shouldEncodeBucketStreamingRequest() throws Exception {
        BucketStreamingRequest request = new BucketStreamingRequest("/path/", "bucket", "password");

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/path/bucket", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Basic YnVja2V0OnBhc3N3b3Jk", outbound.headers().get(HttpHeaders.Names.AUTHORIZATION));
    }

    @Test
    public void shouldDecodeInitialBucketStreamingResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        BucketStreamingResponse inbound = (BucketStreamingResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertNotNull(inbound.configs());
        assertNotNull(inbound.host());
        assertEquals(0, queue.size());
    }

    @Test
    public void shouldPushSubsequentChunks() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("config", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultHttpContent(Unpooled.copiedBuffer("\n\n\n\n", CharsetUtil.UTF_8));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        BucketStreamingResponse inbound = (BucketStreamingResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertNotNull(inbound.configs());
        assertNotNull(inbound.host());

        Observable<String> configs = inbound.configs();
        assertEquals("config", configs.toBlocking().first());
    }

    @Test
    public void shouldPushMixedSizeChunksCorrectly() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("conf", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultHttpContent(Unpooled.copiedBuffer("ig\n", CharsetUtil.UTF_8));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        BucketStreamingResponse inbound = (BucketStreamingResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertNotNull(inbound.configs());
        assertNotNull(inbound.host());

        Observable<String> configs = inbound.configs();

        final CountDownLatch latch = new CountDownLatch(2);
        configs.forEach(new Action1<String>() {
            @Override
            public void call(String config) {
                assertTrue(config.equals("config") || config.equals("new"));
                latch.countDown();
            }
        });

        HttpContent responseChunk3 = new DefaultHttpContent(Unpooled.copiedBuffer("\n\n\nne", CharsetUtil.UTF_8));
        HttpContent responseChunk4 = new DefaultHttpContent(Unpooled.copiedBuffer("w\n\n\n\n", CharsetUtil.UTF_8));

        channel.writeInbound(responseChunk3, responseChunk4);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void shouldDecodeFailingInitialBucketStreamingResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(404, "Object Not Found"));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        BucketStreamingResponse inbound = (BucketStreamingResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.NOT_EXISTS, inbound.status());
        assertNull(inbound.configs());
        assertNotNull(inbound.host());
        assertEquals(0, queue.size());
    }

    @Test
    public void shouldResetStateIfStreamCloses() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("conf", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultHttpContent(Unpooled.copiedBuffer("ig\n", CharsetUtil.UTF_8));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        BucketStreamingResponse inbound = (BucketStreamingResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertNotNull(inbound.configs());
        assertNotNull(inbound.host());

        Observable<String> configs = inbound.configs();

        final CountDownLatch latch = new CountDownLatch(3);
        configs.subscribe(new Subscriber<String>() {
            @Override
            public void onCompleted() {
                latch.countDown();
            }

            @Override
            public void onError(Throwable e) {
                assertTrue(false);
            }

            @Override
            public void onNext(String config) {
                assertTrue(config.equals("config") || config.equals("new"));
                latch.countDown();
            }
        });

        HttpContent responseChunk3 = new DefaultHttpContent(Unpooled.copiedBuffer("\n\n\nne", CharsetUtil.UTF_8));
        HttpContent responseChunk4 = new DefaultLastHttpContent(Unpooled.copiedBuffer("w\n\n\n\n", CharsetUtil.UTF_8));

        channel.writeInbound(responseChunk3, responseChunk4);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void shouldCloseStreamIfChannelDies() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("conf", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultHttpContent(Unpooled.copiedBuffer("ig\n\n\n\n", CharsetUtil.UTF_8));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        BucketStreamingResponse inbound = (BucketStreamingResponse) firedEvents.get(0);

        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertNotNull(inbound.configs());
        assertNotNull(inbound.host());

        Observable<String> configs = inbound.configs();

        final CountDownLatch latch = new CountDownLatch(1);
        configs.subscribe(new Subscriber<String>() {
            @Override
            public void onCompleted() {
                latch.countDown();
            }

            @Override
            public void onError(Throwable e) {

            }

            @Override
            public void onNext(String s) {

            }
        });

        channel.pipeline().remove(handler);
        channel.disconnect().awaitUninterruptibly();
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

}
