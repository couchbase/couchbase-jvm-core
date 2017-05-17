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
package com.couchbase.client.core.endpoint.config;

import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseResponse;
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
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.util.CollectingResponseEventSink;
import com.couchbase.client.core.utils.NetworkAddress;
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
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link ConfigHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ConfigHandlerTest {

    /**
     * The default charset used for all interactions.
     */
    private static final Charset CHARSET = CharsetUtil.UTF_8;

    /**
     * The queue in which requests are pushed to only test decodes in isolation manually.
     */
    private Queue<ConfigRequest> requestQueue;

    /**
     * The channel in which the handler is tested.
     */
    private EmbeddedChannel channel;

    /**
     * The actual handler.
     */
    private ConfigHandler handler;

    /**
     * Represents a custom event sink that collects all events pushed into it.
     */
    private CollectingResponseEventSink eventSink;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.scheduler()).thenReturn(Schedulers.computation());
        AbstractEndpoint endpoint = mock(AbstractEndpoint.class);
        when(endpoint.environment()).thenReturn(environment);
        when(environment.userAgent()).thenReturn("Couchbase Client Mock");
        when(environment.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);

        eventSink = new CollectingResponseEventSink();
        requestQueue = new ArrayDeque<ConfigRequest>();
        handler = new ConfigHandler(endpoint, eventSink, requestQueue, true, false);
        channel = new EmbeddedChannel(handler);
    }

    @After
    public void cleanup() {
        channel.close().awaitUninterruptibly();
    }

    @Test
    public void shouldEncodeBucketConfigRequest() throws Exception {
        BucketConfigRequest request = new BucketConfigRequest("/path/", NetworkAddress.localhost(), "bucket", "password");

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/path/bucket", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertTrue(outbound.headers().contains(HttpHeaders.Names.HOST));
        assertEquals("Basic YnVja2V0OnBhc3N3b3Jk", outbound.headers().get(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
    }


    @Test
    public void shouldDecodeSuccessBucketConfigResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("foo", CHARSET));
        HttpContent responseChunk2 = new DefaultLastHttpContent(Unpooled.copiedBuffer("bar", CHARSET));

        BucketConfigRequest requestMock = mock(BucketConfigRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);
        channel.readInbound();

        assertEquals(1, eventSink.responseEvents().size());
        BucketConfigResponse event = (BucketConfigResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertEquals("foobar", event.config());
        assertTrue(requestQueue.isEmpty());
    }

    @Test
    public void shouldDecodeAuthFailureBucketConfigResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
            new HttpResponseStatus(401, "Unauthorized"));
        HttpContent responseChunk = LastHttpContent.EMPTY_LAST_CONTENT;

        BucketConfigRequest requestMock = mock(BucketConfigRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);

        assertEquals(1, eventSink.responseEvents().size());
        BucketConfigResponse event = (BucketConfigResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.FAILURE, event.status());
        assertEquals("Unauthorized", event.config());
        assertTrue(requestQueue.isEmpty());
    }

    @Test
    public void shouldDecodeNotFoundBucketConfigResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
            new HttpResponseStatus(404, "Object Not Found"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer("Not found.", CharsetUtil.UTF_8));

        BucketConfigRequest requestMock = mock(BucketConfigRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);

        assertEquals(1, eventSink.responseEvents().size());
        BucketConfigResponse event = (BucketConfigResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.NOT_EXISTS, event.status());
        assertEquals("Not found.", event.config());
        assertTrue(requestQueue.isEmpty());
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
        assertTrue(outbound.headers().contains(HttpHeaders.Names.HOST));
        assertEquals("Basic YnVja2V0OnBhc3N3b3Jk", outbound.headers().get(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
    }

    @Test
    public void shouldDecodeSuccessFlushResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
            new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = LastHttpContent.EMPTY_LAST_CONTENT;

        FlushRequest requestMock = mock(FlushRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);

        assertEquals(1, eventSink.responseEvents().size());
        FlushResponse event = (FlushResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertEquals("OK", event.content());
        assertTrue(requestQueue.isEmpty());
    }

    @Test
    public void shouldDecodeFlushNotEnabledResponse() throws Exception {
        String content = "{\"_\":\"Flush is disabled for the bucket\"}";
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
            new HttpResponseStatus(400, "Bad Request"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(content, CharsetUtil.UTF_8));

        FlushRequest requestMock = mock(FlushRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);

        assertEquals(1, eventSink.responseEvents().size());
        FlushResponse event = (FlushResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.INVALID_ARGUMENTS, event.status());
        assertEquals("{\"_\":\"Flush is disabled for the bucket\"}", event.content());
        assertTrue(requestQueue.isEmpty());
    }

    @Test
    public void shouldDecodeListDesignDocumentsResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("foo", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultLastHttpContent(Unpooled.copiedBuffer("bar", CharsetUtil.UTF_8));

        GetDesignDocumentsRequest requestMock = mock(GetDesignDocumentsRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);

        assertEquals(1, eventSink.responseEvents().size());
        GetDesignDocumentsResponse event = (GetDesignDocumentsResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertEquals("foobar", event.content());
        assertTrue(requestQueue.isEmpty());
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
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
    }

    @Test
    public void shouldDecodeInitialBucketStreamingResponse() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader);

        assertEquals(1, eventSink.responseEvents().size());
        BucketStreamingResponse event = (BucketStreamingResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertNotNull(event.configs());
        assertNotNull(event.host());
        assertEquals(0, requestQueue.size());
        ReferenceCountUtil.releaseLater(event);
        ReferenceCountUtil.releaseLater(responseHeader);
    }

    @Test
    public void shouldPushSubsequentChunks() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("config", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultHttpContent(Unpooled.copiedBuffer("\n\n\n\n", CharsetUtil.UTF_8));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);

        assertEquals(1, eventSink.responseEvents().size());
        BucketStreamingResponse event = (BucketStreamingResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertNotNull(event.configs());
        assertNotNull(event.host());

        Observable<String> configs = event.configs();
        assertEquals("config", configs.toBlocking().first());
    }

    @Test
    public void shouldPushMixedSizeChunksCorrectly() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("conf", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultHttpContent(Unpooled.copiedBuffer("ig\n", CharsetUtil.UTF_8));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);

        assertEquals(1, eventSink.responseEvents().size());
        BucketStreamingResponse event = (BucketStreamingResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertNotNull(event.configs());
        assertNotNull(event.host());

        Observable<String> configs = event.configs();

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
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader);

        assertEquals(1, eventSink.responseEvents().size());
        BucketStreamingResponse event = (BucketStreamingResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.NOT_EXISTS, event.status());
        assertNull(event.configs());
        assertNotNull(event.host());
        assertEquals(0, requestQueue.size());
        ReferenceCountUtil.releaseLater(responseHeader);
        ReferenceCountUtil.releaseLater(event);

    }

    @Test
    public void shouldResetStateIfStreamCloses() throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer("conf", CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultHttpContent(Unpooled.copiedBuffer("ig\n", CharsetUtil.UTF_8));

        BucketStreamingRequest requestMock = mock(BucketStreamingRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);

        assertEquals(1, eventSink.responseEvents().size());
        BucketStreamingResponse event = (BucketStreamingResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertNotNull(event.configs());
        assertNotNull(event.host());

        Observable<String> configs = event.configs();

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
        requestQueue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);

        assertEquals(1, eventSink.responseEvents().size());
        BucketStreamingResponse event = (BucketStreamingResponse) eventSink.responseEvents().get(0).getMessage();

        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertNotNull(event.configs());
        assertNotNull(event.host());

        Observable<String> configs = event.configs();

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

    @Test
    public void shouldNotBreakLinesOnLongAuth() throws Exception {
        String longPassword = "thisIsAveryLongPasswordWhichShouldNotContainLineBreaksAfterEncodingOtherwise"
            + "itWillBreakTheRequestResponseFlowWithTheServer";
        BucketConfigRequest request = new BucketConfigRequest("/path/", NetworkAddress.localhost(), "bucket",
            longPassword);

        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/path/bucket", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertNotNull(outbound.headers().get(HttpHeaders.Names.AUTHORIZATION));
        assertTrue(outbound.headers().contains(HttpHeaders.Names.HOST));
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
    }

    @Test
    public void shouldHavePipeliningDisabled() {
        Subject<CouchbaseResponse,CouchbaseResponse> obs1 = AsyncSubject.create();
        GetDesignDocumentsRequest requestMock1 = mock(GetDesignDocumentsRequest.class);
        when(requestMock1.path()).thenReturn("");
        when(requestMock1.bucket()).thenReturn("foo");
        when(requestMock1.username()).thenReturn("foo");
        when(requestMock1.password()).thenReturn("");
        when(requestMock1.observable()).thenReturn(obs1);

        Subject<CouchbaseResponse,CouchbaseResponse> obs2 = AsyncSubject.create();
        GetDesignDocumentsRequest requestMock2 = mock(GetDesignDocumentsRequest.class);
        when(requestMock1.path()).thenReturn("");
        when(requestMock2.bucket()).thenReturn("foo");
        when(requestMock2.username()).thenReturn("foo");
        when(requestMock2.password()).thenReturn("");
        when(requestMock2.observable()).thenReturn(obs2);


        TestSubscriber<CouchbaseResponse> t1 = TestSubscriber.create();
        TestSubscriber<CouchbaseResponse> t2 = TestSubscriber.create();

        obs1.subscribe(t1);
        obs2.subscribe(t2);

        channel.writeOutbound(requestMock1, requestMock2);

        t1.assertNotCompleted();
        t2.assertError(RequestCancelledException.class);
    }

}
