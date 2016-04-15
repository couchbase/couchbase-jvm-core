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
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.message.query.QueryRequest;
import com.couchbase.client.core.util.Resources;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link QueryHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class QueryHandlerTest {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(QueryHandlerTest.class);

    private static final String FAKE_REQUESTID = "1234test-7802-4fc2-acd6-dfcd1c05a288";
    private static final String FAKE_CLIENTID = "1234567890123456789012345678901234567890123456789012345678901234";
    private static final String FAKE_SIGNATURE = "{\"*\":\"*\"}";

    private ObjectMapper mapper = new ObjectMapper();
    private Queue<QueryRequest> queue;
    private EmbeddedChannel channel;
    private Disruptor<ResponseEvent> responseBuffer;
    private RingBuffer<ResponseEvent> responseRingBuffer;
    private List<CouchbaseMessage> firedEvents;
    private CountDownLatch latch;
    private QueryHandler handler;
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
        when(environment.queryEnabled()).thenReturn(Boolean.TRUE);
        when(environment.maxRequestLifetime()).thenReturn(10000L);
        when(environment.autoreleaseAfter()).thenReturn(2000L);
        endpoint = mock(AbstractEndpoint.class);
        when(endpoint.environment()).thenReturn(environment);
        when(environment.userAgent()).thenReturn("Couchbase Client Mock");

        queue = new ArrayDeque<QueryRequest>();
        handler = new QueryHandler(endpoint, responseRingBuffer, queue, false);
        channel = new EmbeddedChannel(handler);
    }

    @After
    public void clear() throws Exception {
        channel.close().awaitUninterruptibly();
        responseBuffer.shutdown();
    }

    private void assertGenericQueryRequest(GenericQueryRequest request, boolean jsonExpected) {
        channel.writeOutbound(request);
        HttpRequest outbound = (HttpRequest) channel.readOutbound();

        assertEquals(HttpMethod.POST, outbound.getMethod());
        assertEquals(HttpVersion.HTTP_1_1, outbound.getProtocolVersion());
        assertEquals("/query", outbound.getUri());
        assertTrue(outbound.headers().contains(HttpHeaders.Names.AUTHORIZATION));
        assertEquals("Couchbase Client Mock", outbound.headers().get(HttpHeaders.Names.USER_AGENT));
        assertTrue(outbound instanceof FullHttpRequest);
        assertEquals("query", ((FullHttpRequest) outbound).content().toString(CharsetUtil.UTF_8));
        if (jsonExpected) {
            assertEquals("application/json", outbound.headers().get(HttpHeaders.Names.CONTENT_TYPE));
        } else {
            assertNotEquals("application/json", outbound.headers().get(HttpHeaders.Names.CONTENT_TYPE));
        }
        assertTrue(outbound.headers().contains(HttpHeaders.Names.HOST));
    }

    @Test
    public void shouldEncodeSimpleStatementToGenericQueryRequest() {
        GenericQueryRequest request = GenericQueryRequest.simpleStatement("query", "bucket", "password");
        assertGenericQueryRequest(request, false);
    }

    @Test
    public void shouldEncodeJsonQueryToGenericQueryRequest() {
        GenericQueryRequest request = GenericQueryRequest.jsonQuery("query", "bucket", "password");
        assertGenericQueryRequest(request, true);
    }

    private void assertResponse(GenericQueryResponse inbound,
            boolean expectedSuccess, ResponseStatus expectedStatus,
            String expectedRequestId, String expectedClientId,
            String expectedFinalStatus, String expectedSignature,
            Action1<ByteBuf> assertRows,
            Action1<ByteBuf> assertErrors,
            Map<String, Object> metricsToCheck) {
        assertEquals(expectedSuccess, inbound.status().isSuccess());
        assertEquals(expectedStatus, inbound.status());
        assertEquals(expectedRequestId, inbound.requestId());
        assertEquals(expectedClientId, inbound.clientRequestId());

        assertEquals(expectedFinalStatus, inbound.queryStatus().timeout(1, TimeUnit.SECONDS).toBlocking().single());

        inbound.rows().timeout(5, TimeUnit.SECONDS).toBlocking()
               .forEach(assertRows);

        List<ByteBuf> metricList = inbound.info().timeout(1, TimeUnit.SECONDS).toList().toBlocking().single();
        assertEquals(1, metricList.size());
        String metricsJson = metricList.get(0).toString(CharsetUtil.UTF_8);
        metricList.get(0).release();
        try {
            Map metrics = mapper.readValue(metricsJson, Map.class);
            assertEquals(7, metrics.size());

            for (Map.Entry<String, Object> entry : metricsToCheck.entrySet()) {
                assertNotNull(metrics.get(entry.getKey()));
                assertEquals(entry.getKey(), entry.getValue(), metrics.get(entry.getKey()));
            }
        } catch (IOException e) {
            fail(e.toString());
        }

        inbound.errors().timeout(1, TimeUnit.SECONDS).toBlocking()
               .forEach(assertErrors);

        List<ByteBuf> signatureList = inbound.signature().timeout(1, TimeUnit.SECONDS).toList().toBlocking().single();
        if (expectedSignature != null) {
            assertEquals(1, signatureList.size());
            String signatureJson = signatureList.get(0).toString(CharsetUtil.UTF_8);
            assertNotNull(signatureJson);
            assertEquals(expectedSignature, signatureJson.replaceAll("\\s", ""));
            ReferenceCountUtil.releaseLater(signatureList.get(0));
        } else {
            assertEquals(0, signatureList.size());
        }
    }

    private static Map<String, Object> expectedMetricsCounts(int expectedErrors, int expectedResults) {
        Map<String, Object> result = new HashMap<String, Object>(2);
        result.put("errorCount", expectedErrors);
        result.put("resultCount", expectedResults);
        return result;
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

        assertResponse(inbound, false, ResponseStatus.FAILURE, FAKE_REQUESTID, FAKE_CLIENTID, "fatal", null,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf byteBuf) {
                        fail("no result expected");
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        String response = buf.toString(CharsetUtil.UTF_8);
                        try {
                            Map error = mapper.readValue(response, Map.class);
                            assertEquals(5, error.size());
                            assertEquals(new Integer(4100), error.get("code"));
                            assertEquals(Boolean.FALSE, error.get("temp"));
                            assertEquals("Parse Error", error.get("msg"));
                        } catch (IOException e) {
                            fail();
                        }
                        ReferenceCountUtil.releaseLater(buf);
                    }
                },
                expectedMetricsCounts(1, 0)
        );
    }

    @Test
    public void shouldDecodeChunkedErrorResponse() throws Exception {
        String response = Resources.read("parse_error.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk1 = new DefaultHttpContent(Unpooled.copiedBuffer(response.substring(0, 223), CharsetUtil.UTF_8));
        HttpContent responseChunk2 = new DefaultLastHttpContent(Unpooled.copiedBuffer(response.substring(223), CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk1, responseChunk2);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        assertResponse(inbound, false, ResponseStatus.FAILURE, FAKE_REQUESTID, FAKE_CLIENTID, "fatal", null,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf byteBuf) {
                        ReferenceCountUtil.releaseLater(byteBuf);
                        fail("no result expected");
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        String response = buf.toString(CharsetUtil.UTF_8);
                        ReferenceCountUtil.releaseLater(buf);
                        try {
                            Map error = mapper.readValue(response, Map.class);
                            assertEquals(5, error.size());
                            assertEquals(new Integer(4100), error.get("code"));
                            assertEquals(Boolean.FALSE, error.get("temp"));
                            assertEquals("Parse Error", error.get("msg"));
                        } catch (IOException e) {
                            fail();
                        }
                    }
                },
                expectedMetricsCounts(1, 0)
        );
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

        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, FAKE_CLIENTID, "success", FAKE_SIGNATURE,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf byteBuf) {
                        fail("no result expected");
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        fail("no error expected");
                    }
                },
                expectedMetricsCounts(0, 0)
        );
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

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, FAKE_CLIENTID, "success", FAKE_SIGNATURE,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        invokeCounter1.incrementAndGet();
                        String response = buf.toString(CharsetUtil.UTF_8);
                        buf.release();
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
                            fail("no result expected");
                        }
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        String error = buf.toString(CharsetUtil.UTF_8);
                        buf.release();
                        fail("no error expected, got " + error);
                    }
                },
                expectedMetricsCounts(0, 1)
        );
        assertEquals(1, invokeCounter1.get());
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

        final AtomicInteger found = new AtomicInteger(0);
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, FAKE_CLIENTID, "success", FAKE_SIGNATURE,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf row) {
                        found.incrementAndGet();
                        String content = row.toString(CharsetUtil.UTF_8);
                        row.release();
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
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        fail("no error expected");
                    }
                },
                expectedMetricsCounts(0, 5)
        );
        assertEquals(5, found.get());
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

        final AtomicInteger found = new AtomicInteger(0);
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, FAKE_CLIENTID, "success", FAKE_SIGNATURE,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf byteBuf) {
                        found.incrementAndGet();
                        String content = byteBuf.toString(CharsetUtil.UTF_8);
                        byteBuf.release();
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
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        fail("no error expected");
                    }
                },
                expectedMetricsCounts(0, 5)
        );
        assertEquals(5, found.get());
    }

    @Test
    public void shouldDecodeOneRowResponseWithQuotesInClientIdAndResults() throws Exception {
        String expectedClientIdWithQuotes = "ThisIsA\\\"Client\\\"Id";

        String response = Resources.read("with_escaped_quotes.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, expectedClientIdWithQuotes, "success",
                FAKE_SIGNATURE,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        invokeCounter1.incrementAndGet();
                        String response = buf.toString(CharsetUtil.UTF_8);
                        ReferenceCountUtil.releaseLater(buf);
                        try {
                            Map found = mapper.readValue(response, Map.class);
                            assertEquals(12, found.size());
                            assertEquals("San Francisco", found.get("city"));
                            assertEquals("United States", found.get("country"));
                            Map geo = (Map) found.get("geo");
                            assertNotNull(geo);
                            assertEquals(3, geo.size());
                            assertEquals("ROOFTOP", geo.get("accuracy"));
                            //TODO check the quote in the result
                        } catch (IOException e) {
                            assertFalse(true);
                        }
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        fail("no error expected");
                    }
                },
                expectedMetricsCounts(0, 1)
        );
        assertEquals(1, invokeCounter1.get());
    }

    @Test
    public void shouldDecodeOneRowResponseWithShortClientID() throws Exception {
        String response = Resources.read("short_client_id.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, "123456789", "success", FAKE_SIGNATURE,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        invokeCounter1.incrementAndGet();
                        String response = buf.toString(CharsetUtil.UTF_8);
                        ReferenceCountUtil.releaseLater(buf);
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
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        fail("no error expected");
                    }
                },
                expectedMetricsCounts(0, 1)
        );
        assertEquals(1, invokeCounter1.get());
    }

    @Test
    public void shouldDecodeOneRowResponseWithNoClientID() throws Exception {
        String response = Resources.read("no_client_id.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, "", "success", FAKE_SIGNATURE,
                new Action1<ByteBuf>() {
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
                        ReferenceCountUtil.releaseLater(buf);
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        fail("no error expected");
                    }
                },
                expectedMetricsCounts(0, 1)
        );
        assertEquals(1, invokeCounter1.get());
    }

    @Test
    public void shouldDecodeOneRowResponseWithoutPrettyPrint() throws Exception {
        String response = Resources.read("no_pretty.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, FAKE_CLIENTID, "success", FAKE_SIGNATURE,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        invokeCounter1.incrementAndGet();
                        String response = buf.toString(CharsetUtil.UTF_8);
                        buf.release();
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
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        fail("no error expected");
                    }
                },
                expectedMetricsCounts(0, 1)
        );
        assertEquals(1, invokeCounter1.get());
    }

    @Test
    public void shouldGroupErrorsAndWarnings() throws InterruptedException {
        String response = Resources.read("errors_and_warnings.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        Map<String, Object> expectedMetrics = expectedMetricsCounts(1, 0);
        expectedMetrics.put("warningCount", 1);

        final AtomicInteger count = new AtomicInteger(0);
        assertResponse(inbound, false, ResponseStatus.FAILURE, FAKE_REQUESTID, FAKE_CLIENTID, "fatal", null,
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf byteBuf) {
                        fail("no result expected");
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        count.incrementAndGet();
                        String response = buf.toString(CharsetUtil.UTF_8);
                        buf.release();
                        try {
                            Map error = mapper.readValue(response, Map.class);
                            assertEquals(5, error.size());
                            if (count.get() == 1) {
                                assertEquals(new Integer(4100), error.get("code"));
                                assertEquals(Boolean.FALSE, error.get("temp"));
                                assertEquals("Parse Error", error.get("msg"));
                            } else if (count.get() == 2) {
                                assertEquals(3, error.get("sev"));
                                assertEquals(201, error.get("code"));
                                assertEquals(Boolean.TRUE, error.get("temp"));
                                assertEquals("Nothing to do", error.get("msg"));
                                assertEquals("nothingToDo", error.get("name"));
                            }
                        } catch (IOException e) {
                            fail();
                        }
                    }
                },
                expectedMetrics
        );
        assertEquals(2, count.get());
    }

    @Test
    public void shouldFireKeepAlive() throws Exception {
        final AtomicInteger keepAliveEventCounter = new AtomicInteger();
        final AtomicReference<ChannelHandlerContext> ctxRef = new AtomicReference();

        QueryHandler testHandler = new QueryHandler(endpoint, responseRingBuffer, queue, false) {
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
        };
        EmbeddedChannel channel = new EmbeddedChannel(testHandler);

        //test idle event triggers a query keepAlive request and hook is called
        testHandler.userEventTriggered(ctxRef.get(), IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);

        assertEquals(1, keepAliveEventCounter.get());
        assertTrue(queue.peek() instanceof QueryHandler.KeepAliveRequest);
        QueryHandler.KeepAliveRequest keepAliveRequest = (QueryHandler.KeepAliveRequest) queue.peek();

        //test responding to the request with http response is interpreted into a KeepAliveResponse and hook is called
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
        LastHttpContent responseEnd = new DefaultLastHttpContent();
        channel.writeInbound(response, responseEnd);
        QueryHandler.KeepAliveResponse keepAliveResponse = keepAliveRequest.observable()
                .cast(QueryHandler.KeepAliveResponse.class)
                .timeout(1, TimeUnit.SECONDS).toBlocking().single();

        channel.pipeline().remove(testHandler);
        assertEquals(2, keepAliveEventCounter.get());
        assertEquals(ResponseStatus.NOT_EXISTS, keepAliveResponse.status());
        assertEquals(0, responseEnd.refCnt());
    }

    @Test
    public void shouldDecodeNRowResponseSmallyChunked() throws Exception {
        String response = Resources.read("chunked.json", this.getClass());
        String[] chunks = new String[] {
                response.substring(0, 48),
                response.substring(48, 84),
                response.substring(84, 144),
                response.substring(144, 258),
                response.substring(258, 438),
                response.substring(438, 564),
                response.substring(564, 702),
                response.substring(702, 740),
                response.substring(740)
        };

        StringBuilder sb = new StringBuilder("Chunks:");
        for (String chunk : chunks) {
            sb.append("\n>").append(chunk);
        }
        LOGGER.info(sb.toString());

        shouldDecodeChunked(chunks);
    }

    @Test
    public void shouldDecodeChunkedResponseSplitAtEveryPosition() throws Throwable {
        String response = Resources.read("chunked.json", this.getClass());
        for (int i = 1; i < response.length() - 1; i++) {
            String chunk1 = response.substring(0, i);
            String chunk2 = response.substring(i);

            try {
                shouldDecodeChunked(chunk1, chunk2);
            } catch (Throwable t) {
                LOGGER.info("Test failed in decoding response with chunk at position " + i);
                throw t;
            }
        }
    }

    private void shouldDecodeChunked(String... chunks) throws Exception {
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        Object[] httpChunks = new Object[chunks.length + 1];
        httpChunks[0] = responseHeader;
        for (int i = 1; i <= chunks.length; i++) {
            String chunk = chunks[i - 1];
            if (i == chunks.length) {
                httpChunks[i] = new DefaultLastHttpContent(Unpooled.copiedBuffer(chunk, CharsetUtil.UTF_8));
            } else {
                httpChunks[i] = new DefaultHttpContent(Unpooled.copiedBuffer(chunk, CharsetUtil.UTF_8));
            }
        }

        Subject<CouchbaseResponse,CouchbaseResponse> obs = AsyncSubject.create();
        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        when(requestMock.observable()).thenReturn(obs);
        queue.add(requestMock);
        channel.writeInbound(httpChunks);
        GenericQueryResponse inbound = (GenericQueryResponse) obs.timeout(1, TimeUnit.SECONDS).toBlocking().last();

        final AtomicInteger found = new AtomicInteger(0);
        final AtomicInteger errors = new AtomicInteger(0);
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, "123456\\\"78901234567890", "success",
                "{\"horseName\":\"json\"}",
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf byteBuf) {
                        found.incrementAndGet();
                        String content = byteBuf.toString(CharsetUtil.UTF_8);
                        byteBuf.release();
                        assertNotNull(content);
                        assertTrue(!content.isEmpty());
                        try {
                            Map decoded = mapper.readValue(content, Map.class);
                            assertTrue(decoded.size() > 0);
                            assertTrue(decoded.containsKey("horseName"));
                        } catch (Exception e) {
                            assertTrue(false);
                        }
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        buf.release();
                        errors.incrementAndGet();
                    }
                },
                expectedMetricsCounts(5678, 1234) //these are the numbers parsed from metrics object, not real count
        );
        assertEquals(5, found.get());
        assertEquals(4, errors.get());
    }

    @Test
    public void shouldDecodeSimpleStringAsSignature() throws Exception {
        String response = Resources.read("signature_simple_string.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, FAKE_CLIENTID, "success", "\"json\"",
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        invokeCounter1.incrementAndGet();
                        String item = buf.toString(CharsetUtil.UTF_8);
                        buf.release();
                        fail("no result expected, got " + item);
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        buf.release();
                        fail("no error expected");
                    }
                },
                //no metrics in this json sample
                expectedMetricsCounts(0, 1)
        );
        assertEquals(0, invokeCounter1.get());
    }

    @Test
    public void shouldDecodeNullAsSignature() throws Exception {
        String response = Resources.read("signature_null.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, FAKE_CLIENTID, "success", "null",
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        invokeCounter1.incrementAndGet();
                        String item = buf.toString(CharsetUtil.UTF_8);
                        buf.release();
                        fail("no result expected, got " + item);
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        buf.release();
                        fail("no error expected");
                    }
                },
                //no metrics in this json sample
                expectedMetricsCounts(0, 1)
        );
        assertEquals(0, invokeCounter1.get());
    }

    @Test
    public void shouldDecodeBooleanAsSignature() throws Exception {
        String response = Resources.read("signature_scalar.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, FAKE_CLIENTID, "success", "true",
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        invokeCounter1.incrementAndGet();
                        String item = buf.toString(CharsetUtil.UTF_8);
                        buf.release();
                        fail("no result expected, got " + item);
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        buf.release();
                        fail("no error expected");
                    }
                },
                //no metrics in this json sample
                expectedMetricsCounts(0, 1)
        );
        assertEquals(0, invokeCounter1.get());
    }

    @Test
    public void shouldDecodeArrayAsSignature() throws Exception {
        String response = Resources.read("signature_array.json", this.getClass());
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        HttpContent responseChunk = new DefaultLastHttpContent(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8));

        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        queue.add(requestMock);
        channel.writeInbound(responseHeader, responseChunk);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GenericQueryResponse inbound = (GenericQueryResponse) firedEvents.get(0);

        final AtomicInteger invokeCounter1 = new AtomicInteger();
        assertResponse(inbound, true, ResponseStatus.SUCCESS, FAKE_REQUESTID, FAKE_CLIENTID, "success",
                "[\"json\",\"array\",[\"sub\",true]]",
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        invokeCounter1.incrementAndGet();
                        String item = buf.toString(CharsetUtil.UTF_8);
                        buf.release();
                        fail("no result expected, got " + item);
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        buf.release();
                        fail("no error expected");
                    }
                },
                //no metrics in this json sample
                expectedMetricsCounts(0, 1)
        );
        assertEquals(0, invokeCounter1.get());
    }

    /**
     * See JVMCBC-239.
     */
    @Test
    public void testEarlyChunkInSignatureDoesntFail() throws Exception {
        String chunk1 = "{\n" +
                "    \"requestID\": \"7cde0ed9-1844-436d-85b2-a7b9cd12361c\",\n" +
                "    \"clientContextID\": \"sdkd-java\",\n" +
                "    \"signature\": {\n" +
                "  ";
        String chunk2 = "      \"*\": \"*\"\n" +
                "    },\n" +
                "    \"results\": [\n" +
                "        {\n" +
                "            \"default\": {\n" +
                "                \"id\": 375,\n" +
                "                \"tag\": \"n1ql\",\n" +
                "                \"type\": \"n1qldoc\"\n" +
                "            }\n" +
                "        }\n" +
                "    ],\n" +
                "    \"status\": \"success\",\n" +
                "    \"metrics\": {\n" +
                " ";
        String chunk3 = "       \"elapsedTime\": \"1m18.410321814s\",\n" +
                "        \"executionTime\": \"1m18.410092882s\",\n" +
                "        \"resultCount\": 1,\n" +
                "        \"resultSize\": 100,\n" +
                "        \"mutationCount\": 0,\n" +
                "        \"errorCount\": 0,\n" +
                "        \"warningCount\": 0\n" +
                "    }\n" +
                "}";

        String[] chunks = new String[] { chunk1, chunk2, chunk3 };
        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        Object[] httpChunks = new Object[chunks.length + 1];
        httpChunks[0] = responseHeader;
        for (int i = 1; i <= chunks.length; i++) {
            String chunk = chunks[i - 1];
            if (i == chunks.length) {
                httpChunks[i] = new DefaultLastHttpContent(Unpooled.copiedBuffer(chunk, CharsetUtil.UTF_8));
            } else {
                httpChunks[i] = new DefaultHttpContent(Unpooled.copiedBuffer(chunk, CharsetUtil.UTF_8));
            }
        }

        Subject<CouchbaseResponse,CouchbaseResponse> obs = AsyncSubject.create();
        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        when(requestMock.observable()).thenReturn(obs);
        queue.add(requestMock);
        channel.writeInbound(httpChunks);
        GenericQueryResponse inbound = (GenericQueryResponse) obs.timeout(1, TimeUnit.SECONDS).toBlocking().last();

        final AtomicInteger found = new AtomicInteger(0);
        final AtomicInteger errors = new AtomicInteger(0);
        assertResponse(inbound, true, ResponseStatus.SUCCESS, "7cde0ed9-1844-436d-85b2-a7b9cd12361c", "sdkd-java", "success",
                "{\"*\":\"*\"}",
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf byteBuf) {
                        found.incrementAndGet();
                        String content = byteBuf.toString(CharsetUtil.UTF_8);
                        byteBuf.release();
                        assertNotNull(content);
                        assertTrue(!content.isEmpty());
                        try {
                            Map decoded = mapper.readValue(content, Map.class);
                            assertTrue(decoded.size() > 0);
                            assertTrue(decoded.containsKey("default"));
                        } catch (Exception e) {
                            assertTrue(false);
                        }
                    }
                },
                new Action1<ByteBuf>() {
                    @Override
                    public void call(ByteBuf buf) {
                        buf.release();
                        errors.incrementAndGet();
                    }
                },
                expectedMetricsCounts(0, 1) //these are the numbers parsed from metrics object, not real count
        );
        assertEquals(1, found.get());
        assertEquals(0, errors.get());
    }

    @Test
    public void testBigChunkedResponseWithEscapedBackslashInRowObject() throws Exception {
        String response = Resources.read("chunkedResponseWithDoubleBackslashes.txt", this.getClass());
        String[] chunks = response.split("(?m)^[0-9a-f]+");

        HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, new HttpResponseStatus(200, "OK"));
        responseHeader.headers().add("Transfer-Encoding", "chunked");
        responseHeader.headers().add("Content-Type", "application/json; version=1.0.0");
        Object[] httpChunks = new Object[chunks.length];
        httpChunks[0] = responseHeader;
        for (int i = 1; i < chunks.length; i++) {
            String chunk = chunks[i];
            if (i == chunks.length - 1) {
                httpChunks[i] = new DefaultLastHttpContent(Unpooled.copiedBuffer(chunk, CharsetUtil.UTF_8));
            } else {
                httpChunks[i] = new DefaultHttpContent(Unpooled.copiedBuffer(chunk, CharsetUtil.UTF_8));
            }
        }

        Subject<CouchbaseResponse,CouchbaseResponse> obs = AsyncSubject.create();
        GenericQueryRequest requestMock = mock(GenericQueryRequest.class);
        when(requestMock.observable()).thenReturn(obs);
        queue.add(requestMock);
        channel.writeInbound(httpChunks);
        GenericQueryResponse inbound = (GenericQueryResponse) obs.timeout(1, TimeUnit.SECONDS).toBlocking().last();

        final AtomicInteger found = new AtomicInteger(0);
        final AtomicInteger errors = new AtomicInteger(0);
        inbound.rows().timeout(5, TimeUnit.SECONDS).toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf byteBuf) {
                int rowNumber = found.incrementAndGet();
                String content = byteBuf.toString(CharsetUtil.UTF_8);
                byteBuf.release();
                assertNotNull(content);
                assertFalse(content.isEmpty());
            }
        });

        inbound.errors().timeout(5, TimeUnit.SECONDS).toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf buf) {
                buf.release();
                errors.incrementAndGet();
            }
        });

        //ignore signature
        ReferenceCountUtil.release(inbound.signature().timeout(5, TimeUnit.SECONDS).toBlocking().single());

        String status = inbound.queryStatus().timeout(5, TimeUnit.SECONDS).toBlocking().single();

        List<ByteBuf> metricList = inbound.info().timeout(1, TimeUnit.SECONDS).toList().toBlocking().single();
        assertEquals(1, metricList.size());
        ByteBuf metricsBuf = metricList.get(0);
        ReferenceCountUtil.releaseLater(metricsBuf);
        Map metrics = mapper.readValue(metricsBuf.toString(CharsetUtil.UTF_8), Map.class);

        assertEquals("success", status);

        assertEquals(5, found.get());
        assertEquals(0, errors.get());

        assertEquals(found.get(), metrics.get("resultCount"));
    }
}
