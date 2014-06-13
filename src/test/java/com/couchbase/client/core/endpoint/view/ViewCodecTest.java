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

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.util.Resources;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of the {@link ViewCodec}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ViewCodecTest {

    private Queue<Class<?>> queue;
    private EmbeddedChannel channel;

    @Before
    public void setup() {
        queue = new ArrayDeque<Class<?>>();
        channel = new EmbeddedChannel(new ViewCodec(queue));
    }

    @Test
    public void shouldEncodeViewQueryRequestWithoutParams() {
        ViewQueryRequest request = new ViewQueryRequest("design", "view", false, "bucket", "password");
        channel.writeOutbound(request);

        HttpRequest outbound = (HttpRequest) channel.readOutbound();
        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals("/bucket/_design/design/_view/view", outbound.getUri());
    }

    @Test
    public void shouldEncodeViewQueryRequestWithParams() {
        ViewQueryRequest request = new ViewQueryRequest("design", "view", true, "query", "bucket", "password");
        channel.writeOutbound(request);

        HttpRequest outbound = (HttpRequest) channel.readOutbound();
        assertEquals(HttpMethod.GET, outbound.getMethod());
        assertEquals("/bucket/_design/dev_design/_view/view?query", outbound.getUri());
    }

    @Test
    public void shouldDecodeNonReducedResponseWithoutValue() throws Exception {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("non-reduced-without-value.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpContent content = new DefaultLastHttpContent(Unpooled.copiedBuffer(json, CharsetUtil.UTF_8));
        channel.writeInbound(response, content);

        ViewQueryResponse inbound = (ViewQueryResponse) channel.readInbound();
        String inboundContent = inbound.content().toString(CharsetUtil.UTF_8);
        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals(96,inbound.totalRows());
        assertEquals(308, inboundContent.length());
        assertTrue(json.contains(inboundContent));
        assertTrue(inboundContent.endsWith("}"));
    }

    @Test
    public void shouldDecodeNonReducedResponseWithValue() {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("non-reduced-with-value.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpContent content = new DefaultLastHttpContent(Unpooled.copiedBuffer(json, CharsetUtil.UTF_8));
        channel.writeInbound(response, content);

        ViewQueryResponse inbound = (ViewQueryResponse) channel.readInbound();
        String inboundContent = inbound.content().toString(CharsetUtil.UTF_8);
        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals(96,inbound.totalRows());
        assertEquals(633, inboundContent.length());
        assertTrue(json.contains(inboundContent));
        assertTrue(inboundContent.endsWith("}"));
    }

    @Test
    public void shouldDecodeReducedResponse() {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("reduced.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpContent content = new DefaultLastHttpContent(Unpooled.copiedBuffer(json, CharsetUtil.UTF_8));
        channel.writeInbound(response, content);

        ViewQueryResponse inbound = (ViewQueryResponse) channel.readInbound();
        String inboundContent = inbound.content().toString(CharsetUtil.UTF_8);
        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals(0,inbound.totalRows());
        assertEquals(23, inboundContent.length());
        assertTrue(json.contains(inboundContent));
        assertTrue(inboundContent.endsWith("}"));
    }

    @Test
    public void shouldDecodeEmptyNonReducedResponse() {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("non-reduced-empty.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpContent content = new DefaultLastHttpContent(Unpooled.copiedBuffer(json, CharsetUtil.UTF_8));
        channel.writeInbound(response, content);

        ViewQueryResponse inbound = (ViewQueryResponse) channel.readInbound();
        String inboundContent = inbound.content().toString(CharsetUtil.UTF_8);
        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals(96,inbound.totalRows());
        assertEquals(0, inboundContent.length());
    }

    @Test
    public void shouldDecodeEmptyReducedResponse() {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("reduced-empty.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpContent content = new DefaultLastHttpContent(Unpooled.copiedBuffer(json, CharsetUtil.UTF_8));
        channel.writeInbound(response, content);

        ViewQueryResponse inbound = (ViewQueryResponse) channel.readInbound();
        String inboundContent = inbound.content().toString(CharsetUtil.UTF_8);
        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals(0,inbound.totalRows());
        assertEquals(0, inboundContent.length());
    }

    @Test
    public void shouldDecodeMissingViewResponse() {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("not-found.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
        HttpContent content = new DefaultLastHttpContent(Unpooled.copiedBuffer(json, CharsetUtil.UTF_8));
        channel.writeInbound(response, content);

        ViewQueryResponse inbound = (ViewQueryResponse) channel.readInbound();
        assertEquals(ResponseStatus.NOT_EXISTS, inbound.status());
        assertEquals(0,inbound.totalRows());
        assertTrue(inbound.content().toString(CharsetUtil.UTF_8).contains("not_found"));
    }

    @Test
    public void shouldCopeWithBracesInStrings() {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("with-braces.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpContent content = new DefaultLastHttpContent(Unpooled.copiedBuffer(json, CharsetUtil.UTF_8));
        channel.writeInbound(response, content);

        ViewQueryResponse inbound = (ViewQueryResponse) channel.readInbound();
        String inboundContent = inbound.content().toString(CharsetUtil.UTF_8);
        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals(96,inbound.totalRows());
        assertEquals(1279, inboundContent.length());
        assertTrue(json.contains(inboundContent));
        assertTrue(inboundContent.endsWith("}"));
    }

    @Test
    public void shouldSupportContentChunks() {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("non-reduced-with-value.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpContent content1 = new DefaultHttpContent(Unpooled.copiedBuffer(json.substring(0, 10), CharsetUtil.UTF_8));
        HttpContent content2 = new DefaultHttpContent(Unpooled.copiedBuffer(json.substring(11, 320), CharsetUtil.UTF_8));
        HttpContent content3 = new DefaultLastHttpContent(Unpooled.copiedBuffer(json.substring(321, 662), CharsetUtil.UTF_8));
        channel.writeInbound(response, content1, content2, content3);

        ViewQueryResponse inbound1 = (ViewQueryResponse) channel.readInbound();
        ViewQueryResponse inbound2 = (ViewQueryResponse) channel.readInbound();

        assertEquals(ResponseStatus.CHUNKED, inbound1.status());
        assertEquals(ResponseStatus.SUCCESS, inbound2.status());
        assertEquals(632, inbound1.content().readableBytes() + inbound2.content().readableBytes());
        assertTrue(inbound1.content().toString(CharsetUtil.UTF_8).endsWith("}"));
        assertTrue(inbound2.content().toString(CharsetUtil.UTF_8).endsWith("}"));
    }

    @Test
    public void shouldSupportContentChunksWitBraces() {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("with-braces.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpContent content1 = new DefaultHttpContent(Unpooled.copiedBuffer(json.substring(0, 10), CharsetUtil.UTF_8));
        HttpContent content2 = new DefaultHttpContent(Unpooled.copiedBuffer(json.substring(11, 320), CharsetUtil.UTF_8));
        HttpContent content3 = new DefaultHttpContent(Unpooled.copiedBuffer(json.substring(321, 662), CharsetUtil.UTF_8));
        HttpContent content4 = new DefaultLastHttpContent(Unpooled.copiedBuffer(json.substring(663, 1308), CharsetUtil.UTF_8));
        channel.writeInbound(response, content1, content2, content3, content4);

        ViewQueryResponse inbound1 = (ViewQueryResponse) channel.readInbound();
        ViewQueryResponse inbound2 = (ViewQueryResponse) channel.readInbound();
        ViewQueryResponse inbound3 = (ViewQueryResponse) channel.readInbound();

        assertEquals(ResponseStatus.CHUNKED, inbound1.status());
        assertEquals(ResponseStatus.CHUNKED, inbound2.status());
        assertEquals(ResponseStatus.SUCCESS, inbound3.status());

        String inbound1Content = inbound1.content().toString(CharsetUtil.UTF_8);
        String inbound2Content = inbound2.content().toString(CharsetUtil.UTF_8);
        String inbound3Content = inbound3.content().toString(CharsetUtil.UTF_8);

        assertEquals(1277, inbound1Content.length() + inbound2Content.length() + inbound3Content.length());
        assertTrue(inbound1.content().toString(CharsetUtil.UTF_8).endsWith("}"));
        assertTrue(inbound2.content().toString(CharsetUtil.UTF_8).endsWith("}"));
        assertTrue(inbound3.content().toString(CharsetUtil.UTF_8).endsWith("}"));
    }

    @Test
    public void shouldSupportChunkSplitInString() {
        queue.add(ViewQueryRequest.class);

        String json = Resources.read("with-braces.json", this.getClass());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpContent content1 = new DefaultHttpContent(Unpooled.copiedBuffer(json.substring(0, 65), CharsetUtil.UTF_8));
        HttpContent content2 = new DefaultHttpContent(Unpooled.copiedBuffer(json.substring(66, 391), CharsetUtil.UTF_8));
        HttpContent content3 = new DefaultHttpContent(Unpooled.copiedBuffer(json.substring(392, 522), CharsetUtil.UTF_8));
        HttpContent content4 = new DefaultLastHttpContent(Unpooled.copiedBuffer(json.substring(523, 1308), CharsetUtil.UTF_8));
        channel.writeInbound(response, content1, content2, content3, content4);

        ViewQueryResponse inbound1 = (ViewQueryResponse) channel.readInbound();
        ViewQueryResponse inbound2 = (ViewQueryResponse) channel.readInbound();

        assertEquals(ResponseStatus.CHUNKED, inbound1.status());
        assertEquals(ResponseStatus.SUCCESS, inbound2.status());

        String inbound1Content = inbound1.content().toString(CharsetUtil.UTF_8);
        String inbound2Content = inbound2.content().toString(CharsetUtil.UTF_8);

        assertEquals(1280, inbound1Content.length() + inbound2Content.length());
        assertTrue(inbound1.content().toString(CharsetUtil.UTF_8).endsWith("}"));
        assertTrue(inbound2.content().toString(CharsetUtil.UTF_8).endsWith("}"));
    }

}