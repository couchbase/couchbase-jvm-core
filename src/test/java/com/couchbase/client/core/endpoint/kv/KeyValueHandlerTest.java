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
package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.AppendRequest;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.CounterRequest;
import com.couchbase.client.core.message.kv.CounterResponse;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.ObserveRequest;
import com.couchbase.client.core.message.kv.ObserveResponse;
import com.couchbase.client.core.message.kv.PrependRequest;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.ReplaceRequest;
import com.couchbase.client.core.message.kv.ReplicaGetRequest;
import com.couchbase.client.core.message.kv.TouchRequest;
import com.couchbase.client.core.message.kv.UnlockRequest;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.util.CollectingResponseEventSink;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Before;
import org.junit.Test;
import rx.subjects.AsyncSubject;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link KeyValueHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class KeyValueHandlerTest {

    /**
     * The name of the bucket.
     */
    private static final String BUCKET = "bucket";

    /**
     * The default charset used for all interactions.
     */
    private static final Charset CHARSET = CharsetUtil.UTF_8;

    /**
     * The channel in which the handler is tested.
     */
    private EmbeddedChannel channel;

    /**
     * The queue in which requests are pushed to only test decodes in isolation manually.
     */
    private Queue<BinaryRequest> requestQueue;

    /**
     * Represents a custom event sink that collects all events pushed into it.
     */
    private CollectingResponseEventSink eventSink;

    @Before
    public void setup() {
        eventSink = new CollectingResponseEventSink();
        requestQueue = new ArrayDeque<BinaryRequest>();
        channel = new EmbeddedChannel(new KeyValueHandler(mock(AbstractEndpoint.class), eventSink, requestQueue));
    }

    @Test
    public void shouldEncodeGet() {
        String id = "key";
        GetRequest request = new GetRequest(id, BUCKET);
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals(id, outbound.getKey());
        assertEquals(id.length(), outbound.getKeyLength());
        assertEquals(id.length(), outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
    }

    @Test
    public void shouldDecodeSuccessfulGet() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setCAS(123456789L);
        response.setExtras(Unpooled.buffer().writeInt(123));
        response.setExtrasLength((byte) 4);

        GetRequest requestMock = mock(GetRequest.class);
        when(requestMock.bucket()).thenReturn(BUCKET);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        assertEquals(1, eventSink.responseEvents().size());
        GetResponse event = (GetResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(123456789L, event.cas());
        assertEquals(123, event.flags());
        assertEquals("content", event.content().toString(CHARSET));
        assertEquals(BUCKET, event.bucket());
    }

    @Test
    public void shouldDecodeNotFoundGet() {
        ByteBuf content = Unpooled.copiedBuffer("Not Found", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setStatus(BinaryMemcacheResponseStatus.KEY_ENOENT);

        GetRequest requestMock = mock(GetRequest.class);
        when(requestMock.bucket()).thenReturn(BUCKET);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        assertEquals(1, eventSink.responseEvents().size());
        GetResponse event = (GetResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(0, event.cas());
        assertEquals(0, event.flags());
        assertEquals("Not Found", event.content().toString(CHARSET));
        assertEquals(BUCKET, event.bucket());
    }

    @Test
    public void shouldEncodeReplicaGetRequest() {
        String id = "replicakey";
        ReplicaGetRequest request = new ReplicaGetRequest(id, BUCKET, (short) 1);
        request.partition((short) 512);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals(id, outbound.getKey());
        assertEquals(id.length(), outbound.getKeyLength());
        assertEquals(id.length(), outbound.getTotalBodyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET_REPLICA, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
    }

    @Test
    public void shouldDecodeReplicaGetResponse() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setCAS(123456789L);
        response.setExtras(Unpooled.buffer().writeInt(123));
        response.setExtrasLength((byte) 4);

        ReplicaGetRequest requestMock = mock(ReplicaGetRequest.class);
        when(requestMock.bucket()).thenReturn(BUCKET);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        assertEquals(1, eventSink.responseEvents().size());
        GetResponse event = (GetResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(123456789L, event.cas());
        assertEquals(123, event.flags());
        assertEquals("content", event.content().toString(CHARSET));
        assertEquals(BUCKET, event.bucket());
    }


    @Test
    public void shouldEncodeGetAndTouch() {
        GetRequest request = new GetRequest("key", "bucket", false, true, 10);
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 4, outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET_AND_TOUCH, outbound.getOpcode());
        assertEquals(4, outbound.getExtrasLength());
        assertEquals(10, outbound.getExtras().readInt());
    }

    @Test
    public void shouldEncodeGetAndLock() {
        GetRequest request = new GetRequest("key", "bucket", true, false, 5);
        request.partition((short) 1024);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 4, outbound.getTotalBodyLength());
        assertEquals(1024, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET_AND_LOCK, outbound.getOpcode());
        assertEquals(4, outbound.getExtrasLength());
        assertEquals(5, outbound.getExtras().readInt());
    }

    @Test
    public void shouldEncodeGetBucketConfigRequest() {
        GetBucketConfigRequest request = new GetBucketConfigRequest("bucket", mock(InetAddress.class));

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals(0, outbound.getReserved());
        assertEquals(0, outbound.getKeyLength());
        assertEquals(0, outbound.getTotalBodyLength());
        assertEquals(0, outbound.getExtrasLength());
        assertEquals(KeyValueHandler.OP_GET_BUCKET_CONFIG, outbound.getOpcode());
    }

    @Test
    public void shouldEncodeInsertRequest() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);

        InsertRequest request = new InsertRequest("key", content.copy(), "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_INSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals(0, outbound.getCAS());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        request = new InsertRequest("key", content.copy(), 10, 0, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_INSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(10, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        request = new InsertRequest("key", content.copy(), 0, 5, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_INSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(5, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        request = new InsertRequest("key", content.copy(), 30, 99, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_INSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(99, outbound.getExtras().readInt());
        assertEquals(30, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldEncodeUpsertRequest() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);

        UpsertRequest request = new UpsertRequest("key", content.copy(), "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UPSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals(0, outbound.getCAS());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        request = new UpsertRequest("key", content.copy(), 10, 0, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UPSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(10, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        request = new UpsertRequest("key", content.copy(), 0, 5, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UPSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(5, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        request = new UpsertRequest("key", content.copy(), 30, 99, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UPSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(99, outbound.getExtras().readInt());
        assertEquals(30, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldEncodeReplaceRequest() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);

        ReplaceRequest request = new ReplaceRequest("key", content.copy(), "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REPLACE, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals(0, outbound.getCAS());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        request = new ReplaceRequest("key", content.copy(), 0, 10, 0, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REPLACE, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(10, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        request = new ReplaceRequest("key", content.copy(), 0, 0, 5, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REPLACE, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(5, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        request = new ReplaceRequest("key", content.copy(), 0, 30, 99, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REPLACE, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(99, outbound.getExtras().readInt());
        assertEquals(30, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));

        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldEncodeReplaceWithCASRequest() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        ReplaceRequest request = new ReplaceRequest("key", content.copy(), 4234234234L, 30, 99, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertEquals(4234234234L, outbound.getCAS());
        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldEncodeRemoveRequest() {
        RemoveRequest request = new RemoveRequest("key", 234234234L, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length(), outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REMOVE, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
        assertEquals(234234234L, outbound.getCAS());
    }

    @Test
    public void shouldEncodePositiveCounterRequest() {
        CounterRequest request = new CounterRequest("key", 5, 10, 15, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 20, outbound.getTotalBodyLength());
        assertEquals(20, outbound.getExtrasLength());
        assertEquals(10, outbound.getExtras().readLong());
        assertEquals(5, outbound.getExtras().readLong());
        assertEquals(15, outbound.getExtras().readInt());
        assertEquals(KeyValueHandler.OP_COUNTER_INCR, outbound.getOpcode());
    }

    @Test
    public void shouldEncodeNegativeCounterRequest() {
        CounterRequest request = new CounterRequest("key", 5, -10, 15, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 20, outbound.getTotalBodyLength());
        assertEquals(20, outbound.getExtrasLength());
        assertEquals(10, outbound.getExtras().readLong());
        assertEquals(5, outbound.getExtras().readLong());
        assertEquals(15, outbound.getExtras().readInt());
        assertEquals(KeyValueHandler.OP_COUNTER_DECR, outbound.getOpcode());
    }

    @Test
    public void shouldEncodeTouchRequest() {
        TouchRequest request = new TouchRequest("key", 30, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 4, outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_TOUCH, outbound.getOpcode());
        assertEquals(4, outbound.getExtrasLength());
        assertEquals(30, outbound.getExtras().readInt());
        assertEquals(0, outbound.getCAS());
    }

    @Test
    public void shouldEncodeUnlockRequest() {
        UnlockRequest request = new UnlockRequest("key", 234234234L, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length(), outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UNLOCK, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
        assertEquals(234234234L, outbound.getCAS());
    }

    @Test
    public void shouldEncodeObserveRequest() {
        ObserveRequest request = new ObserveRequest("key", 234234234L, true, (short) 0, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals(0, outbound.getKeyLength());
        assertEquals(0, outbound.getExtrasLength());
        assertEquals(7, outbound.getTotalBodyLength());
        assertEquals(KeyValueHandler.OP_OBSERVE, outbound.getOpcode());
        assertEquals(1, outbound.content().readShort());
        assertEquals("key".length(), outbound.content().readShort());
        assertEquals("key", outbound.content().readBytes(outbound.content().readableBytes()).toString(CharsetUtil.UTF_8));
    }

    @Test
    public void shouldDecodeGetBucketConfigResponse() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content.copy());

        GetBucketConfigRequest requestMock = mock(GetBucketConfigRequest.class);
        when(requestMock.bucket()).thenReturn("bucket");
        when(requestMock.hostname()).thenReturn(InetAddress.getLocalHost());
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        assertEquals(1, eventSink.responseEvents().size());
        GetBucketConfigResponse event = (GetBucketConfigResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(BUCKET, event.bucket());
        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertEquals(InetAddress.getLocalHost(), event.hostname());
        assertEquals("content", event.content().toString(CHARSET));

        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldDecodeObserveResponseDuringRebalance() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("{someconfig...}", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("", Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setStatus(KeyValueHandler.STATUS_NOT_MY_VBUCKET);

        ObserveRequest requestMock = mock(ObserveRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        assertEquals(1, eventSink.responseEvents().size());
        ObserveResponse event = (ObserveResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(ResponseStatus.RETRY, event.status());
        assertEquals(ObserveResponse.ObserveStatus.UNKNOWN, event.observeStatus());
    }

    @Test
    public void shouldDecodeCounterResponseWhenNotSuccessful() throws Exception {
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("", Unpooled.EMPTY_BUFFER,
            Unpooled.EMPTY_BUFFER);
        response.setStatus(BinaryMemcacheResponseStatus.DELTA_BADVAL);

        CounterRequest requestMock = mock(CounterRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        assertEquals(1, eventSink.responseEvents().size());
        CounterResponse event = (CounterResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(ResponseStatus.FAILURE, event.status());
        assertEquals(0, event.value());
    }

    @Test
    public void shouldRetainContentOnStore() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("foo", CharsetUtil.UTF_8);
        UpsertRequest request = new UpsertRequest("key", content, "bucket");
        request.partition((short) 1);

        assertEquals(1, content.refCnt());
        channel.writeOutbound(request);
        assertEquals(2, content.refCnt());
    }

    @Test
    public void shouldRetainContentOnAppend() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("foo", CharsetUtil.UTF_8);
        AppendRequest request = new AppendRequest("key", 0, content, "bucket");
        request.partition((short) 1);

        assertEquals(1, content.refCnt());
        channel.writeOutbound(request);
        assertEquals(2, content.refCnt());
    }

    @Test
    public void shouldRetainContentOnPrepend() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("foo", CharsetUtil.UTF_8);
        PrependRequest request = new PrependRequest("key", 0, content, "bucket");
        request.partition((short) 1);

        assertEquals(1, content.refCnt());
        channel.writeOutbound(request);
        assertEquals(2, content.refCnt());
    }

    @Test
    public void shouldNotRetainContentOnObserve() throws Exception {
        ObserveRequest request = new ObserveRequest("key", 0, false, (short) 1, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        FullBinaryMemcacheRequest written = (FullBinaryMemcacheRequest) channel.readOutbound();

        assertEquals(1, written.content().refCnt());
    }

    @Test
    public void shouldReleaseStoreRequestContentOnSuccess() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content);
        response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);

        UpsertRequest requestMock = mock(UpsertRequest.class);
        ByteBuf requestContent = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        when(requestMock.bucket()).thenReturn("bucket");
        when(requestMock.observable()).thenReturn(AsyncSubject.<CouchbaseResponse>create());
        when(requestMock.content()).thenReturn(requestContent);
        requestQueue.add(requestMock);

        assertEquals(1, content.refCnt());
        assertEquals(1, requestContent.refCnt());
        channel.writeInbound(response);
        assertEquals(1, content.refCnt());
        assertEquals(0, requestContent.refCnt());
    }

    @Test
    public void shouldNotReleaseStoreRequestContentOnRetry() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content);
        response.setStatus(KeyValueHandler.STATUS_NOT_MY_VBUCKET);

        UpsertRequest requestMock = mock(UpsertRequest.class);
        ByteBuf requestContent = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        when(requestMock.bucket()).thenReturn("bucket");
        when(requestMock.observable()).thenReturn(AsyncSubject.<CouchbaseResponse>create());
        when(requestMock.content()).thenReturn(requestContent);
        requestQueue.add(requestMock);

        assertEquals(1, content.refCnt());
        assertEquals(1, requestContent.refCnt());
        channel.writeInbound(response);
        assertEquals(1, content.refCnt());
        assertEquals(1, requestContent.refCnt());
    }

    @Test
    public void shouldReleaseAppendRequestContentOnSuccess() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content);
        response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);

        AppendRequest requestMock = mock(AppendRequest.class);
        ByteBuf requestContent = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        when(requestMock.bucket()).thenReturn("bucket");
        when(requestMock.observable()).thenReturn(AsyncSubject.<CouchbaseResponse>create());
        when(requestMock.content()).thenReturn(requestContent);
        requestQueue.add(requestMock);

        assertEquals(1, content.refCnt());
        assertEquals(1, requestContent.refCnt());
        channel.writeInbound(response);
        assertEquals(1, content.refCnt());
        assertEquals(0, requestContent.refCnt());
    }

    @Test
    public void shouldNotReleaseAppendRequestContentOnRetry() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content);
        response.setStatus(KeyValueHandler.STATUS_NOT_MY_VBUCKET);

        AppendRequest requestMock = mock(AppendRequest.class);
        ByteBuf requestContent = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        when(requestMock.bucket()).thenReturn("bucket");
        when(requestMock.observable()).thenReturn(AsyncSubject.<CouchbaseResponse>create());
        requestQueue.add(requestMock);

        assertEquals(1, content.refCnt());
        assertEquals(1, requestContent.refCnt());
        channel.writeInbound(response);
        assertEquals(1, content.refCnt());
        assertEquals(1, requestContent.refCnt());
    }

    @Test
    public void shouldReleasePrependRequestContentOnSuccess() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content);
        response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);

        PrependRequest requestMock = mock(PrependRequest.class);
        ByteBuf requestContent = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        when(requestMock.bucket()).thenReturn("bucket");
        when(requestMock.observable()).thenReturn(AsyncSubject.<CouchbaseResponse>create());
        when(requestMock.content()).thenReturn(requestContent);
        requestQueue.add(requestMock);

        assertEquals(1, content.refCnt());
        assertEquals(1, requestContent.refCnt());
        channel.writeInbound(response);
        assertEquals(1, content.refCnt());
        assertEquals(0, requestContent.refCnt());
    }

    @Test
    public void shouldNotReleasePrependRequestContentOnRetry() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content);
        response.setStatus(KeyValueHandler.STATUS_NOT_MY_VBUCKET);

        PrependRequest requestMock = mock(PrependRequest.class);
        ByteBuf requestContent = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        when(requestMock.bucket()).thenReturn("bucket");
        when(requestMock.observable()).thenReturn(AsyncSubject.<CouchbaseResponse>create());
        when(requestMock.content()).thenReturn(requestContent);
        requestQueue.add(requestMock);

        assertEquals(1, content.refCnt());
        assertEquals(1, requestContent.refCnt());
        channel.writeInbound(response);
        assertEquals(1, content.refCnt());
        assertEquals(1, requestContent.refCnt());

    }
}