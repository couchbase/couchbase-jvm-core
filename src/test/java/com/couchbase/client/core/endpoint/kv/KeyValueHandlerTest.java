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

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.CounterRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import com.couchbase.client.core.message.kv.GetRequest;
import com.couchbase.client.core.message.kv.GetResponse;
import com.couchbase.client.core.message.kv.InsertRequest;
import com.couchbase.client.core.message.kv.ObserveRequest;
import com.couchbase.client.core.message.kv.RemoveRequest;
import com.couchbase.client.core.message.kv.ReplaceRequest;
import com.couchbase.client.core.message.kv.ReplicaGetRequest;
import com.couchbase.client.core.message.kv.TouchRequest;
import com.couchbase.client.core.message.kv.UnlockRequest;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link KeyValueHandler}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class KeyValueHandlerTest {

    private Queue<BinaryRequest> queue;
    private EmbeddedChannel channel;
    private AbstractEndpoint endpoint;
    private Disruptor<ResponseEvent> responseBuffer;
    private List<CouchbaseMessage> firedEvents;
    private CountDownLatch latch;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        endpoint = mock(AbstractEndpoint.class);
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

        queue = new ArrayDeque<BinaryRequest>();
        channel = new EmbeddedChannel(new KeyValueHandler(endpoint, responseBuffer.start(), queue));
    }

    @After
    public void clear() {
        responseBuffer.shutdown();
    }

    @Test
    public void shouldEncodeGet() {
        GetRequest request = new GetRequest("key", "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length(), outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
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
    public void shouldEncodeReplicaGetRequest() {
        ReplicaGetRequest request = new ReplicaGetRequest("key", "bucket", (short) 1);
        request.partition((short) 512);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", outbound.getKey());
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length(), outbound.getTotalBodyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET_REPLICA, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
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
    public void shouldDecodeGetSuccessResponse() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setCAS(123456789L);
        response.setExtras(Unpooled.buffer().writeInt(123));
        response.setExtrasLength((byte) 4);

        GetRequest requestMock = mock(GetRequest.class);
        when(requestMock.bucket()).thenReturn("bucket");
        queue.add(requestMock);
        channel.writeInbound(response);
        channel.readInbound();

        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GetResponse inbound = (GetResponse) firedEvents.get(0);
        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals("bucket", inbound.bucket());
        assertEquals(123456789L, inbound.cas());
        assertEquals(123, inbound.flags());
        assertEquals("content", inbound.content().toString(CharsetUtil.UTF_8));
        assertTrue(queue.isEmpty());

        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldDecodeGetNotFoundResponse() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("Not Found", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setStatus(BinaryMemcacheResponseStatus.KEY_ENOENT);

        GetRequest requestMock = mock(GetRequest.class);
        when(requestMock.bucket()).thenReturn("bucket");
        queue.add(requestMock);
        channel.writeInbound(response);
        channel.readInbound();

        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GetResponse inbound = (GetResponse) firedEvents.get(0);
        assertEquals(ResponseStatus.NOT_EXISTS, inbound.status());
        assertEquals("bucket", inbound.bucket());
        assertEquals(0, inbound.cas());
        assertEquals(0, inbound.flags());
        assertEquals("Not Found", inbound.content().toString(CharsetUtil.UTF_8));
        assertTrue(queue.isEmpty());

        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldDecodeReplicaGetResponse() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setCAS(123456789L);
        response.setExtras(Unpooled.buffer().writeInt(123));
        response.setExtrasLength((byte) 4);

        ReplicaGetRequest requestMock = mock(ReplicaGetRequest.class);
        when(requestMock.bucket()).thenReturn("bucket");
        queue.add(requestMock);
        channel.writeInbound(response);
        channel.readInbound();

        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GetResponse inbound = (GetResponse) firedEvents.get(0);
        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals("bucket", inbound.bucket());
        assertEquals(123456789L, inbound.cas());
        assertEquals(123, inbound.flags());
        assertEquals("content", inbound.content().toString(CharsetUtil.UTF_8));
        assertTrue(queue.isEmpty());

        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldDecodeGetBucketConfigResponse() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse("key", Unpooled.EMPTY_BUFFER,
            content.copy());

        GetBucketConfigRequest requestMock = mock(GetBucketConfigRequest.class);
        when(requestMock.bucket()).thenReturn("bucket");
        when(requestMock.hostname()).thenReturn(InetAddress.getLocalHost());
        queue.add(requestMock);
        channel.writeInbound(response);
        channel.readInbound();

        latch.await(1, TimeUnit.SECONDS);
        assertEquals(1, firedEvents.size());
        GetBucketConfigResponse inbound = (GetBucketConfigResponse) firedEvents.get(0);
        assertEquals(ResponseStatus.SUCCESS, inbound.status());
        assertEquals("bucket", inbound.bucket());
        assertEquals(InetAddress.getLocalHost(), inbound.hostname());
        assertEquals("content", inbound.content().toString(CharsetUtil.UTF_8));
        assertTrue(queue.isEmpty());

        ReferenceCountUtil.release(content);
    }

}
