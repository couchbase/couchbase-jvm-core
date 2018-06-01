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
package com.couchbase.client.core.endpoint.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.ServerFeatures;
import com.couchbase.client.core.endpoint.ServerFeaturesEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
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
import com.couchbase.client.core.utils.NetworkAddress;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;
import rx.Subscriber;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
     * Dummy key name which can be reused.
     */
    private static final byte[] KEY = "key".getBytes(CHARSET);

    private static final CoreEnvironment ENVIRONMENT;

    static {
        ENVIRONMENT = mock(CoreEnvironment.class);
        when(ENVIRONMENT.scheduler()).thenReturn(Schedulers.computation());
        when(ENVIRONMENT.compressionMinRatio()).thenReturn(DefaultCoreEnvironment.MIN_COMPRESSION_RATIO);
        when(ENVIRONMENT.compressionMinSize()).thenReturn(DefaultCoreEnvironment.MIN_COMPRESSION_SIZE);
    }

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

    /**
     * The handler used.
     */
    private KeyValueHandler handler;

    /**
     *  A mock endpoint.
     */
    private AbstractEndpoint endpoint;

    @Before
    public void setup() {
        eventSink = new CollectingResponseEventSink();
        requestQueue = new ArrayDeque<BinaryRequest>();
        endpoint = mock(AbstractEndpoint.class);
        when(endpoint.environment()).thenReturn(ENVIRONMENT);
        when(endpoint.context()).thenReturn(new CoreContext(ENVIRONMENT, null, 1));
        handler = new KeyValueHandler(endpoint, eventSink, requestQueue, false, true);
        channel = new EmbeddedChannel(handler);
    }

    @After
    public void cleanup() {
        channel.close().awaitUninterruptibly();
    }

    @Test
    public void shouldEncodeGet() {
        String id = "key";
        GetRequest request = new GetRequest(id, BUCKET);
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals(id, new String(outbound.getKey(), CHARSET));
        assertEquals(id.length(), outbound.getKeyLength());
        assertEquals(id.length(), outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
        assertEquals(request.opaque(), outbound.getOpaque());
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldDecodeSuccessfulGet() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
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
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
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
        assertEquals(id, new String(outbound.getKey(), CHARSET));
        assertEquals(id.length(), outbound.getKeyLength());
        assertEquals(id.length(), outbound.getTotalBodyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET_REPLICA, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldDecodeReplicaGetResponse() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
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
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 4, outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET_AND_TOUCH, outbound.getOpcode());
        assertEquals(4, outbound.getExtrasLength());
        assertEquals(10, outbound.getExtras().readInt());
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldEncodeGetAndLock() {
        GetRequest request = new GetRequest("key", "bucket", true, false, 5);
        request.partition((short) 1024);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 4, outbound.getTotalBodyLength());
        assertEquals(1024, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_GET_AND_LOCK, outbound.getOpcode());
        assertEquals(4, outbound.getExtrasLength());
        assertEquals(5, outbound.getExtras().readInt());
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldEncodeGetBucketConfigRequest() {
        GetBucketConfigRequest request = new GetBucketConfigRequest("bucket", mock(NetworkAddress.class));

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals(0, outbound.getReserved());
        assertEquals(0, outbound.getKeyLength());
        assertEquals(0, outbound.getTotalBodyLength());
        assertEquals(0, outbound.getExtrasLength());
        assertEquals(KeyValueHandler.OP_GET_BUCKET_CONFIG, outbound.getOpcode());
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldEncodeInsertRequest() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);

        InsertRequest request = new InsertRequest("key", content.copy(), "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_INSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals(0, outbound.getCAS());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

        request = new InsertRequest("key", content.copy(), 10, 0, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_INSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(10, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

        request = new InsertRequest("key", content.copy(), 0, 5, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_INSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(5, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

        request = new InsertRequest("key", content.copy(), 30, 99, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_INSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(99, outbound.getExtras().readInt());
        assertEquals(30, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

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
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UPSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals(0, outbound.getCAS());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

        request = new UpsertRequest("key", content.copy(), 10, 0, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UPSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(10, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

        request = new UpsertRequest("key", content.copy(), 0, 5, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UPSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(5, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

        request = new UpsertRequest("key", content.copy(), 30, 99, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UPSERT, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(99, outbound.getExtras().readInt());
        assertEquals(30, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

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
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REPLACE, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals(0, outbound.getCAS());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

        request = new ReplaceRequest("key", content.copy(), 0, 10, 0, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REPLACE, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(10, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

        request = new ReplaceRequest("key", content.copy(), 0, 0, 5, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REPLACE, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(5, outbound.getExtras().readInt());
        assertEquals(0, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

        request = new ReplaceRequest("key", content.copy(), 0, 30, 99, "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals(512, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REPLACE, outbound.getOpcode());
        assertEquals(8, outbound.getExtrasLength());
        assertEquals(99, outbound.getExtras().readInt());
        assertEquals(30, outbound.getExtras().readInt());
        assertEquals(18, outbound.getTotalBodyLength());
        assertEquals("content", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);

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
        ReferenceCountUtil.releaseLater(outbound);
        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldEncodeRemoveRequest() {
        RemoveRequest request = new RemoveRequest("key", 234234234L, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length(), outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_REMOVE, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
        assertEquals(234234234L, outbound.getCAS());
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldEncodePositiveCounterRequest() {
        CounterRequest request = new CounterRequest("key", 5, 10, 15, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 20, outbound.getTotalBodyLength());
        assertEquals(20, outbound.getExtrasLength());
        assertEquals(10, outbound.getExtras().readLong());
        assertEquals(5, outbound.getExtras().readLong());
        assertEquals(15, outbound.getExtras().readInt());
        assertEquals(KeyValueHandler.OP_COUNTER_INCR, outbound.getOpcode());
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldEncodeNegativeCounterRequest() {
        CounterRequest request = new CounterRequest("key", 5, -10, 15, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 20, outbound.getTotalBodyLength());
        assertEquals(20, outbound.getExtrasLength());
        assertEquals(10, outbound.getExtras().readLong());
        assertEquals(5, outbound.getExtras().readLong());
        assertEquals(15, outbound.getExtras().readInt());
        assertEquals(KeyValueHandler.OP_COUNTER_DECR, outbound.getOpcode());
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldEncodeTouchRequest() {
        TouchRequest request = new TouchRequest("key", 30, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length() + 4, outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_TOUCH, outbound.getOpcode());
        assertEquals(4, outbound.getExtrasLength());
        assertEquals(30, outbound.getExtras().readInt());
        assertEquals(0, outbound.getCAS());
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldEncodeUnlockRequest() {
        UnlockRequest request = new UnlockRequest("key", 234234234L, "bucket");
        request.partition((short) 1);

        channel.writeOutbound(request);
        BinaryMemcacheRequest outbound = (BinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("key", new String(outbound.getKey(), CHARSET));
        assertEquals("key".length(), outbound.getKeyLength());
        assertEquals("key".length(), outbound.getTotalBodyLength());
        assertEquals(1, outbound.getReserved());
        assertEquals(KeyValueHandler.OP_UNLOCK, outbound.getOpcode());
        assertEquals(0, outbound.getExtrasLength());
        assertEquals(234234234L, outbound.getCAS());
        ReferenceCountUtil.releaseLater(outbound);
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
        assertEquals("key", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.releaseLater(outbound);
    }

    @Test
    public void shouldDecodeGetBucketConfigResponse() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
            content.copy());

        GetBucketConfigRequest requestMock = mock(GetBucketConfigRequest.class);
        when(requestMock.bucket()).thenReturn("bucket");
        when(requestMock.hostname()).thenReturn(NetworkAddress.localhost());
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        assertEquals(1, eventSink.responseEvents().size());
        GetBucketConfigResponse event = (GetBucketConfigResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(BUCKET, event.bucket());
        assertEquals(ResponseStatus.SUCCESS, event.status());
        assertEquals(NetworkAddress.localhost(), event.hostname());
        assertEquals("content", event.content().toString(CHARSET));

        ReferenceCountUtil.release(content);
    }

    @Test
    public void shouldDecodeObserveResponseDuringRebalance() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("{someconfig...}", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(new byte[] {}, Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setStatus(KeyValueStatus.ERR_NOT_MY_VBUCKET.code());

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
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(new byte[] {}, Unpooled.EMPTY_BUFFER,
            Unpooled.EMPTY_BUFFER);
        response.setStatus(BinaryMemcacheResponseStatus.DELTA_BADVAL);

        CounterRequest requestMock = mock(CounterRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        assertEquals(1, eventSink.responseEvents().size());
        CounterResponse event = (CounterResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(ResponseStatus.INVALID_ARGUMENTS, event.status());
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
        ReferenceCountUtil.releaseLater(content); //release content first time
        ReferenceCountUtil.releaseLater(channel.readOutbound()); //releases extra once + content second time
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
        ReferenceCountUtil.releaseLater(written);
    }

    @Test
    public void shouldReleaseStoreRequestContentOnSuccess() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
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
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
            content);
        response.setStatus(KeyValueStatus.ERR_NOT_MY_VBUCKET.code());

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
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
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
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
            content);
        response.setStatus(KeyValueStatus.ERR_NOT_MY_VBUCKET.code());

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
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
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
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
            content);
        response.setStatus(KeyValueStatus.ERR_NOT_MY_VBUCKET.code());

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

    @Test(expected = CouchbaseException.class)
    public void shouldFailWhenOpaqueDoesNotMatch() throws Exception {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
                content);
        response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
        response.setOpaque(1);

        PrependRequest requestMock = mock(PrependRequest.class);
        ByteBuf requestContent = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        when(requestMock.bucket()).thenReturn("bucket");
        AsyncSubject<CouchbaseResponse> responseSubject = AsyncSubject.<CouchbaseResponse>create();
        when(requestMock.observable()).thenReturn(responseSubject);
        when(requestMock.content()).thenReturn(requestContent);
        when(requestMock.opaque()).thenReturn(3);
        requestQueue.add(requestMock);

        channel.writeInbound(response);
        assertEquals(0, content.refCnt());
        responseSubject.toBlocking().single();
    }

    @Test
    public void shouldFireKeepAlive() throws Exception {
        final AtomicInteger keepAliveEventCounter = new AtomicInteger();
        final AtomicReference<ChannelHandlerContext> ctxRef = new AtomicReference();

        AbstractEndpoint endpoint = mock(AbstractEndpoint.class);
        when(endpoint.environment()).thenReturn(ENVIRONMENT);
        KeyValueHandler testHandler = new KeyValueHandler(endpoint, eventSink,
                requestQueue, false, true) {

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

        //test idle event triggers a k/v keepAlive request and hook is called
        testHandler.userEventTriggered(ctxRef.get(), IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT);

        assertEquals(1, keepAliveEventCounter.get());
        assertTrue(requestQueue.peek() instanceof KeyValueHandler.KeepAliveRequest);
        KeyValueHandler.KeepAliveRequest keepAliveRequest = (KeyValueHandler.KeepAliveRequest) requestQueue.peek();

        //test responding to the request with memcached response is interpreted into a KeepAliveResponse, hook is called
        DefaultFullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(new byte[] {}, Unpooled.EMPTY_BUFFER);
        response.setOpaque(keepAliveRequest.opaque());
        response.setStatus(KeyValueStatus.ERR_NO_MEM.code());

        channel.writeInbound(response);
        KeyValueHandler.KeepAliveResponse keepAliveResponse = keepAliveRequest.observable()
                .cast(KeyValueHandler.KeepAliveResponse.class)
                .timeout(1, TimeUnit.SECONDS).toBlocking().single();

        assertEquals(2, keepAliveEventCounter.get());
        assertEquals(ResponseStatus.OUT_OF_MEMORY, keepAliveResponse.status());
    }

    @Test
    public void shouldHavePipeliningEnabled() {
        Subject<CouchbaseResponse,CouchbaseResponse> obs1 = AsyncSubject.create();
        GetRequest requestMock1 = mock(GetRequest.class);
        when(requestMock1.keyBytes()).thenReturn("hello".getBytes());
        when(requestMock1.bucket()).thenReturn("foo");
        when(requestMock1.observable()).thenReturn(obs1);

        Subject<CouchbaseResponse,CouchbaseResponse> obs2 = AsyncSubject.create();
        GetRequest requestMock2 = mock(GetRequest.class);
        when(requestMock2.keyBytes()).thenReturn("hello".getBytes());

        when(requestMock2.bucket()).thenReturn("foo");
        when(requestMock2.observable()).thenReturn(obs2);


        TestSubscriber<CouchbaseResponse> t1 = TestSubscriber.create();
        TestSubscriber<CouchbaseResponse> t2 = TestSubscriber.create();

        obs1.subscribe(t1);
        obs2.subscribe(t2);

        channel.writeOutbound(requestMock1, requestMock2);

        t1.assertNotCompleted();
        t2.assertNotCompleted();
    }

    @Test
    public void shouldPropagateErrorOnEncode() {
        String id = "key";
        ByteBuf content = Unpooled.buffer();
        content.release(); // provoke a IllegalReferenceCountException
        UpsertRequest request = new UpsertRequest(id, content, BUCKET);
        request.partition((short) 1);


        TestSubscriber<CouchbaseResponse> ts = TestSubscriber.create();
        request.observable().subscribe(ts);

        try {
            channel.writeOutbound(request);
            fail("Expected exception, none thrown.");
        } catch (EncoderException ex) {
            assertTrue(ex.getCause() instanceof IllegalReferenceCountException);
        }

        List<Throwable> onErrorEvents = ts.getOnErrorEvents();
        assertTrue(onErrorEvents.get(0) instanceof RequestCancelledException);
        assertTrue(onErrorEvents.get(0).getCause() instanceof IllegalReferenceCountException);
    }

    @Test
    public void testUnSubscribedRequest() {
        String id = "key";
        GetRequest request = new GetRequest(id, BUCKET);

        TestSubscriber<CouchbaseResponse> ts = TestSubscriber.create();
        request.subscriber(ts);

        ts.unsubscribe();
        assertTrue(!request.isActive());
    }

    @Test
    public void testRequestSubscription() {
        String id = "key";
        GetRequest request = new GetRequest(id, BUCKET);

        TestSubscriber<CouchbaseResponse> ts = TestSubscriber.create();
        request.subscriber(ts);

        assertTrue(request.isActive());
    }

    /**
     * Since empty content is not bigger compressed, it should not send the flags.
     */
    @Test
    public void shouldNotCompressEmptyContent() {
        channel.pipeline().addFirst(new SnappyFeatureHandler());

        ByteBuf content = Unpooled.copiedBuffer("", CharsetUtil.UTF_8);

        UpsertRequest request = new UpsertRequest("key", content.copy(), "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);

        assertEquals(0, outbound.getDataType());
        assertEquals("", outbound.content().toString(CharsetUtil.UTF_8));
        ReferenceCountUtil.release(outbound);
    }

    @Test
    public void shouldNotCompressWithPoorCompressionRatio() throws Exception {
        String text = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo " +
                "ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient " +
                "montes, nascetur ridiculus mus.";

        channel.pipeline().addFirst(new SnappyFeatureHandler());

        ByteBuf content = Unpooled.copiedBuffer(text, CharsetUtil.UTF_8);

        UpsertRequest request = new UpsertRequest("key", content.copy(), "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);

        assertEquals(0, outbound.getDataType());
        ReferenceCountUtil.release(outbound);
    }

    @Test
    public void shouldCompressSmallContent() throws Exception {
        String text = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit.";
        text += text;

        channel.pipeline().addFirst(new SnappyFeatureHandler());

        ByteBuf content = Unpooled.copiedBuffer(text, CharsetUtil.UTF_8);

        UpsertRequest request = new UpsertRequest("key", content.copy(), "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);

        assertEquals(KeyValueHandler.DATATYPE_SNAPPY, outbound.getDataType());

        byte[] compressed = new byte[outbound.content().readableBytes()];
        outbound.content().getBytes(0, compressed);
        byte[] uncompressed = Snappy.uncompress(compressed);
        assertArrayEquals(text.getBytes(CharsetUtil.UTF_8), uncompressed);
        ReferenceCountUtil.release(outbound);
    }

    @Test
    public void shouldCompressLargeContent() throws Exception {
        String text = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula " +
            "eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, " +
            "nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. " +
            "Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate " +
            "eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum " +
            "felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper " +
            "nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, " +
            "eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. " +
            "Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam " +
            "ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus." +
            " Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet " +
            "adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, " +
            "lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis " +
            "faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. " +
            "Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget" +
            " bibendum sodales, augue velit cursus nunc,";

        // because the compression works in appending chunks which are smaller than
        // the short length, let's make sure we test with a text that is longer!
        while (text.length() < Short.MAX_VALUE) {
            text += text;
        }

        channel.pipeline().addFirst(new SnappyFeatureHandler());

        ByteBuf content = Unpooled.copiedBuffer(text, CharsetUtil.UTF_8);

        UpsertRequest request = new UpsertRequest("key", content.copy(), "bucket");
        request.partition((short) 512);
        channel.writeOutbound(request);
        FullBinaryMemcacheRequest outbound = (FullBinaryMemcacheRequest) channel.readOutbound();
        assertNotNull(outbound);

        assertEquals(KeyValueHandler.DATATYPE_SNAPPY, outbound.getDataType());

        byte[] compressed = new byte[outbound.content().readableBytes()];
        outbound.content().getBytes(0, compressed);
        byte[] uncompressed = Snappy.uncompress(compressed);
        assertArrayEquals(text.getBytes(CharsetUtil.UTF_8), uncompressed);
        ReferenceCountUtil.release(outbound);
    }

    @Test
    public void shouldDecompressEmptyContent() {
        channel.pipeline().addFirst(new SnappyFeatureHandler());

        ByteBuf content = Unpooled.directBuffer().writeBytes(Unpooled.copiedBuffer("", CharsetUtil.UTF_8));
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setDataType(KeyValueHandler.DATATYPE_SNAPPY);

        GetRequest requestMock = mock(GetRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        GetResponse event = (GetResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals("", event.content().toString(CHARSET));
        ReferenceCountUtil.release(event);
    }

    @Test
    public void shouldDecompressSmallContent() throws Exception {
        String text = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo " +
            "ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient " +
            "montes, nascetur ridiculus mus.";

        channel.pipeline().addFirst(new SnappyFeatureHandler());

        ByteBuf content = Unpooled.wrappedBuffer(Snappy.compress(text.getBytes(CharsetUtil.UTF_8)));
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setDataType(KeyValueHandler.DATATYPE_SNAPPY);

        GetRequest requestMock = mock(GetRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        GetResponse event = (GetResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(text, event.content().toString(CHARSET));
        ReferenceCountUtil.release(event);
    }

    @Test
    public void shouldDecompressLargeContent() throws Exception {
        String text = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula " +
            "eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, " +
            "nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. " +
            "Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate " +
            "eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum " +
            "felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper " +
            "nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, " +
            "eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. " +
            "Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam " +
            "ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus." +
            " Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet " +
            "adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, " +
            "lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis " +
            "faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. " +
            "Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget" +
            " bibendum sodales, augue velit cursus nunc,";

        // make sure decompression also works on large chunks....
        while (text.length() < Short.MAX_VALUE) {
            text += text;
        }

        channel.pipeline().addFirst(new SnappyFeatureHandler());

        ByteBuf content = Unpooled.wrappedBuffer(Snappy.compress(text.getBytes(CharsetUtil.UTF_8)));
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setDataType(KeyValueHandler.DATATYPE_SNAPPY);

        GetRequest requestMock = mock(GetRequest.class);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        GetResponse event = (GetResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(text, event.content().toString(CHARSET));
        ReferenceCountUtil.release(event);
    }


    @Test
    public void shouldDecodeEmptyTracingDurationFrame() {
        ByteBuf input = Unpooled.buffer();
        assertEquals(0, KeyValueHandler.parseServerDurationFromFrame(input));
        assertEquals(0, input.readableBytes());
    }

    @Test
    public void shouldDecodeZeroTracingDurationFrame() {
        ByteBuf input = Unpooled.buffer()
            .writeByte(2) // id: 0, len: 2
            .writeShort(0);
        assertEquals(0, KeyValueHandler.parseServerDurationFromFrame(input));
        assertEquals(0, input.readableBytes());
    }

    @Test
    public void shouldDecodeMaxTracingDurationFrame() {
        ByteBuf input = Unpooled.buffer()
            .writeByte(2) // id: 0, len: 2
            .writeShort(65535);
        assertEquals(120125042, KeyValueHandler.parseServerDurationFromFrame(input));
        assertEquals(0, input.readableBytes());
    }

    @Test
    public void shouldDecodeNormalTracingDurationFrame() {
        ByteBuf input = Unpooled.buffer()
            .writeByte(2) // id: 0, len: 2
            .writeShort(1234);
        assertEquals(119635, KeyValueHandler.parseServerDurationFromFrame(input));
        assertEquals(0, input.readableBytes());
    }

    @Test
    public void shouldSkipDifferentFrame() {
        ByteBuf input = Unpooled.buffer()
            .writeByte(0x12) // id: 1, len: 2
            .writeShort(0)
            .writeByte(0x02) // id: 0, len: 2
            .writeShort(1234);
        assertEquals(119635, KeyValueHandler.parseServerDurationFromFrame(input));
        assertEquals(0, input.readableBytes());
    }

    @Test
    public void shouldDecodeResponseGetWithTracingData() {
        ByteBuf content = Unpooled.copiedBuffer("content", CharsetUtil.UTF_8);
        FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(KEY, Unpooled.EMPTY_BUFFER,
            content.copy());
        response.setCAS(123456789L);
        response.setExtras(Unpooled.buffer().writeInt(123));
        response.setExtrasLength((byte) 4);
        response.setFramingExtras(Unpooled.buffer().writeByte(0x02).writeShort(1234));
        response.setFramingExtrasLength((byte) 3);

        GetRequest requestMock = mock(GetRequest.class);
        when(requestMock.bucket()).thenReturn(BUCKET);
        requestQueue.add(requestMock);
        channel.writeInbound(response);

        assertEquals(1, eventSink.responseEvents().size());
        GetResponse event = (GetResponse) eventSink.responseEvents().get(0).getMessage();
        assertEquals(123456789L, event.cas());
        assertEquals(123, event.flags());
        assertEquals("content", event.content().toString(CHARSET));
        assertEquals(119635, event.serverDuration());
        assertEquals(BUCKET, event.bucket());
        event.release();
    }

    /**
     * Make sure that if nothing is in the request queue that a keepalive should be sent.
     */
    @Test
    public void shouldSendKeepaliveIfEmpty() {
        assertTrue(handler.shouldSendKeepAlive());
    }

    /**
     * Make sure that if an operation is in-flight and active, since pipelining IS
     * available, a keepalive request should be sent.
     */
    @Test
    public void shouldSendKeepaliveIfActiveInFlight() {
        Subscriber subscriber = new TestSubscriber();
        GetRequest request = new GetRequest("key", "bucket");
        request.subscriber(subscriber);
        requestQueue.add(request);
        channel.writeOutbound();
        assertTrue(handler.shouldSendKeepAlive());
    }

    /**
     * Make sure that if an operation is in-flight and inactive, since pipelining
     * IS enabled, a keepalive request is sent to proactively check the socket.
     */
    @Test
    public void shouldSendKeepaliveIfInactiveInFlight() {
        Subscriber subscriber = new TestSubscriber();
        GetRequest request = new GetRequest("key", "bucket");
        request.subscriber(subscriber);
        requestQueue.add(request);
        channel.writeOutbound();
        assertTrue(handler.shouldSendKeepAlive());
        subscriber.unsubscribe();
        assertTrue(handler.shouldSendKeepAlive());
    }

    class SnappyFeatureHandler extends SimpleChannelInboundHandler<FullBinaryMemcacheResponse> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullBinaryMemcacheResponse msg) throws Exception {

        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            super.handlerAdded(ctx);
            ctx.fireUserEventTriggered(new ServerFeaturesEvent(Collections.singletonList(ServerFeatures.SNAPPY)));
            ctx.pipeline().remove(this);
        }
    }
}
