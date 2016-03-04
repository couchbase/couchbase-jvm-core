/*
 * Copyright (c) 2015 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint.dcp;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.dcp.DCPMessage;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.utils.UnicastAutoReleaseSubject;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Sergey Avseyev
 */
public class DCPConnection {
    private static final int MINIMUM_HEADER_SIZE = 24;

    /**
     * Counter for stream identifiers.
     */
    private static volatile int nextStreamId = 0;
    private final String name;
    private final SerializedSubject<DCPRequest, DCPRequest> subject;
    private final String bucket;
    private final CoreEnvironment env;
    private volatile int totalReceivedBytes;
    private List<Integer> streams = Collections.synchronizedList(new ArrayList<Integer>());
    private ChannelHandlerContext lastCtx;

    public DCPConnection(final CoreEnvironment env, final String name, final String bucket) {
        this.name = name;
        this.totalReceivedBytes = 0;
        this.env = env;
        this.bucket = bucket;
        subject = UnicastAutoReleaseSubject.<DCPRequest>create(env.autoreleaseAfter(), TimeUnit.MILLISECONDS, env.scheduler())
                .toSerialized();
    }

    public int addStream(final String connectionName) {
        int streamId = nextStreamId++;
        streams.add(streamId);
        return streamId;
    }

    public void removeStream(final int streamId) {
        streams.remove((Integer) streamId);
    }

    public int streamsCount() {
        return streams.size();
    }

    public String name() {
        return name;
    }

    public String bucket() {
        return bucket;
    }

    public Subject<DCPRequest, DCPRequest> subject() {
        return subject;
    }

    public void consumed(final DCPMessage event) {
        consumed(event.totalBodyLength());
    }

    /*package*/ void consumed(final FullBinaryMemcacheResponse response) {
        consumed(response.getTotalBodyLength());
    }

    private void consumed(int delta) {
        if (env.dcpConnectionBufferSize() > 0 && lastCtx != null) {
            totalReceivedBytes += MINIMUM_HEADER_SIZE + delta;
            if (totalReceivedBytes >= env.dcpConnectionBufferSize() * env.dcpConnectionBufferAckThreshold()) {
                lastCtx.writeAndFlush(createBufferAcknowledgmentRequest(totalReceivedBytes));
                totalReceivedBytes = 0;
            }
        }
    }

    /**
     * FIXME: At the moment, we cannot use send() to schedule BufferAcknowledgmentRequest,
     *        because requests and responses will interleave in DCPHandler. Instead the handler
     *        store context object here, and DCPConnection can send acknowledgment as soon as
     *        consumer signals about processed events.
     */
    /*package*/ void setLastContext(ChannelHandlerContext ctx) {
        lastCtx = ctx;
    }

    private BinaryMemcacheRequest createBufferAcknowledgmentRequest(int bufferBytes) {
        ByteBuf extras = lastCtx.alloc().buffer(4).writeInt(bufferBytes);
        BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest("", extras);
        request.setOpcode(DCPHandler.OP_BUFFER_ACK);
        request.setExtrasLength((byte) extras.readableBytes());
        request.setTotalBodyLength(extras.readableBytes());
        return request;
    }
}
