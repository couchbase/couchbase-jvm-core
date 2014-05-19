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
package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.cluster.ResponseHandler;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelHandlerAppender;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * Generic handler which is responsible for general request/response management of the pipeline.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class GenericEndpointHandler extends ChannelHandlerAppender {

    /**
     * Reference to the parent endpoint (to notify certain signals).
     */
    private final AbstractEndpoint endpoint;

    /**
     * A queue which holds all the outgoing request in order.
     */
    private final Queue<CouchbaseRequest> queue = new ArrayDeque<CouchbaseRequest>();

    /**
     * The {@link ResponseEvent} {@link RingBuffer}.
     */
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * Holds the current request.
     */
    private CouchbaseRequest currentRequest;

    /**
     * Creates a new {@link GenericEndpointHandler}.
     *
     * @param endpoint the endpoint reference.
     * @param responseBuffer the response buffer where to write response into.
     */
    public GenericEndpointHandler(final AbstractEndpoint endpoint, final RingBuffer<ResponseEvent> responseBuffer) {
        add(new EventResponseDecoder(), new EventRequestEncoder());
        this.endpoint = endpoint;
        this.responseBuffer = responseBuffer;
    }

    /**
     * Notify the endpoint if the channel is inactive now.
     *
     * This is important as the upper endpoint needs to coordinate the reconnect process.
     *
     * @param ctx the channel handler context.
     * @throws Exception if something goes wrong while setting the channel inactive.
     */
    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        endpoint.notifyChannelInactive();
        ctx.fireChannelInactive();
    }


    /**
     * The {@link EventRequestEncoder} stores the {@link CouchbaseRequest} and puts the payload into the pipeline.
     */
    final class EventRequestEncoder extends MessageToMessageEncoder<CouchbaseRequest> {

        @Override
        protected void encode(final ChannelHandlerContext ctx, final CouchbaseRequest msg,
            final List<Object> out) throws Exception {
            queue.offer(msg);
            out.add(msg);
        }

    }

    /**
     * The {@link EventResponseDecoder} takes the {@link CouchbaseRequest} off the queue and completes the promise.
     */
    final class EventResponseDecoder extends MessageToMessageDecoder<CouchbaseResponse> {

        @Override
        @SuppressWarnings("unchecked")
        protected void decode(final ChannelHandlerContext ctx, final CouchbaseResponse in, final List<Object> out)
            throws Exception {
            if (currentRequest == null) {
                currentRequest = queue.poll();
            }

            responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, in, currentRequest.observable());
            if (in.status() != ResponseStatus.CHUNKED) {
                currentRequest = null;
            }
        }

    }
}
