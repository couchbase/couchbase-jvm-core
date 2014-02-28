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

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAppender;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.function.Consumer;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static reactor.event.selector.Selectors.$;

/**
 * The toplevel handler which accepts and completes promises and handles writing tasks.
 *
 * This handler implements common functionality needed by all pipelines and reduces code duplication. It is designed
 * that the actual endpoint codecs only have to implement the protocol-level translation and never deal with connection
 * management and promise completions. It automatically handles flushing/writing and notifies the upper endpoint if
 * the channel has been disconnected (so that a proper reconnect can be established).
 *
 * The encoder/decoder present in this handler have the purpose of completing the right deferred promises with the
 * correct set of data given from the underlying handlers.
 */
public class GenericEndpointHandler extends ChannelHandlerAppender {

    /**
     * A queue which holds all the outgoing request in order.
     */
    private final Queue<Event<CouchbaseRequest>> queue = new ArrayDeque<Event<CouchbaseRequest>>();

    /**
     * Reference to the parent endpoint (to notify certain signals).
     */
    private final AbstractEndpoint endpoint;

    /**
     * The flush interval in microseconds.
     */
    private final long flushInterval;

    /**
     * Create a new {@link GenericEndpointHandler}.
     *
     * @param endpoint the parent endpoint for communication purposes.
     */
    public GenericEndpointHandler(final AbstractEndpoint endpoint, long flushInterval) {
        add(new EventResponseDecoder(), new EventRequestEncoder());
        this.endpoint = endpoint;
        this.flushInterval = flushInterval;
    }

    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        flushMeMaybe(ctx);
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);

        if (flushInterval > 0) {
            ctx.channel().eventLoop().scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    flushMeMaybe(ctx);
                }
            }, 0, flushInterval, TimeUnit.MICROSECONDS);
        }
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
     * Helper method to flush the pipeline if possible.
     *
     * Hey, I just connected you,
     * And this is crazy,
     * But here's my data,
     * So flush me maybe!
     * It's hard to read right,
     * From your channel,
     * But here's my data,
     * So flush me maybe!
     *
     * @param ctx the channel handler context.
     */
    private void flushMeMaybe(final ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        if (channel.isWritable() && channel.isActive()) {
            ctx.flush();
        }
    }

    /**
     * The {@link EventRequestEncoder} stores the {@link CouchbaseRequest} and puts the payload into the pipeline.
     */
    final class EventRequestEncoder extends MessageToMessageEncoder<Event<CouchbaseRequest>> {

        @Override
        protected void encode(final ChannelHandlerContext ctx, final Event<CouchbaseRequest> msg,
            final List<Object> out) throws Exception {
            queue.offer(msg);
            out.add(msg.getData());
            if (flushInterval <= 0) {
                flushMeMaybe(ctx);
            }
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
            Event<CouchbaseRequest> event = queue.poll();
            ((Consumer<CouchbaseResponse>) event.getReplyTo()).accept(in);
        }

    }

}
