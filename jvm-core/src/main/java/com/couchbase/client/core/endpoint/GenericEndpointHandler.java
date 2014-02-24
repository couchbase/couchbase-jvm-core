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
import io.netty.channel.ChannelHandlerAppender;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import reactor.core.composable.Deferred;
import reactor.event.Event;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class GenericEndpointHandler extends ChannelHandlerAppender {

    private final Queue<Event<CouchbaseRequest>> queue = new ArrayDeque<Event<CouchbaseRequest>>();

    public GenericEndpointHandler() {
        add(new EventResponseDecoder(), new EventRequestEncoder());
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);

        ctx.channel().eventLoop().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                ctx.flush();
            }
        }, 0, 75, TimeUnit.MICROSECONDS);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);

        if (ctx.channel().isWritable() == false && ctx.channel().isActive()) {
            ctx.flush();
        }
    }

    final class EventResponseDecoder extends MessageToMessageDecoder<CouchbaseResponse> {

        @Override
        protected void decode(ChannelHandlerContext ctx, CouchbaseResponse in, List<Object> out) throws Exception {
            Event<CouchbaseRequest> event = queue.poll();
            ((Deferred) event.getReplyTo()).accept(in);
        }

    }

    final class EventRequestEncoder extends MessageToMessageEncoder<Event<CouchbaseRequest>> {

        @Override
        protected void encode(ChannelHandlerContext ctx, Event<CouchbaseRequest> msg, List<Object> out) throws Exception {
            queue.offer(msg);
            out.add(msg.getData());
        }

    }

}
