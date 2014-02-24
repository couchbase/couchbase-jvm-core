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
