package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.cluster.RequestEvent;
import com.couchbase.client.core.cluster.ResponseEvent;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelHandlerAppender;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class GenericEndpointHandler extends ChannelHandlerAppender {

    /**
     * Translates {@link CouchbaseRequest}s into {@link RequestEvent}s.
     */
    private static final EventTranslatorOneArg<ResponseEvent, CouchbaseResponse> RESPONSE_TRANSLATOR =
        new EventTranslatorOneArg<ResponseEvent, CouchbaseResponse>() {
            @Override
            public void translateTo(ResponseEvent event, long sequence, CouchbaseResponse response) {
                event.setResponse(response);
            }
        };

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
            CouchbaseRequest request = queue.poll();
            in.observable(request.observable());
            responseBuffer.publishEvent(RESPONSE_TRANSLATOR, in);
        }

    }
}
