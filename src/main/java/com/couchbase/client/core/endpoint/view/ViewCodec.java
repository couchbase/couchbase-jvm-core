package com.couchbase.client.core.endpoint.view;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.message.view.ViewRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class ViewCodec extends MessageToMessageCodec<HttpObject, ViewRequest> {

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<Class<?>> queue;

    private Class<?> currentRequest;
    private ByteBuf currentChunk;
    private ParsingState currentState = ParsingState.INITIAL;
    private int currentTotalRows;

    /**
     * Creates a new {@link ViewCodec} with the default dequeue.
     */
    public ViewCodec() {
        this(new ArrayDeque<Class<?>>());
    }

    /**
     * Creates a new {@link ViewCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public ViewCodec(final Queue<Class<?>> queue) {
        this.queue = queue;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ViewRequest msg, List<Object> out) throws Exception {
        HttpRequest request;
        if (msg instanceof ViewQueryRequest) {
            request = handleViewQueryRequest((ViewQueryRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown Messgae to encode: " + msg);
        }
        out.add(request);
        queue.offer(msg.getClass());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> in) throws Exception {
        if (currentRequest == null) {
            currentRequest = queue.poll();
            currentChunk = ctx.alloc().buffer();
        }

        if (currentRequest.equals(ViewQueryRequest.class)) {
            handleViewQueryResponse(ctx, msg, in);
        } else {
            throw new IllegalStateException("Got a response message for a request that was not sent." + msg);
        }
    }

    private HttpRequest handleViewQueryRequest(final ViewQueryRequest msg) {
        String design = msg.development() ? "dev_" + msg.design() : msg.design();
        String uri = "/" + msg.bucket() + "/_design/" + design + "/_view/" + msg.view();
        if (msg.query() != null && !msg.query().isEmpty()) {
            uri = uri + "?" + msg.query();
        }
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    }

    private void handleViewQueryResponse(ChannelHandlerContext ctx, HttpObject msg, List<Object> in) {
        switch (currentState) {
            case INITIAL:
                if (msg instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) msg;
                    // Todo: error handling or retry based on the http response code.
                    currentState = ParsingState.PREAMBLE;
                    return;
                } else {
                    throw new IllegalStateException("Only expecting HttpResponse in INITIAL");
                }
            case PREAMBLE:
                if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    if (content.content().readableBytes() > 0) {
                        currentChunk.writeBytes(content.content());
                        content.content().clear();
                    }

                    int pos = currentChunk.bytesBefore((byte) ',');
                    if (pos > 0) {
                        String[] rowsInfo = currentChunk.readBytes(pos+1).toString(CharsetUtil.UTF_8).split(":");
                        currentTotalRows = Integer.parseInt(rowsInfo[1].substring(0, rowsInfo[1].length()-1));
                    } else {
                        return;
                    }
                    if (currentChunk.readableBytes() >= 8) {
                        currentChunk.readerIndex(currentChunk.readerIndex() + 8);
                    } else {
                        return;
                    }
                } else {
                    throw new IllegalStateException("Only expecting HttpContent in PREAMBLE");
                }
                currentState = ParsingState.ROWS;
            case ROWS:
                if (msg instanceof HttpContent) {
                    // parse rows until done
                    HttpContent content = (HttpContent) msg;
                    if (content.content().readableBytes() > 0) {
                        currentChunk.writeBytes(content.content());
                        content.content().clear();
                    }
                    MarkerProcessor processor = new MarkerProcessor();
                    currentChunk.forEachByte(new MarkerProcessor());

                    boolean last = msg instanceof LastHttpContent;
                    ResponseStatus status = last ? ResponseStatus.SUCCESS : ResponseStatus.CHUNKED;
                    ByteBuf returnContent = currentChunk.readBytes(processor.marker());
                    if (processor.marker() > 0 || last) {
                        in.add(new ViewQueryResponse(status, currentTotalRows, returnContent.copy()));
                        currentChunk.discardSomeReadBytes();
                    }
                    returnContent.release();

                    if (last) {
                        currentRequest = null;
                        currentChunk.release();
                        currentChunk = null;
                        currentState = ParsingState.INITIAL;
                    }
                } else {
                    throw new IllegalStateException("Only expecting HttpContent in ROWS");
                }
        }
    }

    static enum ParsingState {
        /**
         * Start of the incremental parsing process.
         */
        INITIAL,

        /**
         * Parses non-row data like total rows and others.
         */
        PREAMBLE,

        /**
         * Parses the individual view rows.
         */
        ROWS
    }

    private static class MarkerProcessor implements ByteBufProcessor {
        private int marker = 0;
        private int depth;
        private byte open = '{';
        private byte close = '}';
        private int counter = 0;
        @Override
        public boolean process(byte value) throws Exception {
            counter++;
            if (value == open) {
                depth++;
            }
            if (value == close) {
                depth--;
                if (depth == 0) {
                    marker = counter;
                }
            }
            return true;
        }

        public int marker() {
            return marker;
        }
    }
}
