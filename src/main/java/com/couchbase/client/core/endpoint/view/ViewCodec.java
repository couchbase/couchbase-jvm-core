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
package com.couchbase.client.core.endpoint.view;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.message.view.ViewRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * Code which handles encoding and decoding of requests/responses against the Couchbase View Engine.
 *
 * It uses a JSON streaming parser approach which does not actually parse the contents but only extracts the pieces
 * from the HTTP chunk that are needed.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ViewCodec extends MessageToMessageCodec<HttpObject, ViewRequest> {

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<Class<?>> queue;

    /**
     * The current request class.
     */
    private Class<?> currentRequest;

    /**
     * The current chunked up buffer.
     */
    private ByteBuf currentChunk;

    /**
     * The current state of the parser.
     */
    private ParsingState currentState = ParsingState.INITIAL;

    /**
     * The number of total rows (if parsed) for this view response.
     */
    private int currentTotalRows;

    /**
     * The current HTTP response code.
     */
    private int currentCode;

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
    ViewCodec(final Queue<Class<?>> queue) {
        this.queue = queue;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ViewRequest msg, List<Object> out) throws Exception {
        HttpRequest request;
        if (msg instanceof ViewQueryRequest) {
            request = handleViewQueryRequest((ViewQueryRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown Message to encode: " + msg);
        }

        ByteBuf raw = Unpooled.copiedBuffer(msg.bucket() + ":" + msg.password(), CharsetUtil.UTF_8);
        ByteBuf encoded = Base64.encode(raw);
        request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CharsetUtil.UTF_8));
        encoded.release();
        raw.release();

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
            handleViewQueryResponse(msg, in);
        } else {
            throw new IllegalStateException("Got a response message for a request that was not sent." + msg);
        }
    }

    /**
     * Handles the view query request.
     *
     * @param msg the actual message.
     * @return a converted request to be sent over the wire.
     */
    private HttpRequest handleViewQueryRequest(final ViewQueryRequest msg) {
        StringBuilder requestBuilder = new StringBuilder();
        requestBuilder.append("/").append(msg.bucket()).append("/_design/");
        requestBuilder.append(msg.development() ? "dev_" + msg.design() : msg.design());
        requestBuilder.append("/_view/").append(msg.view());
        if (msg.query() != null && !msg.query().isEmpty()) {
            requestBuilder.append("?").append(msg.query());
        }
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, requestBuilder.toString());
    }

    /**
     * Handles the streaming view query response.
     *
     * @param msg the incoming message.
     * @param in the output object list.
     */
    private void handleViewQueryResponse(HttpObject msg, List<Object> in) {
        switch (currentState) {
            case INITIAL:
                if (msg instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) msg;
                    currentCode = response.getStatus().code();
                    currentState = currentCode == 200 ? ParsingState.PREAMBLE : ParsingState.ERROR;
                    return;
                } else {
                    throw new IllegalStateException("Only expecting HttpResponse in INITIAL");
                }
            case ERROR:
                if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    if (content.content().readableBytes() > 0) {
                        currentChunk.writeBytes(content.content());
                        content.content().clear();
                    }

                    if (msg instanceof LastHttpContent) {
                        ResponseStatus status = currentCode == 404 ? ResponseStatus.NOT_EXISTS : ResponseStatus.FAILURE;
                        in.add(new ViewQueryResponse(status, currentTotalRows, currentChunk.copy(), null));
                        reset();

                    }
                    return;
                }
            case PREAMBLE:
                if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    if (content.content().readableBytes() > 0) {
                        currentChunk.writeBytes(content.content());
                        content.content().clear();
                    }

                    int rowsStart = currentChunk.bytesBefore((byte) '[');
                    if (rowsStart < 0) {
                        return;
                    }

                    if (currentChunk.bytesBefore((byte) 't') >= 0) {
                        ByteBuf slice = currentChunk.readBytes(rowsStart+1);
                        String[] sliced = slice.toString(CharsetUtil.UTF_8).split(":");
                        String[] parts = sliced[1].split(",");
                        currentTotalRows = Integer.parseInt(parts[0]);
                    } else {
                        currentChunk.readerIndex(currentChunk.readerIndex() + rowsStart+1);
                    }
                } else {
                    throw new IllegalStateException("Only expecting HttpContent in PREAMBLE");
                }
                currentState = ParsingState.ROWS;
            case ROWS:
                if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    if (content.content().readableBytes() > 0) {
                        currentChunk.writeBytes(content.content());
                        content.content().clear();
                    }
                    MarkerProcessor processor = new MarkerProcessor();
                    currentChunk.forEachByte(processor);

                    boolean last = msg instanceof LastHttpContent;
                    ResponseStatus status = last ? ResponseStatus.SUCCESS : ResponseStatus.CHUNKED;
                    ByteBuf returnContent = currentChunk.readBytes(processor.marker());
                    if (processor.marker() > 0 || last) {
                        in.add(new ViewQueryResponse(status, currentTotalRows, returnContent.copy(), null));
                        currentChunk.discardSomeReadBytes();
                    }
                    returnContent.release();

                    if (last) {
                        reset();
                    }
                } else {
                    throw new IllegalStateException("Only expecting HttpContent in ROWS");
                }
        }
    }

    /**
     * Reset the response objects to a fresh state.
     */
    private void reset() {
        currentRequest = null;
        currentChunk.release();
        currentChunk = null;
        currentState = ParsingState.INITIAL;
        currentCode = 0;
    }

    /**
     * All the possible parsing states for a streaming view response.
     */
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
        ROWS,

        /**
         * Parses an error state.
         */
        ERROR
    }

    /**
     * A custom {@link ByteBufProcessor} which finds and counts open and closing JSON object markers.
     */
    private static class MarkerProcessor implements ByteBufProcessor {

        private int marker = 0;
        private int counter = 0;
        private int depth = 0;
        private byte open = '{';
        private byte close = '}';
        private byte stringMarker = '"';
        private boolean inString = false;

        @Override
        public boolean process(byte value) throws Exception {
            counter++;
            if (value == stringMarker) {
                inString = !inString;
            }
            if (!inString && value == open) {
                depth++;
            }
            if (!inString && value == close) {
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
