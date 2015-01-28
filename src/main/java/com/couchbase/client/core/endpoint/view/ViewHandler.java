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

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.endpoint.util.ClosingPositionBufProcessor;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.view.GetDesignDocumentRequest;
import com.couchbase.client.core.message.view.GetDesignDocumentResponse;
import com.couchbase.client.core.message.view.RemoveDesignDocumentRequest;
import com.couchbase.client.core.message.view.RemoveDesignDocumentResponse;
import com.couchbase.client.core.message.view.UpsertDesignDocumentRequest;
import com.couchbase.client.core.message.view.UpsertDesignDocumentResponse;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.message.view.ViewRequest;
import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import rx.Scheduler;
import rx.subjects.ReplaySubject;

import java.util.Queue;

/**
 * The {@link ViewHandler} is responsible for encoding {@link ViewRequest}s into lower level
 * {@link HttpRequest}s as well as decoding {@link HttpObject}s into
 * {@link CouchbaseResponse}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ViewHandler extends AbstractGenericHandler<HttpObject, HttpRequest, ViewRequest> {

    private static final byte QUERY_STATE_INITIAL = 0;
    private static final byte QUERY_STATE_ROWS = 1;
    private static final byte QUERY_STATE_INFO = 2;
    private static final byte QUERY_STATE_ERROR = 3;
    private static final byte QUERY_STATE_DONE = 4;

    /**
     * Contains the current pending response header if set.
     */
    private HttpResponse responseHeader;

    /**
     * Contains the accumulating buffer for the response content.
     */
    private ByteBuf responseContent;

    /**
     * Represents a observable that sends config chunks if instructed.
     */
    private ReplaySubject<ByteBuf> viewRowObservable;

    /**
     * Contains info-level data about the view response.
     */
    private ReplaySubject<ByteBuf> viewInfoObservable;

    /**
     * Represents the current query parsing state.
     */
    private byte viewParsingState = QUERY_STATE_INITIAL;

    /**
     * Creates a new {@link ViewHandler} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public ViewHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, boolean isTransient) {
        super(endpoint, responseBuffer, isTransient);
    }

    /**
     * Creates a new {@link ViewHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    ViewHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, Queue<ViewRequest> queue, boolean isTransient) {
        super(endpoint, responseBuffer, queue, isTransient);
    }

    @Override
    protected HttpRequest encodeRequest(final ChannelHandlerContext ctx, final ViewRequest msg) throws Exception {
        if (msg instanceof KeepAliveRequest) {
            FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, "/",
                    Unpooled.EMPTY_BUFFER);
            request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
            request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, 0);
            return request;
        }

        StringBuilder path = new StringBuilder();

        HttpMethod method = HttpMethod.GET;
        ByteBuf content = null;
        if (msg instanceof ViewQueryRequest) {
            ViewQueryRequest queryMsg = (ViewQueryRequest) msg;
            path.append("/").append(msg.bucket()).append("/_design/");
            path.append(queryMsg.development() ? "dev_" + queryMsg.design() : queryMsg.design());
            if (queryMsg.spatial()) {
                path.append("/_spatial/");
            } else {
                path.append("/_view/");
            }
            path.append(queryMsg.view());
            if (queryMsg.query() != null && !queryMsg.query().isEmpty()) {
                path.append("?").append(queryMsg.query());
            }
        } else if (msg instanceof GetDesignDocumentRequest) {
            GetDesignDocumentRequest queryMsg = (GetDesignDocumentRequest) msg;
            path.append("/").append(msg.bucket()).append("/_design/");
            path.append(queryMsg.development() ? "dev_" + queryMsg.name() : queryMsg.name());
        } else if (msg instanceof UpsertDesignDocumentRequest) {
            method = HttpMethod.PUT;
            UpsertDesignDocumentRequest queryMsg = (UpsertDesignDocumentRequest) msg;
            path.append("/").append(msg.bucket()).append("/_design/");
            path.append(queryMsg.development() ? "dev_" + queryMsg.name() : queryMsg.name());
            content = Unpooled.copiedBuffer(queryMsg.body(), CHARSET);
        } else if (msg instanceof RemoveDesignDocumentRequest) {
            method = HttpMethod.DELETE;
            RemoveDesignDocumentRequest queryMsg = (RemoveDesignDocumentRequest) msg;
            path.append("/").append(msg.bucket()).append("/_design/");
            path.append(queryMsg.development() ? "dev_" + queryMsg.name() : queryMsg.name());
        } else {
            throw new IllegalArgumentException("Unknown incoming ViewRequest type "
                + msg.getClass());
        }

        if (content == null) {
            content =  Unpooled.buffer(0);
        }
        FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path.toString(), content);
        request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());
        request.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
        addAuth(ctx, request, msg.bucket(), msg.password());

        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        ViewRequest request = currentRequest();
        CouchbaseResponse response = null;

        if (msg instanceof HttpResponse) {
            responseHeader = (HttpResponse) msg;

            if (responseContent != null) {
                responseContent.clear();
            } else {
                responseContent = ctx.alloc().buffer();
            }
        }

        if (request instanceof KeepAliveRequest) {
            response = new KeepAliveResponse(statusFromCode(responseHeader.getStatus().code()), request);
            responseContent.clear();
            responseContent.discardReadBytes();
        } else if (msg instanceof HttpContent) {
            responseContent.writeBytes(((HttpContent) msg).content());

            if (currentRequest() instanceof ViewQueryRequest) {
                if (viewRowObservable == null) {
                    response = handleViewQueryResponse();
                }

                parseQueryResponse(msg instanceof LastHttpContent);
            }
        }

        if (msg instanceof LastHttpContent) {
            if (request instanceof GetDesignDocumentRequest) {
                response = handleGetDesignDocumentResponse((GetDesignDocumentRequest) request);
                finishedDecoding();
            } else if (request instanceof UpsertDesignDocumentRequest) {
                response = handleUpsertDesignDocumentResponse((UpsertDesignDocumentRequest) request);
                finishedDecoding();
            } else if (request instanceof RemoveDesignDocumentRequest) {
                response = handleRemoveDesignDocumentResponse((RemoveDesignDocumentRequest) request);
                finishedDecoding();
            } else if (request instanceof KeepAliveRequest) {
                finishedDecoding();
            }
        }

        return response;
    }

    /**
     * Creates a {@link GetDesignDocumentResponse} from its request based on the returned info.
     *
     * @param request the outgoing request.
     * @return the parsed response.
     */
    private CouchbaseResponse handleGetDesignDocumentResponse(final GetDesignDocumentRequest request) {
        ResponseStatus status = statusFromCode(responseHeader.getStatus().code());
        return new GetDesignDocumentResponse(request.name(), request.development(), responseContent.copy(), status,
            request);
    }

    private CouchbaseResponse handleUpsertDesignDocumentResponse(final UpsertDesignDocumentRequest request) {
        ResponseStatus status = statusFromCode(responseHeader.getStatus().code());
        return new UpsertDesignDocumentResponse(status, responseContent.copy(), request);
    }

    private CouchbaseResponse handleRemoveDesignDocumentResponse(final RemoveDesignDocumentRequest request) {
        ResponseStatus status = statusFromCode(responseHeader.getStatus().code());
        return new RemoveDesignDocumentResponse(status, responseContent.copy(), request);
    }

    /**
     * Creates a {@link ViewQueryResponse} from its request based on the returned info.
     *
     * Note that observables are attached to this response which are completed later in the response cycle.
     *
     * @return the initial response.
     */
    private CouchbaseResponse handleViewQueryResponse() {
        int code = responseHeader.getStatus().code();
        String phrase = responseHeader.getStatus().reasonPhrase();
        ResponseStatus status = statusFromCode(responseHeader.getStatus().code());
        Scheduler scheduler = env().scheduler();

        viewRowObservable = ReplaySubject.create();
        viewInfoObservable = ReplaySubject.create();
        return new ViewQueryResponse(
            viewRowObservable.onBackpressureBuffer().observeOn(scheduler),
            viewInfoObservable.onBackpressureBuffer().observeOn(scheduler),
            code,
            phrase,
            status,
            currentRequest()
        );
    }

    /**
     * Main dispatch method for a query parse cycle.
     *
     * @param last if the given content chunk is the last one.
     */
    private void parseQueryResponse(boolean last) {
        if (viewParsingState == QUERY_STATE_INITIAL) {
            parseViewInitial();
        }

        if (viewParsingState == QUERY_STATE_INFO) {
            parseViewInfo();
        }

        if (viewParsingState == QUERY_STATE_ERROR) {
            parseViewError(last);
        }

        if (viewParsingState == QUERY_STATE_ROWS) {
            parseViewRows(last);
        }

        if (viewParsingState == QUERY_STATE_DONE) {
            cleanupViewStates();
        }
    }

    /**
     * Clean up the query states after all rows have been consumed.
     */
    private void cleanupViewStates() {
        finishedDecoding();
        viewInfoObservable = null;
        viewRowObservable = null;
        viewParsingState = QUERY_STATE_INITIAL;
    }

    /**
     * Parse the initial view query state.
     */
    private void parseViewInitial() {
        switch (responseHeader.getStatus().code()) {
            case 200:
                viewParsingState = QUERY_STATE_INFO;
                break;
            default:
                viewRowObservable.onCompleted();
                viewParsingState = QUERY_STATE_ERROR;
        }
    }

    /**
     * The query response is an error, parse it and attache it to the observable.
     *
     * @param last if the given content chunk is the last one.
     */
    private void parseViewError(boolean last) {
        if (!last) {
            return;
        }

        viewInfoObservable.onNext(responseContent.copy());
        viewInfoObservable.onCompleted();
        viewParsingState = QUERY_STATE_DONE;
    }

    /**
     * Parse out the info portion from the header part of the query response.
     *
     * This includes the total rows, but also debug info if attached.
     */
    private void parseViewInfo() {
        int rowsStart = -1;
        for (int i = responseContent.readerIndex(); i < responseContent.writerIndex() - 2; i++) {
            byte curr = responseContent.getByte(i);
            byte f1 = responseContent.getByte(i + 1);
            byte f2 = responseContent.getByte(i + 2);

            if (curr == '"' && f1 == 'r' && f2 == 'o') {
                rowsStart = i;
                break;
            }
        }

        if (rowsStart == -1) {
            return;
        }

        ByteBuf info = responseContent.readBytes(rowsStart - responseContent.readerIndex());
        int closingPointer = info.forEachByteDesc(new ByteBufProcessor() {
            @Override
            public boolean process(byte value) throws Exception {
                return value != ',';
            }
        });

        if (closingPointer > 0) {
            info.setByte(closingPointer, '}');
            viewInfoObservable.onNext(info);
        } else {
            viewInfoObservable.onNext(Unpooled.EMPTY_BUFFER);
        }
        viewInfoObservable.onCompleted();
        viewParsingState = QUERY_STATE_ROWS;
    }

    /**
     * Streaming parse the actual rows from the response and pass to the underlying observable.
     *
     * @param last if the given content chunk is the last one.
     */
    private void parseViewRows(boolean last) {
        while (true) {
            int openBracketPos = responseContent.bytesBefore((byte) '{');
            int closeBracketPos = findSectionClosingPosition(responseContent, '{', '}');
            if (closeBracketPos == -1) {
                break;
            }

            int from = responseContent.readerIndex() + openBracketPos;
            int to = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            viewRowObservable.onNext(responseContent.slice(from, to).copy());
            responseContent.readerIndex(closeBracketPos);
        }

        responseContent.discardReadBytes();
        if (last) {
            viewRowObservable.onCompleted();
            viewParsingState = QUERY_STATE_DONE;
        }
    }

    /**
     * Add basic authentication headers to a {@link HttpRequest}.
     *
     * The given information is Base64 encoded and the authorization header is set appropriately. Since this needs
     * to be done for every request, it is refactored out.
     *
     * @param ctx the handler context.
     * @param request the request where the header should be added.
     * @param user the username for auth.
     * @param password the password for auth.
     */
    private static void addAuth(final ChannelHandlerContext ctx, final HttpRequest request, final String user,
        final String password) {
        final String pw = password == null ? "" : password;

        ByteBuf raw = ctx.alloc().buffer(user.length() + pw.length() + 1);
        raw.writeBytes((user + ":" + pw).getBytes(CHARSET));
        ByteBuf encoded = Base64.encode(raw);
        request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CHARSET));
        encoded.release();
        raw.release();
    }

    /**
     * Converts a HTTP status code in its appropriate {@link ResponseStatus} representation.
     *
     * @param code the http code.
     * @return the parsed status.
     */
    private static ResponseStatus statusFromCode(int code) {
        ResponseStatus status;
        switch (code) {
            case 200:
            case 201:
                status = ResponseStatus.SUCCESS;
                break;
            case 404:
                status = ResponseStatus.NOT_EXISTS;
                break;
            default:
                status = ResponseStatus.FAILURE;
        }
        return status;
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        if (viewRowObservable != null) {
            viewRowObservable.onCompleted();
        }
        if (viewInfoObservable != null) {
            viewInfoObservable.onCompleted();
        }
        cleanupViewStates();
        if (responseContent != null && responseContent.refCnt() > 0) {
            responseContent.release();
        }
        super.handlerRemoved(ctx);
    }

    /**
     * Finds the position of the correct closing character, taking into account the fact that before the correct one,
     * other sub section with same opening and closing characters can be encountered.
     *
     * @param buf the {@link ByteBuf} where to search for the end of a section enclosed in openingChar and closingChar.
     * @param openingChar the section opening char, used to detect a sub-section.
     * @param closingChar the section closing char, used to detect the end of a sub-section / this section.
     * @return
     */
    private static int findSectionClosingPosition(ByteBuf buf, char openingChar, char closingChar) {
        return buf.forEachByte(new ClosingPositionBufProcessor(openingChar, closingChar));
    }

    @Override
    protected CouchbaseRequest createKeepAliveRequest() {
        return new KeepAliveRequest();
    }

    protected static class KeepAliveRequest extends AbstractCouchbaseRequest implements ViewRequest {
        protected KeepAliveRequest() {
            super(null, null);
        }
    }

    protected static class KeepAliveResponse extends AbstractCouchbaseResponse {
        protected KeepAliveResponse(ResponseStatus status, CouchbaseRequest request) {
            super(status, request);
        }
    }
}
