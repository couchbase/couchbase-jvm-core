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
package com.couchbase.client.core.endpoint.query;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.message.query.QueryRequest;
import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
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
import rx.subjects.ReplaySubject;

import java.util.Queue;

/**
 * The {@link QueryHandler} is responsible for encoding {@link QueryRequest}s into lower level
 * {@link HttpRequest}s as well as decoding {@link HttpObject}s into
 * {@link CouchbaseResponse}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class QueryHandler extends AbstractGenericHandler<HttpObject, HttpRequest, QueryRequest> {

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
    private ReplaySubject<ByteBuf> queryRowObservable;

    /**
     * Represents a observable tha has additional info associated (error/info).
     */
    private ReplaySubject<ByteBuf> queryInfoObservable;

    /**
     * Represents the current query parsing state.
     */
    private byte queryParsingState = QUERY_STATE_INITIAL;

    /**
     * Creates a new {@link QueryHandler} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public QueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer) {
        super(endpoint, responseBuffer);
    }

    /**
     * Creates a new {@link QueryHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    QueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, Queue<QueryRequest> queue) {
        super(endpoint, responseBuffer, queue);
    }

    @Override
    protected HttpRequest encodeRequest(final ChannelHandlerContext ctx, final QueryRequest msg) throws Exception {
        FullHttpRequest request;

        if (msg instanceof GenericQueryRequest) {
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/query");
            request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
            ByteBuf query = ctx.alloc().buffer(((GenericQueryRequest) msg).query().length());
            query.writeBytes(((GenericQueryRequest) msg).query().getBytes(CHARSET));
            request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, query.readableBytes());
            request.content().writeBytes(query);
            query.release();
        } else {
            throw new IllegalArgumentException("Unknown incoming QueryRequest type "
                + msg.getClass());
        }

        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        CouchbaseResponse response = null;

        if (msg instanceof HttpResponse) {
            responseHeader = (HttpResponse) msg;
            if (responseContent != null) {
                responseContent.clear();
            } else {
                responseContent = ctx.alloc().buffer();
            }
        }

        if (msg instanceof HttpContent) {
            responseContent.writeBytes(((HttpContent) msg).content());

            if (currentRequest() instanceof GenericQueryRequest) {
                if (queryRowObservable == null) {
                    response = handleGenericQueryResponse();
                }

                parseQueryResponse(msg instanceof LastHttpContent);
            }
        }

        return response;
    }

    /**
     * Base method to handle the response for the generic query request.
     *
     * It waits for the first few bytes on the actual response to determine if an error is raised or if a successful
     * response can be expected. The actual error and/or chunk parsing is deferred to other parts of this handler.
     *
     * @return a {@link CouchbaseResponse} if eligible.
     */
    private CouchbaseResponse handleGenericQueryResponse() {
        boolean success = true;
        if (responseContent.readableBytes() >= 20) {
            ByteBuf firstPart = responseContent.slice(0, 20);
            if (firstPart.toString(CHARSET).contains("error")) {
                success = false;
            }
        } else {
            return null;
        }

        ResponseStatus status = statusFromCode(responseHeader.getStatus().code());
        if (!success) {
            status = ResponseStatus.FAILURE;
        }
        queryRowObservable = ReplaySubject.create();
        queryInfoObservable = ReplaySubject.create();
        return new GenericQueryResponse(queryRowObservable, queryInfoObservable, status, currentRequest());
    }

    /**
     * Generic dispatch method to parse the query response chunks.
     *
     * Depending on the state the parser is currently in, several different sub-methods are caleld which do the actual
     * handling.
     *
     * @param lastChunk if the current emitted content body is the last one.
     */
    private void parseQueryResponse(boolean lastChunk) {
        if (queryParsingState == QUERY_STATE_INITIAL) {
            parseQueryInitial();
        }

        if (queryParsingState == QUERY_STATE_ROWS) {
            parseQueryRows();
        }

        if (queryParsingState == QUERY_STATE_INFO) {
            parseQueryInfo(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_ERROR) {
            parseQueryError(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_DONE) {
            cleanupQueryStates();
        }
    }

    /**
     * Helper method to parse the initial bits of the query response.
     */
    private void parseQueryInitial() {
        ByteBuf content = responseContent;

        if (content.readableBytes() >= 20) {
            ByteBuf prefixBuf = content.slice(0, 20);
            String prefix = prefixBuf.toString(CHARSET);

            if (prefix.contains("resultset")) {
                queryParsingState = QUERY_STATE_ROWS;
            } else if (prefix.contains("error")) {
                queryRowObservable.onCompleted();
                queryParsingState = QUERY_STATE_ERROR;
            } else {
                throw new IllegalStateException("Error parsing query response (in INITIAL): "
                    + content.toString(CHARSET));
            }
            content.readerIndex(prefixBuf.bytesBefore((byte) ':')+1);
        }
    }

    /**
     * Parses the query rows from the content stream as long as there is data to be found.
     */
    private void parseQueryRows() {
        while(true) {
            int openBracketPos = responseContent.bytesBefore((byte) '{');
            int nextColonPos = responseContent.bytesBefore((byte) ':');
            if (nextColonPos < openBracketPos) {
                queryParsingState = QUERY_STATE_INFO;
                queryRowObservable.onCompleted();
                break;
            }
            int closeBracketPos = -1;
            int openBrackets = 0;
            for (int i = responseContent.readerIndex(); i <= responseContent.writerIndex(); i++) {
                byte current = responseContent.getByte(i);
                if (current == '{') {
                    openBrackets++;
                } else if (current == '}' && openBrackets > 0) {
                    openBrackets--;
                    if (openBrackets == 0) {
                        closeBracketPos = i;
                        break;
                    }
                }
            }

            if (closeBracketPos == -1) {
                break;
            }

            int from = responseContent.readerIndex() + openBracketPos;
            int to = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            queryRowObservable.onNext(responseContent.slice(from, to).copy());
            responseContent.readerIndex(closeBracketPos);
        }

        responseContent.discardReadBytes();
    }

    /**
     * At the end of the row stream, parse out the info portion.
     *
     * For the sake of easiness, since we know it comes at the end, we wait until the full data is together and read
     * the info json objects off in one shot (but they are still emitted separately).
     *
     * @param last if this batch is the last one.
     */
    private void parseQueryInfo(boolean last) {
        if (!last) {
            return;
        }

        int initColon = responseContent.bytesBefore((byte) ':');
        responseContent.readerIndex(initColon);

        while(true) {
            int openBracketPos = responseContent.bytesBefore((byte) '{');
            int closeBracketPos = -1;
            int openBrackets = 0;
            for (int i = responseContent.readerIndex(); i <= responseContent.writerIndex(); i++) {
                byte current = responseContent.getByte(i);
                if (current == '{') {
                    openBrackets++;
                } else if (current == '}' && openBrackets > 0) {
                    openBrackets--;
                    if (openBrackets == 0) {
                        closeBracketPos = i;
                        break;
                    }
                }
            }

            if (closeBracketPos == -1) {
                break;
            }

            int from = responseContent.readerIndex() + openBracketPos;
            int to = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            queryInfoObservable.onNext(responseContent.slice(from, to).copy());
            responseContent.readerIndex(to + openBracketPos);
        }

        queryInfoObservable.onCompleted();
        queryParsingState = QUERY_STATE_DONE;
    }

    /**
     * Parses the error portion of the query response.
     *
     * @param last if this batch is the last one.
     */
    private void parseQueryError(boolean last) {
        if (!last) {
            return;
        }

        short found = 0;
        int foundIndex = 0;
        for (int i = responseContent.writerIndex(); i > responseContent.readerIndex(); i--) {
            if (responseContent.getByte(i) == 125) {
                found++;
            }
            if (found == 2) {
                foundIndex = i;
                break;
            }
        }

        int length = foundIndex - responseContent.readerIndex() + 1;
        queryInfoObservable.onNext(responseContent.copy(responseContent.readerIndex(), length));
        queryInfoObservable.onCompleted();
        queryParsingState = QUERY_STATE_DONE;
    }

    /**
     * Clean up the query states after all rows have been consumed.
     */
    private void cleanupQueryStates() {
        finishedDecoding();
        queryInfoObservable = null;
        queryRowObservable = null;
        queryParsingState = QUERY_STATE_INITIAL;
    }

    /**
     * Converts a HTTP status code in its appropriate {@link ResponseStatus} representation.
     *
     * @param code the http code.
     * @return the parsed status.
     */
    private static ResponseStatus statusFromCode(int code) {
        ResponseStatus status;
        switch(code) {
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
        if (queryRowObservable != null) {
            queryRowObservable.onCompleted();
        }
        if (queryInfoObservable != null) {
            queryInfoObservable.onCompleted();
        }
        cleanupQueryStates();
        super.handlerRemoved(ctx);
    }


}
