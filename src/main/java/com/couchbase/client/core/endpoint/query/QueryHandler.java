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

import static com.couchbase.client.core.endpoint.util.ByteBufJsonHelper.*;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
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
import rx.Scheduler;
import rx.subjects.AsyncSubject;
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

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(QueryHandler.class);

    private static final byte QUERY_STATE_INITIAL = 0;
    private static final byte QUERY_STATE_SIGNATURE = 1;
    private static final byte QUERY_STATE_ROWS = 2;
    private static final byte QUERY_STATE_ERROR = 3;
    private static final byte QUERY_STATE_WARNING = 4;
    private static final byte QUERY_STATE_STATUS = 5;
    private static final byte QUERY_STATE_INFO = 6;
    private static final byte QUERY_STATE_DONE = 7;

    /**
     * This is the number of characters expected to be present to be able to read
     * the beginning of the JSON, including the "requestID" token and its value
     * (currently expected to be 36 chars, but the code is adaptative).
     */
    private static final int MINIMUM_WINDOW_FOR_REQUESTID = 55;

    /**
     * This is a window of characters allowing to detect the clientContextID token
     * (including room for JSON separators, etc...).
     */
    public static final int MINIMUM_WINDOW_FOR_CLIENTID_TOKEN = 27;

    /**
     * Contains the current pending response header if set.
     */
    private HttpResponse responseHeader;

    /**
     * Contains the accumulating buffer for the response content.
     */
    private ByteBuf responseContent;

    /**
     * Represents an observable that sends result chunks.
     */
    private ReplaySubject<ByteBuf> queryRowObservable;

    /**
     * Represents an observable that has the signature of the N1QL results if there are any.
     */
    private AsyncSubject<ByteBuf> querySignatureObservable;

    /**
     * Represents an observable that sends errors and warnings if any during query execution.
     */
    private ReplaySubject<ByteBuf> queryErrorObservable;

    /**
     * Represent an observable that has the final execution status of the query, once all result rows and/or
     * errors/warnings have been sent.
     */
    private AsyncSubject<String> queryStatusObservable;

    /**
     * Represents an observable containing metrics on a terminated query.
     */
    private AsyncSubject<ByteBuf> queryInfoObservable;

    /**
     * Represents the current query parsing state.
     */
    private byte queryParsingState = QUERY_STATE_INITIAL;

    /**
     * In case of chunked processing, allows to detect we are still parsing a section.
     */
    private boolean sectionDone = false;

    /**
     * Creates a new {@link QueryHandler} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public QueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, boolean isTransient) {
        super(endpoint, responseBuffer, isTransient);
    }

    /**
     * Creates a new {@link QueryHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    QueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, Queue<QueryRequest> queue, boolean isTransient) {
        super(endpoint, responseBuffer, queue, isTransient);
    }

    @Override
    protected HttpRequest encodeRequest(final ChannelHandlerContext ctx, final QueryRequest msg) throws Exception {
        FullHttpRequest request;

        if (msg instanceof GenericQueryRequest) {
            GenericQueryRequest queryRequest = (GenericQueryRequest) msg;
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/query");
            request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
            if (queryRequest.isJsonFormat()) {
                request.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
            }
            ByteBuf query = ctx.alloc().buffer(((GenericQueryRequest) msg).query().length());
            query.writeBytes(((GenericQueryRequest) msg).query().getBytes(CHARSET));
            request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, query.readableBytes());
            request.content().writeBytes(query);
            query.release();
        } else if (msg instanceof KeepAliveRequest) {
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/admin/ping");
            request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
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

        if (currentRequest() instanceof KeepAliveRequest) {
            if (msg instanceof LastHttpContent) {
                response = new KeepAliveResponse(statusFromCode(responseHeader.getStatus().code()), currentRequest());
                responseContent.clear();
                responseContent.discardReadBytes();
                finishedDecoding();
            }
        } else if (msg instanceof HttpContent) {
            responseContent.writeBytes(((HttpContent) msg).content());
            boolean lastChunk = msg instanceof LastHttpContent;

            if (currentRequest() instanceof GenericQueryRequest) {
                if (queryRowObservable == null) {
                    //still in initial parsing
                    response = handleGenericQueryResponse(lastChunk);
                    //null response indicates need for more data before continuing parsing
                    if (response != null) {
                        parseQueryResponse(lastChunk);
                    }
                } else {
                    parseQueryResponse(lastChunk);
                }

            }
        }

        return response;
    }

    /**
     * Checks if there's not another section opened before the current one,
     * which starts at openBracketPos
     *
     * @param openBracketPos the position of the current section's opening bracket
     * @return true if transition to next state should be made because there's a new
     * section opening.
     */
    private boolean isEmptySection(int openBracketPos) {
        int nextColon = findNextChar(responseContent, ':');
        return nextColon > -1 && nextColon < openBracketPos;
    }

    /**
     * Base method to handle the response for the generic query request.
     *
     * It waits for the first few bytes on the actual response to determine if an error is raised or if a successful
     * response can be expected. The actual error and/or chunk parsing is deferred to other parts of this handler.
     *
     * @return a {@link CouchbaseResponse} if eligible.
     */
    private CouchbaseResponse handleGenericQueryResponse(boolean lastChunk) {
        String requestId;
        String clientId = "";

        if (responseContent.readableBytes() < MINIMUM_WINDOW_FOR_REQUESTID + MINIMUM_WINDOW_FOR_CLIENTID_TOKEN
                && !lastChunk) {
            return null; //wait for more data
        }

        int startIndex = responseContent.readerIndex();

        if (responseContent.readableBytes() >= MINIMUM_WINDOW_FOR_REQUESTID) {
            responseContent.skipBytes(findNextChar(responseContent, ':'));
            responseContent.skipBytes(findNextChar(responseContent, '"') + 1);
            int endOfId = findNextChar(responseContent, '"');
            ByteBuf slice = responseContent.readSlice(endOfId);
            requestId = slice.toString(CHARSET);
        } else {
            return null;
        }

        if (responseContent.readableBytes() >= MINIMUM_WINDOW_FOR_CLIENTID_TOKEN
                && findNextChar(responseContent, ':') < MINIMUM_WINDOW_FOR_CLIENTID_TOKEN) {
            responseContent.markReaderIndex();
            ByteBuf slice = responseContent.readSlice(findNextChar(responseContent, ':'));
            if (slice.toString(CHARSET).contains("clientContextID")) {
                //find the size of the client id
                responseContent.skipBytes(findNextChar(responseContent, '"') + 1); //opening of clientId
                int clientIdSize = findNextCharNotPrefixedBy(responseContent, '"', '\\');
                if (clientIdSize < 0) {
                    //reset the cursor way back before requestID, there was not enough data to get the whole id
                    responseContent.readerIndex(startIndex);
                    //wait for more data
                    return null;
                }
                //read it
                clientId = responseContent.readSlice(clientIdSize).toString(CHARSET);
                //advance to next token
                responseContent.skipBytes(1);//closing quote
                responseContent.skipBytes(findNextChar(responseContent, '"')); //next token's quote
            } else {
                //reset the cursor, there was no client id
                responseContent.resetReaderIndex();
            }
        }

        boolean success = true;
        if (responseContent.readableBytes() >= 20) {
            ByteBuf peekForErrors = responseContent.slice(responseContent.readerIndex(), 20);
            if (peekForErrors.toString(CHARSET).contains("errors")) {
                success = false;
            }
        } else {
            return null;
        }

        ResponseStatus status = statusFromCode(responseHeader.getStatus().code());
        if (!success) {
            status = ResponseStatus.FAILURE;
        }

        Scheduler scheduler = env().scheduler();
        queryRowObservable = ReplaySubject.create();
        queryErrorObservable = ReplaySubject.create();
        queryStatusObservable = AsyncSubject.create();
        queryInfoObservable = AsyncSubject.create();
        querySignatureObservable = AsyncSubject.create();
        return new GenericQueryResponse(
                queryErrorObservable.onBackpressureBuffer().observeOn(scheduler),
                queryRowObservable.onBackpressureBuffer().observeOn(scheduler),
                querySignatureObservable.onBackpressureBuffer().observeOn(scheduler),
                queryStatusObservable.onBackpressureBuffer().observeOn(scheduler),
                queryInfoObservable.onBackpressureBuffer().observeOn(scheduler),
                currentRequest(),
                status, requestId, clientId
        );
    }

    /**
     * Generic dispatch method to parse the query response chunks.
     *
     * Depending on the state the parser is currently in, several different sub-methods are called
     * which do the actual handling.
     *
     * @param lastChunk if the current emitted content body is the last one.
     */
    private void parseQueryResponse(boolean lastChunk) {
        if (sectionDone || queryParsingState == QUERY_STATE_INITIAL) {
            queryParsingState = transitionToNextToken(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_SIGNATURE) {
            parseQuerySignature(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_ROWS) {
            parseQueryRows(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_ERROR) {
            parseQueryError(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_WARNING) {
            parseQueryError(lastChunk); //warning are treated the same as errors -> sent to errorObservable
        }

        if (queryParsingState == QUERY_STATE_STATUS) {
            parseQueryStatus(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_INFO) {
            parseQueryInfo(lastChunk);
        }

        if (queryParsingState == QUERY_STATE_DONE) {
            cleanupQueryStates();
        }
    }

    /**
     * Peek the next token, returning the QUERY_STATE corresponding to it and placing the readerIndex just after
     * the token's ':'. Must be at the end of the previous token.
     *
     * @param lastChunk true if this is the last chunk
     * @return the next QUERY_STATE
     */
    private byte transitionToNextToken(boolean lastChunk) {
        int endNextToken = findNextChar(responseContent, ':');
        if (endNextToken < 0 && !lastChunk) {
            return queryParsingState;
        }
        byte newState;
        ByteBuf peekSlice = responseContent.readSlice(endNextToken + 1);
        String peek = peekSlice.toString(CHARSET);
        if (peek.contains("\"signature\":")) {
            newState = QUERY_STATE_SIGNATURE;
        } else if (peek.endsWith("\"results\":")) {
            newState = QUERY_STATE_ROWS;
        } else if (peek.endsWith("\"status\":")) {
            newState = QUERY_STATE_STATUS;
        } else if (peek.endsWith("\"errors\":")) {
            newState = QUERY_STATE_ERROR;
        } else if (peek.endsWith("\"warnings\":")) {
            newState = QUERY_STATE_WARNING;
        } else if (peek.endsWith("\"metrics\":")) {
            newState = QUERY_STATE_INFO;
        } else {
            IllegalStateException e = new IllegalStateException("Error parsing query response (in TRANSITION) at " + peek);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(responseContent.toString(CHARSET), e);
            }
            throw e;
        }

        sectionDone = false;
        return newState;
    }

    private void sectionDone() {
        this.sectionDone = true;
        responseContent.discardReadBytes();
    }

    /**
     * Parse the signature section in the N1QL response.
     */
    private void parseQuerySignature(boolean lastChunk) {
        int openPos = findNextChar(responseContent, '{');
        if (!isEmptySection(openPos)) { //checks for empty signature
            int closePos = findSectionClosingPosition(responseContent, '{', '}');
            if (closePos > 0) {
                int length = closePos - openPos - responseContent.readerIndex() + 1;
                responseContent.skipBytes(openPos);
                ByteBuf signature = responseContent.readSlice(length);
                querySignatureObservable.onNext(signature.copy());
            } else {
                //wait for more data
                return;
            }
        }
        //note: the signature section could be absent, so we'll make sure to complete the observable
        // when receiving status since this is in every well-formed response.
        sectionDone();
        queryParsingState = transitionToNextToken(lastChunk);
    }

    /**
     * Parses the query rows from the content stream as long as there is data to be found.
     */
    private void parseQueryRows(boolean lastChunk) {
        while (true) {
            int openBracketPos = findNextChar(responseContent, '{');
            if (isEmptySection(openBracketPos)) {
                sectionDone();
                queryParsingState = transitionToNextToken(lastChunk);
                break;
            }

            int closeBracketPos = findSectionClosingPosition(responseContent, '{', '}');
            if (closeBracketPos == -1) {
                break;
            }

            int length = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            responseContent.skipBytes(openBracketPos);
            ByteBuf resultSlice = responseContent.readSlice(length);
            queryRowObservable.onNext(resultSlice.copy());
            responseContent.discardSomeReadBytes();
        }
    }

    /**
     * Parses the errors and warnings from the content stream as long as there are some to be found.
     */
    private void parseQueryError(boolean lastChunk) {
        while (true) {
            int openBracketPos = findNextChar(responseContent, '{');
            if (isEmptySection(openBracketPos)) {
                sectionDone();
                queryParsingState = transitionToNextToken(lastChunk); //warnings or status
                break;
            }

            int closeBracketPos = findSectionClosingPosition(responseContent, '{', '}');
            if (closeBracketPos == -1) {
                break;
            }

            int length = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
            responseContent.skipBytes(openBracketPos);
            ByteBuf resultSlice = responseContent.readSlice(length);
            queryErrorObservable.onNext(resultSlice.copy());
        }
    }

    /**
     * Last before the end of the stream, we can now parse the final result status
     * (including full execution of the query).
     */
    private void parseQueryStatus(boolean lastChunk) {
        //some sections don't always come up, unlike status. Take this chance to close said sections' observables here.
        querySignatureObservable.onCompleted();
        queryRowObservable.onCompleted();
        queryErrorObservable.onCompleted();

        responseContent.markReaderIndex();
        responseContent.skipBytes(findNextChar(responseContent, '"') + 1);
        int endStatus = findNextChar(responseContent, '"');
        if (endStatus > -1) {
            ByteBuf resultSlice = responseContent.readSlice(endStatus);
            queryStatusObservable.onNext(resultSlice.toString(CHARSET));
            queryStatusObservable.onCompleted();
            sectionDone();
            queryParsingState = transitionToNextToken(lastChunk);
        } else {
            responseContent.resetReaderIndex();
            return; //need more data
        }
    }

    /**
     * At the end of the response stream, parse out the info portion (metrics).
     *
     * For the sake of easiness, since we know it comes at the end, we wait until the full data is together and read
     * the info json objects off in one shot (but they are still emitted separately).
     *
     * @param last if this batch is the last one.
     */
    private void parseQueryInfo(boolean last) {
        int openBracketPos = findNextChar(responseContent, '{');
        int closeBracketPos = findSectionClosingPosition(responseContent, '{', '}');
        if (closeBracketPos == -1) {
            if (last) {
                throw new IllegalStateException("Could not find metrics closing in last chunk");
            } else {
                return; //wait for more data
            }
        }

        int from = responseContent.readerIndex() + openBracketPos;
        int to = closeBracketPos - openBracketPos - responseContent.readerIndex() + 1;
        queryInfoObservable.onNext(responseContent.slice(from, to).copy());
        responseContent.readerIndex(to + openBracketPos);

        queryInfoObservable.onCompleted();
        sectionDone();
        queryParsingState = QUERY_STATE_DONE;
    }

    /**
     * Clean up the query states after all rows have been consumed.
     */
    private void cleanupQueryStates() {
        finishedDecoding();
        queryInfoObservable = null;
        queryRowObservable = null;
        queryErrorObservable = null;
        queryStatusObservable = null;
        querySignatureObservable = null;
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
        if (queryRowObservable != null) {
            queryRowObservable.onCompleted();
        }
        if (queryInfoObservable != null) {
            queryInfoObservable.onCompleted();
        }
        if (queryErrorObservable != null) {
            queryErrorObservable.onCompleted();
        }
        if (queryStatusObservable != null) {
            queryStatusObservable.onCompleted();
        }
        if (querySignatureObservable != null) {
            querySignatureObservable.onCompleted();
        }
        cleanupQueryStates();
        if (responseContent != null && responseContent.refCnt() > 0) {
            responseContent.release();
        }
        super.handlerRemoved(ctx);
    }

    @Override
    protected CouchbaseRequest createKeepAliveRequest() {
        return new KeepAliveRequest();
    }

    protected static class KeepAliveRequest extends AbstractCouchbaseRequest implements QueryRequest {
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
