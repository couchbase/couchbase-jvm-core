/*
 * Copyright (c) 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.endpoint.query.parser;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.utils.UnicastAutoReleaseSubject;
import com.couchbase.client.core.utils.yasjl.ByteBufJsonParser;
import com.couchbase.client.core.utils.yasjl.Callbacks.JsonPointerCB1;
import com.couchbase.client.core.utils.yasjl.JsonPointer;
import java.io.EOFException;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import rx.Scheduler;
import rx.subjects.AsyncSubject;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * A N1QL/Query response parser, based on yasjl.
 *
 * @author Subhashni Balakrishnan
 * @author Michael Nitschinger
 * @since 1.4.3
 */
public class YasjlQueryResponseParser {

    /**
     * The logger used for this parser.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(YasjlQueryResponseParser.class);

    /**
     * The default charset used when decoding.
     */
    private static final Charset CHARSET = CharsetUtil.UTF_8;

    /**
     * Scheduler for query response
     */
    private final Scheduler scheduler;

    /**
     * TTL for response observables
     */
    private final long ttl;

    /**
     * The actual yasjl parser handling the response.
     */
    private final ByteBufJsonParser parser;

    /**
     * Represents an observable that sends result chunks.
     */
    private UnicastAutoReleaseSubject<ByteBuf> queryRowObservable;

    /**
     * Represents an observable that has the signature of the N1QL results if there are any.
     */
    private UnicastAutoReleaseSubject<ByteBuf> querySignatureObservable;

    /**
     * Represents an observable that sends errors and warnings if any during query execution.
     */
    private UnicastAutoReleaseSubject<ByteBuf> queryErrorObservable;

    /**
     * Represent an observable that has the final execution status of the query, once all result rows and/or
     * errors/warnings have been sent.
     */
    private AsyncSubject<String> queryStatusObservable;

    /**
     * Represents an observable containing metrics on a terminated query.
     */
    private UnicastAutoReleaseSubject<ByteBuf> queryInfoObservable;

    /**
     * Represents an observable containing profile info on a terminated query.
     */
    private UnicastAutoReleaseSubject<ByteBuf> queryProfileInfoObservable;

    /**
     * Represents the current request
     */
    private CouchbaseRequest currentRequest;

    /**
     * Response status
     */
    private ResponseStatus status;

    /**
     * Flag to indicate if the parser is initialized
     */
    private boolean initialized;

    /**
     * Response that should be returned on parse call
     */
    private GenericQueryResponse response;

    /**
     * Holds the current request ID of the response.
     */
    private String requestID;

    /**
     * Holds the current context ID of the response.
     */
    private String clientContextID;

    /**
     * True if the current response has been sent already.
     */
    private boolean sentResponse;

    /**
     * A buffer for the current raw response content.
     */
    private ByteBuf responseContent;

    private final CoreEnvironment env;

    /**
     * Create a new {@link YasjlQueryResponseParser}.
     *
     * @param scheduler the scheduler which should be used when computations are moved out.
     * @param ttl the ttl used for the subjects until their contents are garbage collected.
     */
    public YasjlQueryResponseParser(final Scheduler scheduler, final long ttl, final CoreEnvironment env) {
        this.scheduler = scheduler;
        this.ttl = ttl;
        this.response = null;
        this.env = env;

        JsonPointer[] jsonPointers = {
                new JsonPointer("/requestID", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        requestID = buf.toString(CHARSET);
                        requestID = requestID.substring(1, requestID.length() - 1);
                        buf.release();
                        if (queryRowObservable != null) {
                            queryRowObservable.withTraceIdentifier("queryRow." + requestID);
                        }
                        if (queryErrorObservable != null) {
                            queryErrorObservable.withTraceIdentifier("queryError." + requestID);
                        }
                        if (queryInfoObservable != null) {
                            queryInfoObservable.withTraceIdentifier("queryInfo." + requestID);
                        }
                        if (querySignatureObservable != null) {
                            querySignatureObservable.withTraceIdentifier("querySignature." + requestID);
                        }
                        if (queryProfileInfoObservable != null) {
                            queryProfileInfoObservable.withTraceIdentifier("queryProfileInfo." + requestID);
                        }
                    }
                }),
                new JsonPointer("/clientContextID", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        clientContextID = buf.toString(CHARSET);
                        clientContextID = clientContextID.substring(1, clientContextID.length() - 1);
                        buf.release();
                    }
                }),
                new JsonPointer("/signature", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (querySignatureObservable != null) {
                            querySignatureObservable.onNext(buf);
                        }
                    }
                }),
                new JsonPointer("/status", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryStatusObservable != null) {
                            String statusStr = buf.toString(CHARSET);
                            buf.release();

                            statusStr = statusStr.substring(1, statusStr.length() - 1);
                            if (!statusStr.equals("success")) {
                                status = ResponseStatus.FAILURE;
                            }
                            queryStatusObservable.onNext(statusStr);

                            //overwrite existing response object if streamed in status
                            if (!sentResponse) {
                                createResponse();
                                LOGGER.trace("Received status for requestId {}", requestID);
                            }
                        }
                    }
                }),
                new JsonPointer("/results/-", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryRowObservable != null) {
                            queryRowObservable.onNext(buf);
                            if (response == null) {
                                createResponse();
                                LOGGER.trace("Started receiving results for requestId {}", requestID);
                            }
                        }
                    }
                }),
                new JsonPointer("/errors/-", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryErrorObservable != null) {
                            queryErrorObservable.onNext(buf);
                            if (response == null) {
                                createResponse();
                                LOGGER.trace("Started receiving errors for requestId {}", requestID);
                            }
                        }
                    }
                }),
                new JsonPointer("/warnings/-", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryErrorObservable != null) {
                            queryErrorObservable.onNext(buf);
                            if (response == null) {
                                createResponse();
                                LOGGER.trace("Started receiving warnings for requestId {}", requestID);
                            }
                        }
                    }
                }),
                new JsonPointer("/profile", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryProfileInfoObservable != null) {
                            queryProfileInfoObservable.onNext(buf);
                        }
                    }
                }),
                new JsonPointer("/metrics", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryInfoObservable != null) {
                            queryInfoObservable.onNext(buf);
                        }

                        if (currentRequest.span() != null) {
                            if (env.operationTracingEnabled()) {
                                env.tracer().scopeManager()
                                    .activate(response.request().span(), true)
                                    .close();
                            }
                        }
                    }
                }),
        };
        this.parser = new ByteBufJsonParser(jsonPointers);
    }

    /**
     * True if this parser is currently initialized and ready to parse a response.
     *
     * @return true if initialized, false otherwise.
     */
    public boolean isInitialized() {
        return this.initialized;
    }

    /**
     * Initialize this parser for a response parsing cycle.
     *
     *
     * @param responseContent the raw content to parse from.
     * @param responseStatus the status of the response.
     * @param request the original request.
     */
    public void initialize(final ByteBuf responseContent, final ResponseStatus responseStatus,
        final CouchbaseRequest request) {
        this.requestID = "";
        this.clientContextID = ""; //initialize to empty string instead of null as it is optional on the wire
        this.sentResponse = false;
        this.response = null;
        this.status = responseStatus;
        this.responseContent = responseContent;
        this.currentRequest = request;

        queryRowObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
        queryErrorObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
        queryStatusObservable = AsyncSubject.create();
        queryInfoObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
        querySignatureObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
        queryProfileInfoObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);

        parser.initialize(responseContent);
        initialized = true;
    }

    /**
     * Helper method to initialize the internal response structure once ready.
     */
    private void createResponse() {
        response = new GenericQueryResponse(
            queryErrorObservable.onBackpressureBuffer(),
            queryRowObservable.onBackpressureBuffer(),
            querySignatureObservable.onBackpressureBuffer(),
            queryStatusObservable.onBackpressureBuffer(),
            queryInfoObservable.onBackpressureBuffer(),
            queryProfileInfoObservable.onBackpressureBuffer(),
            currentRequest,
            status,
            requestID,
            clientContextID
        );
    }

    /**
     * Instruct the parser to run a new parsing cycle on the current response content.
     *
     * @return the {@link GenericQueryResponse} if ready, null otherwise.
     * @throws Exception if the internal parsing can't complete.
     */
    public GenericQueryResponse parse() throws Exception {
        try {
            parser.parse();
            //discard only if EOF is not thrown
            responseContent.discardReadBytes();
            LOGGER.trace("Received last chunk and completed parsing for requestId {}", requestID);
        } catch (EOFException ex) {
            //ignore as we expect chunked responses
            LOGGER.trace("Still expecting more data for requestId {}", requestID);
        }

        //return back response only once
        if (!this.sentResponse && this.response != null) {
            this.sentResponse = true;
            return this.response;
        }

        return null;
    }

    /**
     * Instruct the parser to finish the parsing and reset its internal state, turning it
     * back to uninitialized as well.
     */
    public void finishParsingAndReset() {
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
        if (queryProfileInfoObservable != null) {
            queryProfileInfoObservable.onCompleted();
        }
        queryInfoObservable = null;
        queryRowObservable = null;
        queryErrorObservable = null;
        queryStatusObservable = null;
        querySignatureObservable = null;
        queryProfileInfoObservable = null;
        this.initialized = false;
    }
}