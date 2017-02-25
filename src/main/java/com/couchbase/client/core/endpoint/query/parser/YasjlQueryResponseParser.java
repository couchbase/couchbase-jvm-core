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
 * yasjl based query response parser
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.3
 */
public class YasjlQueryResponseParser {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(YasjlQueryResponseParser.class);

    private ByteBufJsonParser parser;
    private String requestID;
    private String clientContextID;
    private boolean sentResponse;

    protected static final Charset CHARSET = CharsetUtil.UTF_8;

    protected ByteBuf responseContent;
    /**
     * Represents an observable that sends result chunks.
     */
    protected UnicastAutoReleaseSubject<ByteBuf> queryRowObservable;

    /**
     * Represents an observable that has the signature of the N1QL results if there are any.
     */
    protected UnicastAutoReleaseSubject<ByteBuf> querySignatureObservable;

    /**
     * Represents an observable that sends errors and warnings if any during query execution.
     */
    protected UnicastAutoReleaseSubject<ByteBuf> queryErrorObservable;

    /**
     * Represent an observable that has the final execution status of the query, once all result rows and/or
     * errors/warnings have been sent.
     */
    protected AsyncSubject<String> queryStatusObservable;

    /**
     * Represents an observable containing metrics on a terminated query.
     */
    protected UnicastAutoReleaseSubject<ByteBuf> queryInfoObservable;

    /**
     * Represents the current request
     */
    protected CouchbaseRequest currentRequest;

    /**
     * Scheduler for query response
     */
    protected Scheduler scheduler;

    /**
     * TTL for response observables
     */
    protected long ttl;

    /**
     * Response status
     */
    protected ResponseStatus status;

    /**
     * Flag to indicate if the parser is initialized
     */
    protected boolean initialized;

    /**
     * Response that should be returned on parse call
     */
    protected GenericQueryResponse response;

    public YasjlQueryResponseParser(Scheduler scheduler, long ttl) {
        this.scheduler = scheduler;
        this.ttl = ttl;
        this.response = null;

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
                new JsonPointer("/metrics", new JsonPointerCB1() {
                    public void call(ByteBuf buf) {
                        if (queryInfoObservable != null) {
                            queryInfoObservable.onNext(buf);
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
        };
        this.parser = new ByteBufJsonParser(jsonPointers);
    }

    public boolean isInitialized() {
        return this.initialized;
    }

    public void initialize(ByteBuf responseContent, final ResponseStatus responseStatus) {
        this.requestID = "";
        this.clientContextID = ""; //initialize to empty strings instead of null as we may not receive context id sometimes
        this.sentResponse = false;
        this.response = null;
        this.status = responseStatus;
        this.responseContent = responseContent;

        queryRowObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
        queryErrorObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
        queryStatusObservable = AsyncSubject.create();
        queryInfoObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);
        querySignatureObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);

        parser.initialize(responseContent);
        initialized = true;
    }

    private void createResponse() {
        //when streaming results/errors/status starts, build out the response
        response = new GenericQueryResponse(
                queryErrorObservable.onBackpressureBuffer().observeOn(scheduler),
                queryRowObservable.onBackpressureBuffer().observeOn(scheduler),
                querySignatureObservable.onBackpressureBuffer().observeOn(scheduler),
                queryStatusObservable.onBackpressureBuffer().observeOn(scheduler),
                queryInfoObservable.onBackpressureBuffer().observeOn(scheduler),
                currentRequest,
                status, requestID, clientContextID);
    }

    //parses the response content
    public GenericQueryResponse parse(boolean lastChunk) throws Exception {
        try {
            parser.parse();
            //discard only if EOF is not thrown
            responseContent.discardSomeReadBytes();
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
        queryInfoObservable = null;
        queryRowObservable = null;
        queryErrorObservable = null;
        queryStatusObservable = null;
        querySignatureObservable = null;
        this.initialized = false;
    }
}