/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint.analytics.parser;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

import com.couchbase.client.core.message.analytics.GenericAnalyticsResponse;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.utils.UnicastAutoReleaseSubject;
import com.couchbase.client.core.utils.yasjl.ByteBufJsonParser;
import com.couchbase.client.core.utils.yasjl.Callbacks.JsonPointerCB1;
import com.couchbase.client.core.utils.yasjl.JsonPointer;
import java.io.EOFException;
import io.netty.buffer.ByteBuf;
import rx.Scheduler;
import java.util.concurrent.TimeUnit;

/**
 * Analytics query response parser, based on yasjl.
 *
 * @author Subhashni Balakrishnan
 */
public class YasjlAnalyticsDeferredResponseParser {

    /**
     * The logger used for this parser.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(YasjlAnalyticsResponseParser.class);

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
     * Represents the current request
     */
    private CouchbaseRequest currentRequest;

    /**
     * Flag to indicate if the parser is initialized
     */
    private boolean initialized;

    /**
     * Response that should be returned on parse call
     */
    private GenericAnalyticsResponse response;

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
     * Create a new {@link com.couchbase.client.core.endpoint.query.parser.YasjlQueryResponseParser}.
     *
     * @param scheduler the scheduler which should be used when computations are moved out.
     * @param ttl the ttl used for the subjects until their contents are garbage collected.
     */
    public YasjlAnalyticsDeferredResponseParser(final Scheduler scheduler, final long ttl, final CoreEnvironment env) {
        this.scheduler = scheduler;
        this.ttl = ttl;
        this.response = null;
        this.env = env;

        JsonPointer[] jsonPointers = {

            new JsonPointer("/-", new JsonPointerCB1() {
                public void call(ByteBuf buf) {
                    if (queryRowObservable != null) {
                        queryRowObservable.onNext(buf);
                        if (response == null) {
                            createResponse();
                            LOGGER.trace("Started receiving results for deferred queries");
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
        this.sentResponse = false;
        this.response = null;
        this.responseContent = responseContent;
        this.currentRequest = request;

        queryRowObservable = UnicastAutoReleaseSubject.create(ttl, TimeUnit.MILLISECONDS, scheduler);

        parser.initialize(responseContent);
        initialized = true;
    }

    /**
     * Helper method to initialize the internal response structure once ready.
     */
    private void createResponse() {
        response = new GenericAnalyticsResponse(
            null,
            queryRowObservable.onBackpressureBuffer(),
            null,
            null,
            null,
            null,
            currentRequest,
            null,
            null,
            null
        );
    }

    /**
     * Instruct the parser to run a new parsing cycle on the current response content.
     *
     * @return the {@link GenericQueryResponse} if ready, null otherwise.
     * @throws Exception if the internal parsing can't complete.
     */
    public GenericAnalyticsResponse parse() throws Exception {
        try {
            parser.parse();
            //discard only if EOF is not thrown
            responseContent.discardReadBytes();
        } catch (EOFException ex) {
            //ignore as we expect chunked responses
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
        queryRowObservable = null;
        this.initialized = false;
    }
}