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
package com.couchbase.client.core.message.analytics;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.utils.Buffers;
import io.netty.buffer.ByteBuf;
import rx.Observable;

/**
 * The base response for Analytics (SQL++) requests. Response is divided into sub-sections, each of which
 * can be asynchronously fed. They are represented as {@link Observable Observable}, most of them of
 * {@link ByteBuf}. Note that it is important that these streams are consumed and their ByteBuf released.
 *
 * In order to reuse the values of a section but still correctly release the ByteBuf, the best is to
 * convert them into an appropriate gc-able value, release the buffer and cache the resulting stream.
 *
 * If one isn't interested in a particular sub-section, it should still be released by subscribing a
 * {@link Buffers#BYTE_BUF_RELEASER} to its stream.
 */
@InterfaceStability.Uncommitted
@InterfaceAudience.Public
public class GenericAnalyticsResponse extends AbstractCouchbaseResponse {

    private final Observable<ByteBuf> errors;
    private final Observable<ByteBuf> rows;
    private final Observable<String> queryStatus;
    private final Observable<ByteBuf> info;
    private final Observable<ByteBuf> signature;
    private final String handle;

    private final String requestId;
    private final String clientRequestId;

    public GenericAnalyticsResponse(Observable<ByteBuf> errors, Observable<ByteBuf> rows, Observable<ByteBuf> signature,
        Observable<String> queryStatus, Observable<ByteBuf> info, String handle,
        CouchbaseRequest request, ResponseStatus status, String requestId, String clientRequestId) {
        super(status, request);
        this.errors = errors;
        this.rows = rows;
        this.signature = signature;
        this.info = info;
        this.queryStatus = queryStatus;
        this.requestId = requestId;
        this.clientRequestId = clientRequestId == null ? "" : clientRequestId;
        this.handle = handle;
    }

    /**
     * Contains one {@link ByteBuf} for each result item returned by the server. Each item is a JSON object.
     */
    public Observable<ByteBuf> rows() {
        return rows;
    }

    /**
     * Contains a single {@link ByteBuf} representing the Analytics json signature of the results. May not appear at all
     * if there are no results (in case of fatal errors for example).
     */
    public Observable<ByteBuf> signature() {
        return this.signature;
    }

    /**
     * If there were errors and/or warnings while executing the query, contains a {@link ByteBuf} for each error
     * and each warning. These are JSON objects, that should at least contain a 'msg' and a 'code'.
     */
    public Observable<ByteBuf> errors() {
        return errors;
    }

    /**
     * Contains a single String denoting the status of the query (success, running, errors, completed, stopped, timeout,
     * fatal). The status is always emitted AFTER all {@link #rows()} and all {@link #errors()} have been emitted.
     */
    public Observable<String> queryStatus() {
        return queryStatus;
    }

    /**
     * Contains a single {@link ByteBuf} representing the JSON object of query execution metrics (or empty if metrics
     * haven't been activated).
     */
    public Observable<ByteBuf> info() { return info; }

    /**
     * @return the UUID for this request, can be used on the server side for tracing.
     */
    public String requestId() {
        return requestId;
    }

    /**
     * @return the client-provided identifier if provided in the request, empty string otherwise.
     */
    public String clientRequestId() {
        return clientRequestId;
    }

    /**
     * Contains the deferred handle url
     */
    public String handle() { return this.handle; }
}
