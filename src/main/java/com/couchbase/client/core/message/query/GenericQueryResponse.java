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
package com.couchbase.client.core.message.query;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.utils.Buffers;
import io.netty.buffer.ByteBuf;
import rx.Observable;

/**
 * The base response for Query (N1QL) requests. Response is divided into sub-sections, each of which
 * can be asynchronously fed. They are represented as {@link Observable Observable}, most of them of
 * {@link ByteBuf}. Note that it is important that these streams are consumed and their ByteBuf released.
 *
 * In order to reuse the values of a section but still correctly release the ByteBuf, the best is to
 * convert them into an appropriate gc-able value, release the buffer and cache the resulting stream.
 *
 * If one isn't interested in a particular sub-section, it should still be released by subscribing a
 * {@link Buffers#BYTE_BUF_RELEASER} to its stream.
 */
public class GenericQueryResponse extends AbstractCouchbaseResponse {

    private final Observable<ByteBuf> errors;
    private final Observable<ByteBuf> rows;
    private final Observable<String> queryStatus;
    private final Observable<ByteBuf> info;
    private final Observable<ByteBuf> signature;

    private final String requestId;
    private final String clientRequestId;

    public GenericQueryResponse(Observable<ByteBuf> errors, Observable<ByteBuf> rows, Observable<ByteBuf> signature,
            Observable<String> queryStatus, Observable<ByteBuf> info,
            CouchbaseRequest request, ResponseStatus status, String requestId, String clientRequestId) {
        super(status, request);
        this.errors = errors;
        this.rows = rows;
        this.signature = signature;
        this.info = info;
        this.queryStatus = queryStatus;
        this.requestId = requestId;
        this.clientRequestId = clientRequestId == null ? "" : clientRequestId;
    }

    /**
     * Contains one {@link ByteBuf} for each result item returned by the server. Each item is a JSON object.
     */
    public Observable<ByteBuf> rows() {
        return rows;
    }

    /**
     * Contains a single {@link ByteBuf} representing the N1QL json signature of the results. May not appear at all
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
}
