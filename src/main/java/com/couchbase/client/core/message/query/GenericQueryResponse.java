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
import io.netty.buffer.ByteBuf;
import rx.Observable;


public class GenericQueryResponse extends AbstractCouchbaseResponse {

    private final Observable<ByteBuf> errors;
    private final Observable<ByteBuf> rows;
    private final Observable<String> queryStatus;
    private final Observable<ByteBuf> info;
    private final String requestId;
    private final String clientRequestId;

    public GenericQueryResponse(Observable<ByteBuf> errors, Observable<ByteBuf> rows,
            Observable<String> queryStatus, Observable<ByteBuf> info,
            CouchbaseRequest request, ResponseStatus status, String requestId, String clientRequestId) {
        super(status, request);
        this.errors = errors;
        this.rows = rows;
        this.info = info;
        this.queryStatus = queryStatus;
        this.requestId = requestId;
        this.clientRequestId = clientRequestId == null ? "" : clientRequestId;
    }

    public Observable<ByteBuf> rows() {
        return rows;
    }

    public Observable<ByteBuf> errors() {
        return errors;
    }

    public Observable<String> queryStatus() {
        return queryStatus;
    }

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
