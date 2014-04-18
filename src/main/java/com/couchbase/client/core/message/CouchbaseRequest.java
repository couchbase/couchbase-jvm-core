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
package com.couchbase.client.core.message;

import rx.subjects.Subject;

import java.util.Observable;

/**
 * High-Level marker interface for all {@link CouchbaseRequest}s.
 */
public interface CouchbaseRequest {

    /**
     * Get the underlying {@link Observable}.
     *
     * @return the observable which will complete the response.
     */
    Subject<CouchbaseResponse, CouchbaseResponse> observable();

    /**
     * Set the underlying {@link Observable} which will complete the associated response.
     *
     * @param observable the observable to complete later.
     * @return the {@link CouchbaseRequest} for chaining purposes.
     */
    CouchbaseRequest observable(Subject<CouchbaseResponse, CouchbaseResponse> observable);

    /**
     * The associated bucket name for this request.
     *
     * @return the bucket name.
     */
    String bucket();

    /**
     * Set the associated bucket name for this request.
     *
     * @param bucket the bucket name.
     * @return the {@link CouchbaseRequest} for chaining purposes.
     */
    CouchbaseRequest bucket(String bucket);

}
