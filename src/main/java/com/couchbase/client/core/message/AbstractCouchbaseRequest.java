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

import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

/**
 * Default implementation for a {@link CouchbaseRequest}, should be extended by child messages.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public abstract class AbstractCouchbaseRequest implements CouchbaseRequest {

    /**
     * The observable which eventually completes the response.
     */
    private final Subject<CouchbaseResponse, CouchbaseResponse> observable;

    /**
     * The name of the bucket for this request.
     */
    private final String bucket;

    /**
     * The password of the bucket for this request.
     */
    private final String password;

    /**
     * Create a new {@link AbstractCouchbaseRequest}.
     *
     * Depending on the type of operation, bucket and password may be null, this needs to
     * be enforced properly by the child implementations.
     *
     * This constructor will create a AsyncSubject, which implies that the response for this
     * request only emits one message. If you need to expose a streaming response, use the
     * other constructor and feed it a ReplaySubject or something similar.
     *
     * @param bucket the name of the bucket.
     * @param password the password of the bucket.
     */
    protected AbstractCouchbaseRequest(String bucket, String password) {
        this(bucket, password, AsyncSubject.<CouchbaseResponse>create());
    }

    /**
     * Create a new {@link AbstractCouchbaseRequest}.
     *
     * Depending on the type of operation, bucket and password may be null, this needs to
     * be enforced properly by the child implementations.
     *
     * @param bucket the name of the bucket.
     * @param password the password of the bucket.
     */
    protected AbstractCouchbaseRequest(final String bucket, final String password,
        final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        this.bucket = bucket;
        this.password = password;
        this.observable = observable;
    }

    @Override
    public Subject<CouchbaseResponse, CouchbaseResponse> observable() {
        return observable;
    }

    @Override
    public String bucket() {
        return bucket;
    }

    @Override
    public String password() {
        return password;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(this.getClass().getSimpleName() + "{");
        sb.append("observable=").append(observable);
        sb.append(", bucket='").append(bucket).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
