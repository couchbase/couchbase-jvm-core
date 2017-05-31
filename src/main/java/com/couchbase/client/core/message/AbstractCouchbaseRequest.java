/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.core.message;

import com.couchbase.client.core.time.Delay;
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
     * User authorized for bucket access
     */
    private final String username;

    /**
     * The password of the bucket/user for this request.
     */
    private final String password;

    /**
     * The time when the request was created.
     */
    private final long creationTime;

    private volatile int retryCount;

    private volatile long retryAfter;

    private volatile long maxRetryDuration;

    private Delay retryDelay;

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
     * @param password user password.
     */
    protected AbstractCouchbaseRequest(String bucket, String password) {
        this(bucket, bucket, password, AsyncSubject.<CouchbaseResponse>create());
    }

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
     * @param username user authorized to access the bucket.
     * @param password user password.
     */
    protected AbstractCouchbaseRequest(String bucket, String username, String password) {
        this(bucket, username, password, AsyncSubject.<CouchbaseResponse>create());
    }

    /**
     * Create a new {@link AbstractCouchbaseRequest}.
     *
     * Depending on the type of operation, bucket and password may be null, this needs to
     * be enforced properly by the child implementations.
     *
     * @param bucket the name of the bucket.
     * @param username user authorized to access the bucket.
     * @param password the password of the bucket.
     */
    protected AbstractCouchbaseRequest(final String bucket, final String username, final String password,
        final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        this.bucket = bucket;
        this.username = username;
        this.password = password;
        this.observable = observable;
        this.creationTime = System.nanoTime();
        this.retryCount = 0;
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
    public String username() {
        return username;
    }

    @Override
    public String password() {
        return password;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public int incrementRetryCount() {
        return retryCount++;
    }

    @Override
    public int retryCount() {
        return retryCount;
    }

    @Override
    public void retryAfter(long after) {
        this.retryAfter = after;
    }

    @Override
    public long retryAfter() {
        return this.retryAfter;
    }

    @Override
    public void maxRetryDuration(long maxRetryDuration) {
        this.maxRetryDuration = maxRetryDuration;
    }
    @Override
    public long maxRetryDuration() { return this.maxRetryDuration; }

    @Override
    public void retryDelay(Delay retryDelay) {
        this.retryDelay = retryDelay;
    }

    @Override
    public Delay retryDelay() {
        return this.retryDelay;
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
