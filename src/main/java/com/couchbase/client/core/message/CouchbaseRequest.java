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

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.time.Delay;
import io.opentracing.Span;
import rx.Subscriber;
import rx.subjects.Subject;

import java.util.Observable;

/**
 * High-Level marker interface for all {@link CouchbaseRequest}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface CouchbaseRequest extends CouchbaseMessage {

    /**
     * Get the underlying {@link Observable}.
     *
     * @return the observable which will complete the response.
     */
    Subject<CouchbaseResponse, CouchbaseResponse> observable();



    /**
     * Emits the {@link CouchbaseResponse} into the underlying state holder (like a subject)
     * without completing it as per contract.
     *
     * @param response the response to emit.
     */
    void emit(CouchbaseResponse response);

    /**
     * Completes the underlying state holder.
     */
    void complete();

    /**
     * Emits the {@link CouchbaseResponse} into the underlying state holder (like a subject)
     * and completes it as well per contract.
     *
     * Its basically a shortcut for {@link #emit(CouchbaseResponse)} followed by {@link #complete()}.
     *
     * @param response the response to emit and complete.
     */
    void succeed(CouchbaseResponse response);

    /**
     * Fails the {@link CouchbaseResponse} on the state holder as per contract.
     *
     * @param throwable the throwable to fail the response with.
     */
    void fail(Throwable throwable);

    /**
     * The associated bucket name for this request.
     *
     * @return the bucket name.
     */
    String bucket();

    /**
     * User authorized for bucket access
     */
    String username();

    /**
     * The password associated with the bucket/ user
     *
     * @return the password.
     */
    String password();

    /**
     * Returns the old retry count and increments it by one.
     *
     * @return the old retryCount.
     */
    int incrementRetryCount();

    /**
     * Returns the current retry count.
     *
     * @return the current retry count.
     */
    int retryCount();

    /**
     * Sets the initial retry after time for the request.
     *
     * @param after
     */
    void retryAfter(long after);

    /**
     * Gets the initial retry after time for the request.
     *
     * @returns initial after time
     */
    long retryAfter();

    /**
     * Sets the maximum retry duration for the request.
     *
     * @param duration
     */
    void maxRetryDuration(long duration);

    /**
     * Returns the maximum retry duration for the request.
     *
     * @return duration
     */
    long maxRetryDuration();

    /**
     * Sets the retry delay config
     *
     * @param delay
     */
    void retryDelay(Delay delay);

    /**
     * Returns the retry delay config
     *
     * @return delay
     */
    Delay retryDelay();
    /**
     * Checks if the request is unsubscribed on a timeout
     *
     * @return false if timed out, else true
     */
    @InterfaceAudience.Private
    @InterfaceStability.Uncommitted
    boolean isActive();

    /**
     * Optionally add subscriber to check for the timeouts
     */
    @InterfaceAudience.Private
    @InterfaceStability.Uncommitted
    void subscriber(Subscriber subscriber);

    String dispatchHostname();

    void dispatchHostname(String hostname);

    /**
     * Returns the {@link Span} for this request if set.
     *
     * @return the span used.
     */
    Span span();

    /**
     * Allows to set a custom span that should be used.
     *
     * @param span the span to use.
     * @param env the core env used for context-sensitive stuff, may be null.
     */
    void span(Span span, CoreEnvironment env);

    /**
     * A unique operation id, used for tracing purposes.
     *
     * May or may not be null depending on the service implementation.
     * @return the operation id or null.
     */
    String operationId();
}
