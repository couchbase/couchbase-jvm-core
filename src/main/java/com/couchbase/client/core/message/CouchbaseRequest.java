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
}
