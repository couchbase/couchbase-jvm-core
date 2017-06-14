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

/**
 * The default representation of a {@link CouchbaseResponse}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public abstract class AbstractCouchbaseResponse implements CouchbaseResponse {

    /**
     * The status for this response.
     */
    private final ResponseStatus status;

    private final CouchbaseRequest request;

    /**
     * The time when the response was created.
     */
    private final long creationTime;

    /**
     * The detailed status for this response, might be not set at all (null).
     */
    private volatile ResponseStatusDetails statusDetails;

    /**
     * Sets the required properties for the response.
     *
     * @param status the status of the response.
     */
    protected AbstractCouchbaseResponse(final ResponseStatus status, final CouchbaseRequest request) {
        this.status = status;
        this.request = request;
        this.creationTime = System.nanoTime();
    }

    @Override
    public ResponseStatus status() {
        return status;
    }

    @Override
    public ResponseStatusDetails statusDetails() {
        return statusDetails;
    }

    @Override
    public void statusDetails(final ResponseStatusDetails statusDetails) {
        if (this.statusDetails == null) {
            this.statusDetails = statusDetails;
        }
    }

    /**
     * Stub method implementation which needs to be overridden by all responses that support cloning.
     */
    @Override
    public CouchbaseRequest request() {
        return request;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "status=" + status +
                ", request=" + request +
                ", creationTime=" + creationTime +
                ", statusDetails=" + statusDetails +
                '}';
    }
}
