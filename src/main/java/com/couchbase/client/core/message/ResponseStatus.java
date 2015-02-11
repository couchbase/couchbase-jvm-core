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

/**
 * Typesafe status code returned by {@link CouchbaseResponse}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public enum ResponseStatus {

    /**
     * If the response is successful and finished.
     */
    SUCCESS,

    /**
     * If the request expected the document to not exist, but it existed already.
     */
    EXISTS,

    /**
     * If the request expected the document to exit, but it didn't exist already.
     */
    NOT_EXISTS,

    /**
     * The response indicates that a mutation try did not happen properly.
     */
    NOT_STORED,

    /**
     * The response indicates that the request was too big for some reason.
     */
    TOO_BIG,

    /**
     * Indicates a failure which is considered to be transient.
     */
    TEMPORARY_FAILURE,

    /**
     * Indicates that the server is busy, which is considered to be transient.
     */
    SERVER_BUSY,

    /**
     * Indicates that the request type was dispatched but not known by the server or it is not supported.
     */
    COMMAND_UNAVAILABLE,

    /**
     * The requested service is currently out of memory.
     */
    OUT_OF_MEMORY,

    /**
     * The service reported that the request arguments are invalid.
     */
    INVALID_ARGUMENTS,

    /**
     * The remote service failed for an internal reason.
     */
    INTERNAL_ERROR,

    /**
     * Generic failure status.
     */
    FAILURE,

    /**
     * The underlying response indicates retry is in order.
     *
     * This is a internal response and should not bubble up to the user level.
     */
    RETRY;

    /**
     * Check if the current {@link ResponseStatus} is success.
     */
    public boolean isSuccess() {
        return this == ResponseStatus.SUCCESS;
    }

}
