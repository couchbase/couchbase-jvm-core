package com.couchbase.client.core.message;

/**
 * Typesafe status code returned by {@link CouchbaseResponse}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public enum ResponseStatus {

    /**
     * If the response is successful.
     */
    OK,

    /**
     * If the request expected the document to not exist, but it existed already.
     */
    EXISTS,

    /**
     * If the request expected the document to exit, but it didn't exist already.
     */
    NOT_EXISTS,

    /**
     * The underlying response indicates retry is in order.
     *
     * This is a internal response and should not bubble up to the user level.
     */
    RETRY,

    /**
     * Generic failure status.
     */
    FAILURE
}
