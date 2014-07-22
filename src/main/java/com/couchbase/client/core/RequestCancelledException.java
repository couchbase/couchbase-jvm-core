package com.couchbase.client.core;

/**
 * Created by michaelnitschinger on 28/07/14.
 */
public class RequestCancelledException extends CouchbaseException {

    public RequestCancelledException() {
    }

    public RequestCancelledException(String message) {
        super(message);
    }

    public RequestCancelledException(String message, Throwable cause) {
        super(message, cause);
    }

    public RequestCancelledException(Throwable cause) {
        super(cause);
    }
}
