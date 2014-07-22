package com.couchbase.client.core.endpoint.binary;

import com.couchbase.client.core.CouchbaseException;

public class AuthenticationException extends CouchbaseException {

    public AuthenticationException() {
    }

    public AuthenticationException(String message) {
        super(message);
    }

    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuthenticationException(Throwable cause) {
        super(cause);
    }
}
