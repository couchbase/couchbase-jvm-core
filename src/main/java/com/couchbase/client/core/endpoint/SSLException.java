package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.CouchbaseException;

/**
 * Created by michael on 13/05/14.
 */
public class SSLException extends CouchbaseException {

    public SSLException() {
    }

    public SSLException(String message) {
        super(message);
    }

    public SSLException(String message, Throwable cause) {
        super(message, cause);
    }

    public SSLException(Throwable cause) {
        super(cause);
    }
}
