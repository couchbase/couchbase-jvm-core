package com.couchbase.client.core.env;

import com.couchbase.client.core.CouchbaseException;

/**
 * Created by michael on 17/04/14.
 */
public class EnvironmentException extends CouchbaseException {

    private static final long serialVersionUID = -5347402369944018053L;

    public EnvironmentException(String message) {
        super(message);
    }

    public EnvironmentException(String message, Throwable cause) {
        super(message, cause);
    }

}
