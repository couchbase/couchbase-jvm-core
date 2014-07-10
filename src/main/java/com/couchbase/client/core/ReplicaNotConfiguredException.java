package com.couchbase.client.core;

/**
 * Created by michaelnitschinger on 09/07/14.
 */
public class ReplicaNotConfiguredException extends CouchbaseException {

    public ReplicaNotConfiguredException() {
    }

    public ReplicaNotConfiguredException(String message) {
        super(message);
    }

    public ReplicaNotConfiguredException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReplicaNotConfiguredException(Throwable cause) {
        super(cause);
    }
}
