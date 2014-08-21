package com.couchbase.client.core;

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
