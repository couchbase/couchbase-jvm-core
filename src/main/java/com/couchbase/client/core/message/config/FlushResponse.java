package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class FlushResponse extends AbstractCouchbaseResponse {

    private final boolean done;

    public FlushResponse(boolean done, ResponseStatus status) {
        super(status, null);
        this.done = done;
    }

    public boolean isDone() {
        return done;
    }

}
