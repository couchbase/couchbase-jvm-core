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
    private final String content;

    public FlushResponse(boolean done, String content, ResponseStatus status) {
        super(status, null);
        this.done = done;
        this.content = content;
    }

    public boolean isDone() {
        return done;
    }

    public String content() {
        return content;
    }
}
