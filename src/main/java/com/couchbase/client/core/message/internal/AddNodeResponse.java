package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class AddNodeResponse extends AbstractCouchbaseResponse implements CouchbaseResponse {

    private final String hostname;

    public AddNodeResponse(ResponseStatus status, String hostname) {
        super(status);
        this.hostname = hostname;
    }

    public String hostname() {
        return hostname;
    }
}
