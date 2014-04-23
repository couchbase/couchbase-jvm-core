package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class AddServiceResponse extends AbstractCouchbaseResponse implements CouchbaseResponse {

    private final String hostname;

    public AddServiceResponse(final ResponseStatus status, final String hostname) {
        super(status);
        this.hostname = hostname;
    }

    public String hostname() {
        return hostname;
    }
}
