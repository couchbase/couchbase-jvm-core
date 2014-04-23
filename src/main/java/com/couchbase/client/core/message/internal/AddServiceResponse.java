package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.CouchbaseResponse;

/**
 * Created by michael on 22/04/14.
 */
public class AddServiceResponse implements CouchbaseResponse {

    private final String hostname;

    public AddServiceResponse(final String hostname) {
        this.hostname = hostname;
    }

    public String hostname() {
        return hostname;
    }
}
