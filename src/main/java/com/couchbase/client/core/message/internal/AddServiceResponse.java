package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

import java.net.InetAddress;

public class AddServiceResponse extends AbstractCouchbaseResponse implements CouchbaseResponse {

    private final InetAddress hostname;

    public AddServiceResponse(final ResponseStatus status, final InetAddress hostname) {
        super(status, null);
        this.hostname = hostname;
    }

    public InetAddress hostname() {
        return hostname;
    }
}
