package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class ReplaceResponse extends AbstractCouchbaseResponse implements BinaryResponse {

    private final long cas;

    public ReplaceResponse(ResponseStatus status, long cas) {
        super(status);
        this.cas = cas;
    }

    public long cas() {
        return cas;
    }
}
