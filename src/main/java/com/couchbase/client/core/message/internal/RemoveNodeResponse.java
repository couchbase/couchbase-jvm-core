package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class RemoveNodeResponse extends AbstractCouchbaseResponse {

    public RemoveNodeResponse(ResponseStatus status) {
        super(status, null);
    }
}
