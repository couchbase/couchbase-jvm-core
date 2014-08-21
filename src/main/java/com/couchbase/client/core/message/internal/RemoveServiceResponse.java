package com.couchbase.client.core.message.internal;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class RemoveServiceResponse extends AbstractCouchbaseResponse {

    public RemoveServiceResponse(ResponseStatus status) {
        super(status, null);
    }
}
