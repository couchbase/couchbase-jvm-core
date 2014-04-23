package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class RemoveResponse extends AbstractCouchbaseResponse implements BinaryResponse {

    public RemoveResponse(ResponseStatus status) {
        super(status);
    }
}
