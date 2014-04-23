package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class ReplaceResponse extends AbstractCouchbaseResponse implements BinaryResponse {

    public ReplaceResponse(ResponseStatus status) {
        super(status);
    }
}
