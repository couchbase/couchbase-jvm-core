package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;


public class DisconnectResponse extends AbstractCouchbaseResponse {

    public DisconnectResponse(ResponseStatus status) {
        super(status);
    }
}
