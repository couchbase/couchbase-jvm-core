package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class SeedNodesResponse extends AbstractCouchbaseResponse {

    public SeedNodesResponse(ResponseStatus status) {
        super(status, null);
    }

}
