package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class OpenBucketResponse extends AbstractCouchbaseResponse {

    public OpenBucketResponse(ResponseStatus status) {
        super(status, null);
    }
}
