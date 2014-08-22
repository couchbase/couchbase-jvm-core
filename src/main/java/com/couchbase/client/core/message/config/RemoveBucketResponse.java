package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

public class RemoveBucketResponse extends AbstractCouchbaseResponse {

    public RemoveBucketResponse(ResponseStatus status) {
        super(status, null);
    }

}
