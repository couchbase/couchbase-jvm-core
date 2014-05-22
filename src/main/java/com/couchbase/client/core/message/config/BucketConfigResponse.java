package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * Created by michael on 22/05/14.
 */
public class BucketConfigResponse extends AbstractCouchbaseResponse {

    private final String config;

    public BucketConfigResponse(String config, ResponseStatus status) {
        super(status);
        this.config = config;
    }

    public String config() {
        return config;
    }
}
