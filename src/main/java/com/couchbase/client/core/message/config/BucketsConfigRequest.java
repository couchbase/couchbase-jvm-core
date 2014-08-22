package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class BucketsConfigRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    public BucketsConfigRequest(String bucket, String password) {
        super(bucket, password);
    }

    @Override
    public String path() {
        return "/pools/default/buckets/";
    }

}
