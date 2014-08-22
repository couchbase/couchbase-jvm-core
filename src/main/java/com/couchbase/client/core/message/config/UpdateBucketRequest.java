package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class UpdateBucketRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private final String payload;
    private final String bucket;

    public UpdateBucketRequest(String bucket, String payload, String username, String password) {
        super(username, password);
        this.bucket = bucket;
        this.payload = payload;
    }

    @Override
    public String path() {
        return "/pools/default/buckets/" + bucket;
    }

    public String payload() {
        return payload;
    }
}
