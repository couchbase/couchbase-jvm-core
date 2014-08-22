package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class RemoveBucketRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private final String bucket;

    public RemoveBucketRequest(String bucket, String username, String password) {
        super(username, password);
        this.bucket = bucket;
    }

    @Override
    public String path() {
        return "/pools/default/buckets/" + bucket;
    }

}
