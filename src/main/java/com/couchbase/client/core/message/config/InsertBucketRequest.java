package com.couchbase.client.core.message.config;


import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class InsertBucketRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private final String payload;

    public InsertBucketRequest(String payload, String username, String password) {
        super(username, password);
        this.payload = payload;
    }

    @Override
    public String path() {
        return "/pools/default/buckets";
    }

    public String payload() {
        return payload;
    }
}
