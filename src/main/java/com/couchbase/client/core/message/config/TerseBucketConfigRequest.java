package com.couchbase.client.core.message.config;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class TerseBucketConfigRequest extends AbstractCouchbaseRequest implements ConfigRequest {

    private static final String PATH = "/pools/default/b/";

    private final String hostname;

    public TerseBucketConfigRequest(String hostname, String bucket, String password) {
        super(bucket, password);
        this.hostname = hostname;
    }

    public String hostname() {
        return hostname;
    }

    public String path() {
        return PATH + bucket();
    }
}
