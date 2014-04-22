package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class OpenBucketRequest extends AbstractCouchbaseRequest implements ClusterRequest {

    private final String bucket;
    private final String password;

    public OpenBucketRequest(String bucket, String password) {
        this.bucket = bucket;
        this.password = password;
    }

    @Override
    public String bucket() {
        return bucket;
    }

    public String password() {
        return password;
    }
}
