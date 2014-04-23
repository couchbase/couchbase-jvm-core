package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class OpenBucketRequest extends AbstractCouchbaseRequest implements ClusterRequest {

    public OpenBucketRequest(String bucket, String password) {
        super(bucket, password);
    }

}
