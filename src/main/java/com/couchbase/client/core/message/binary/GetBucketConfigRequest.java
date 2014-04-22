package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class GetBucketConfigRequest extends AbstractCouchbaseRequest implements BinaryRequest {

    private final String hostname;

    public GetBucketConfigRequest(String bucket, String hostname) {
        this.bucket(bucket);
        this.hostname = hostname;
    }

    @Override
    public String key() {
        return null;
    }

    @Override
    public short partition() {
        return 0;
    }

    @Override
    public BinaryRequest partition(short id) {
        return null;
    }

    public String hostname() {
        return hostname;
    }
}
