package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class GetBucketConfigRequest extends AbstractCouchbaseRequest implements BinaryRequest {

    @Override
    public String key() {
        return null;
    }

    @Override
    public short partition() {
        return 0;
    }
}
