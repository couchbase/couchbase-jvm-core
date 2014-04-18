package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

/**
 * Created by michael on 17/04/14.
 */
public class GetBucketConfigRequest extends AbstractCouchbaseRequest implements BinaryRequest {

    @Override
    public String key() {
        return null;
    }
}
