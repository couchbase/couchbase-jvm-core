package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

/**
 * Created by michael on 17/04/14.
 */
public class GetRequest extends AbstractCouchbaseRequest implements BinaryRequest {

    private final String key;

    public GetRequest(final String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
