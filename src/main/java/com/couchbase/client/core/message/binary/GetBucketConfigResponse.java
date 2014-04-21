package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;

public class GetBucketConfigResponse extends AbstractCouchbaseResponse implements BinaryResponse {

    private final String content;

    public GetBucketConfigResponse(final String content) {
        this.content = content;
    }

    public String content() {
        return content;
    }

}
