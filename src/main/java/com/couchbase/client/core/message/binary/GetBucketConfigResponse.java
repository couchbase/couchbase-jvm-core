package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;

public class GetBucketConfigResponse extends AbstractCouchbaseResponse implements BinaryResponse {

    private final String content;
    private String hostname;

    public GetBucketConfigResponse(final String content, final String hostname) {
        this.content = content;
        this.hostname = hostname;
    }

    public String content() {
        return content;
    }

    public String hostname() {
        return hostname;
    }
}
