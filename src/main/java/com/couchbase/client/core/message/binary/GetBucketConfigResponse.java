package com.couchbase.client.core.message.binary;

public class GetBucketConfigResponse implements BinaryResponse {

    private final String content;

    public GetBucketConfigResponse(final String content) {
        this.content = content;
    }

    public String content() {
        return content;
    }

}
