package com.couchbase.client.core.message.binary;

/**
 * Created by michael on 17/04/14.
 */
public class GetResponse implements BinaryResponse {

    private final String content;

    public GetResponse(final String content) {
        this.content = content;
    }

    public String content() {
        return content;
    }
}
