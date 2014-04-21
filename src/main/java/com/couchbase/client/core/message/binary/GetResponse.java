package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;

/**
 * Created by michael on 17/04/14.
 */
public class GetResponse extends AbstractCouchbaseResponse implements BinaryResponse {

    private final String content;

    public GetResponse(final String content) {
        this.content = content;
    }

    public String content() {
        return content;
    }
}
