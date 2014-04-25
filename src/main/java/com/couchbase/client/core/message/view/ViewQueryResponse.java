package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

public class ViewQueryResponse extends AbstractCouchbaseResponse {

    private final ByteBuf content;

    public ViewQueryResponse(ResponseStatus status, ByteBuf content) {
        super(status);
        this.content = content;
    }

    public ByteBuf content() {
        return content;
    }
}
