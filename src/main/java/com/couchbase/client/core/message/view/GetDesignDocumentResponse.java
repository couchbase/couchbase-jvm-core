package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

public class GetDesignDocumentResponse extends AbstractCouchbaseResponse {

    private final String name;
    private final boolean development;
    private final ByteBuf content;

    public GetDesignDocumentResponse(String name, boolean development, ByteBuf content, ResponseStatus status, CouchbaseRequest request) {
        super(status, request);
        this.name = name;
        this.development = development;
        this.content = content;
    }

    public String name() {
        return name;
    }

    public boolean development() {
        return development;
    }

    public ByteBuf content() {
        return content;
    }
}
