package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class UpsertDesignDocumentResponse extends AbstractCouchbaseResponse {

    private final ByteBuf content;

    public UpsertDesignDocumentResponse(ResponseStatus status, ByteBuf content, CouchbaseRequest request) {
        super(status, request);
        this.content = content;
    }

    public ByteBuf content() {
        return content;
    }
}
