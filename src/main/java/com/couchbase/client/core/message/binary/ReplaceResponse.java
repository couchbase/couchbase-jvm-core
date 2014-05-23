package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

public class ReplaceResponse extends AbstractBinaryResponse {

    private final long cas;

    public ReplaceResponse(ResponseStatus status, long cas, String bucket, ByteBuf content, CouchbaseRequest request) {
        super(status, bucket, content, request);
        this.cas = cas;
    }

    public long cas() {
        return cas;
    }
}
