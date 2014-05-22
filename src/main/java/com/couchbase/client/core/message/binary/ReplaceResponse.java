package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

public class ReplaceResponse extends AbstractBinaryResponse {

    private final long cas;

    public ReplaceResponse(ResponseStatus status, long cas, String bucket, ByteBuf content) {
        super(status, bucket, content);
        this.cas = cas;
    }

    public long cas() {
        return cas;
    }
}
