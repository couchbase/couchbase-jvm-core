package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import io.netty.buffer.ByteBuf;

/**
 * Created by michael on 17/04/14.
 */
public class UpsertRequest extends AbstractCouchbaseRequest implements BinaryRequest {

    private final String key;
    private final ByteBuf content;

    public UpsertRequest(String key, ByteBuf content, String bucket, String password) {
        this.key = key;
        this.content = content;
    }

    public String key() {
        return key;
    }

    public ByteBuf content() {
        return content;
    }
}
