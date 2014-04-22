package com.couchbase.client.core.message.binary;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import io.netty.buffer.ByteBuf;

public class UpsertRequest extends AbstractCouchbaseRequest implements BinaryRequest {

    private final String key;
    private final ByteBuf content;
    private short partition;

    public UpsertRequest(String key, ByteBuf content, String bucket, String password) {
        this.key = key;
        this.content = content;
        bucket(bucket);
    }

    public String key() {
        return key;
    }

    public ByteBuf content() {
        return content;
    }

    @Override
    public short partition() {
        return partition;
    }

    public int expiration() {
        return 0;
    }

    public int flags() {
        return 0;
    }

    @Override
    public BinaryRequest partition(short id) {
        this.partition = id;
        return this;
    }
}
