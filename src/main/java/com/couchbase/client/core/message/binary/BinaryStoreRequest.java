package com.couchbase.client.core.message.binary;

import io.netty.buffer.ByteBuf;

public interface BinaryStoreRequest extends BinaryRequest {

    int expiration();

    int flags();

    ByteBuf content();

    boolean isJson();
}
