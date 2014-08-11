package com.couchbase.client.core.message.binary;

import io.netty.buffer.ByteBuf;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class PrependRequest extends AbstractBinaryRequest {

    private final long cas;
    private final ByteBuf content;

    public PrependRequest(String key, long cas, ByteBuf content, String bucket) {
        super(key, bucket, null);
        this.cas = cas;
        this.content = content;
    }

    public long cas() {
        return cas;
    }

    public ByteBuf content() {
        return content;
    }
    
}
