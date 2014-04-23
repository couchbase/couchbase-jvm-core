package com.couchbase.client.core.message.binary;


import io.netty.buffer.ByteBuf;

public class ReplaceRequest extends AbstractBinaryRequest {

    /**
     * The content of the document.
     */
    private final ByteBuf content;

    /**
     * The optional expiration time.
     */
    private final int expiration;

    /**
     * The optional flags.
     */
    private final int flags;

    /**
     * The cas value.
     */
    private final long cas;

    /**
     * Creates a new {@link ReplaceRequest}.
     *
     * @param key the key of the document.
     * @param content the content of the document.
     * @param bucket the name of the bucket.
     */
    public ReplaceRequest(final String key, final ByteBuf content, final String bucket) {
        this(key, content, 0, 0, 0, bucket);
    }

    /**
     * Creates a new {@link ReplaceRequest}.
     *
     * @param key the key of the document.
     * @param content the content of the document.
     * @param cas the cas value.
     * @param bucket the name of the bucket.
     */
    public ReplaceRequest(final String key, final ByteBuf content, final long cas, final String bucket) {
        this(key, content, cas, 0, 0, bucket);
    }

    /**
     * Creates a new {@link ReplaceRequest}.
     *
     * @param key the key of the document.
     * @param content the content of the document.
     * @param cas the cas value.
     * @param exp the expiration time.
     * @param flags optional flags.
     * @param bucket the the name of the bucket.
     */
    public ReplaceRequest(final String key, final ByteBuf content, final long cas, final int exp, final int flags, final String bucket) {
        super(key, bucket, null);
        this.content = content;
        this.expiration = exp;
        this.flags = flags;
        this.cas = cas;
    }

    /**
     * Returns the expiration time for this document.
     *
     * @return the expiration time.
     */
    public int expiration() {
        return expiration;
    }

    /**
     * Returns the flags for this document.
     *
     * @return the flags.
     */
    public int flags() {
        return flags;
    }

    /**
     * The content of the document.
     *
     * @return the content.
     */
    public ByteBuf content() {
        return content;
    }

    /**
     * The CAS value of the request.
     *
     * @return the cas value.
     */
    public long cas() {
        return cas;
    }
}
