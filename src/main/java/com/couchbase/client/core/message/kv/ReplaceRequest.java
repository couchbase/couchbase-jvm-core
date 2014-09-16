/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.message.kv;


import io.netty.buffer.ByteBuf;

public class ReplaceRequest extends AbstractKeyValueRequest implements BinaryStoreRequest {

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

    private final boolean json;

    /**
     * Creates a new {@link ReplaceRequest}.
     *
     * @param key the key of the document.
     * @param content the content of the document.
     * @param bucket the name of the bucket.
     */
    public ReplaceRequest(final String key, final ByteBuf content, final String bucket) {
        this(key, content, 0, 0, 0, bucket, false);
    }

    public ReplaceRequest(final String key, final ByteBuf content, final String bucket, final boolean json) {
        this(key, content, 0, 0, 0, bucket, json);
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
        this(key, content, cas, 0, 0, bucket, false);
    }

    public ReplaceRequest(final String key, final ByteBuf content, final long cas, final String bucket, final boolean json) {
        this(key, content, cas, 0, 0, bucket, json);
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
        this(key, content, cas, exp, flags, bucket, false);
    }

    public ReplaceRequest(final String key, final ByteBuf content, final long cas, final int exp, final int flags, final String bucket, final boolean json) {
        super(key, bucket, null);
        this.content = content;
        this.expiration = exp;
        this.flags = flags;
        this.cas = cas;
        this.json = json;
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

    public boolean isJson() {
        return json;
    }
}
