/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    public ReplaceRequest(final String key, final ByteBuf content, final long cas, final String bucket,
        final boolean json) {
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
    public ReplaceRequest(final String key, final ByteBuf content, final long cas, final int exp, final int flags,
        final String bucket) {
        this(key, content, cas, exp, flags, bucket, false);
    }

    public ReplaceRequest(final String key, final ByteBuf content, final long cas, final int exp, final int flags,
        final String bucket, final boolean json) {
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
