/*
 * Copyright (c) 2015 Couchbase, Inc.
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

package com.couchbase.client.core.message.kv.subdoc.simple;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.endpoint.kv.KeyValueHandler;
import io.netty.buffer.ByteBuf;

/**
 * A sub-document dictionnary upsert operation.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class SubDictUpsertRequest extends AbstractSubdocMutationRequest {

    /**
     * Creates a new {@link SubDictUpsertRequest}.
     *
     * @param key        the key of the document.
     * @param path       the subdocument path to consider inside the document.
     * @param fragment   the fragment of valid JSON to mutate into at the site denoted by the path.
     * @param bucket     the bucket of the document.
     * @throws NullPointerException     if the path is null (see {@link #EXCEPTION_NULL_PATH})
     * @throws IllegalArgumentException if the path is empty (see {@link #EXCEPTION_EMPTY_PATH})
     */
    public SubDictUpsertRequest(String key, String path, ByteBuf fragment, String bucket) {
        this(key, path, fragment, bucket, 0, 0L);
    }

    /**
     * Creates a new {@link SubDictUpsertRequest}.
     *
     * @param key        the key of the document.
     * @param path       the subdocument path to consider inside the document.
     * @param fragment   the fragment of valid JSON to mutate into at the site denoted by the path.
     * @param bucket     the bucket of the document.
     * @param expiration the TTL of the whole enclosing document.
     * @param cas        the CAS value.
     * @throws NullPointerException     if the path is null (see {@link #EXCEPTION_NULL_PATH})
     * @throws IllegalArgumentException if the path is empty (see {@link #EXCEPTION_EMPTY_PATH})
     */
    public SubDictUpsertRequest(String key, String path, ByteBuf fragment, String bucket, int expiration, long cas) {
        super(key, path, fragment, bucket, expiration, cas);
        if (path.isEmpty()) {
            cleanUpAndThrow(EXCEPTION_EMPTY_PATH);
        }
    }

    /**
     * {@inheritDoc}
     * @return {@link KeyValueHandler#OP_SUB_DICT_UPSERT}
     */
    @Override
    public byte opcode() {
        return KeyValueHandler.OP_SUB_DICT_UPSERT;
    }
}
