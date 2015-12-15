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
import io.netty.buffer.Unpooled;

/**
 * A sub-document delete operation.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class SubDeleteRequest extends AbstractSubdocMutationRequest {

    /**
     * Creates a new {@link SubDeleteRequest}.
     *
     * @param key        the key of the document.
     * @param path       the subdocument path to delete from the document.
     * @param bucket     the bucket of the document.
     * @param expiration the TTL of the whole enclosing document.
     * @param cas        the cas value for the operation
     * @throws NullPointerException if the path is null (see {@link #EXCEPTION_NULL_PATH})
     * @throws IllegalArgumentException if the path is empty (see {@link #EXCEPTION_EMPTY_PATH})
     */
    public SubDeleteRequest(String key, String path, String bucket, int expiration, long cas) {
        super(key, path, Unpooled.EMPTY_BUFFER, bucket, expiration, cas);
        if (path.isEmpty()) {
            //it is important to use cleanUpAndThrow in order to release the internal buffer
            cleanUpAndThrow(EXCEPTION_EMPTY_PATH);
        }
    }

    /**
     * Creates a new {@link SubDeleteRequest}.
     *
     * @param key        the key of the document.
     * @param path       the subdocument path to delete from the document.
     * @param bucket     the bucket of the document.
     * @throws NullPointerException if the path is null (see {@link #EXCEPTION_NULL_PATH})
     * @throws IllegalArgumentException if the path is empty (see {@link #EXCEPTION_EMPTY_PATH})
     */
    public SubDeleteRequest(String key, String path, String bucket) {
        this(key, path, bucket, 0, 0L);
    }

    /**
     * {@inheritDoc}
     * @return {@link KeyValueHandler#OP_SUB_DELETE}
     */
    @Override
    public byte opcode() {
        return KeyValueHandler.OP_SUB_DELETE;
    }
}
