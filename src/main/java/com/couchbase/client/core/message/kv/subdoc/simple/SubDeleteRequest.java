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
@InterfaceStability.Committed
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
