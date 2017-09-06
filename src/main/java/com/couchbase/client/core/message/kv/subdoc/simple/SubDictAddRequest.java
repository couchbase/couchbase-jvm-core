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
import io.netty.buffer.ByteBuf;

/**
 * A sub-document dictionary add operation.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public class SubDictAddRequest extends AbstractSubdocMutationRequest {

    /**
     * Creates a new {@link SubDictAddRequest}.
     *
     * @param key        the key of the document.
     * @param path       the subdocument path to consider inside the document. Last element of the path is expected to
     *                   be created by this operation.
     * @param fragment   the fragment of valid JSON to mutate into at the site denoted by the path.
     * @param bucket     the bucket of the document.
     * @throws NullPointerException     if the path is null (see {@link #EXCEPTION_NULL_PATH})
     * @throws IllegalArgumentException if the path is empty (see {@link #EXCEPTION_EMPTY_PATH})
     */
    public SubDictAddRequest(String key, String path, ByteBuf fragment, String bucket) {
        this(key, path, fragment, bucket, 0, 0L);
    }

    /**
     * Creates a new {@link SubDictAddRequest}.
     *
     * @param key        the key of the document.
     * @param path       the subdocument path to consider inside the document. Last element of the path is expected to
     *                   be created by this operation.
     * @param fragment   the fragment of valid JSON to mutate into at the site denoted by the path.
     * @param bucket     the bucket of the document.
     * @param expiration the TTL of the whole enclosing document.
     * @param cas        the CAS value for the mutation.
     * @throws NullPointerException     if the path is null (see {@link #EXCEPTION_NULL_PATH})
     * @throws IllegalArgumentException if the path is empty (see {@link #EXCEPTION_EMPTY_PATH})
     */
    public SubDictAddRequest(String key, String path, ByteBuf fragment, String bucket, int expiration, long cas) {
        super(key, path, fragment, bucket, expiration, cas);
        if (path.isEmpty()) {
            cleanUpAndThrow(EXCEPTION_EMPTY_PATH);
        }
    }

    /**
     * {@inheritDoc}
     * @return {@link KeyValueHandler#OP_SUB_DICT_ADD}
     */
    @Override
    public byte opcode() {
        return KeyValueHandler.OP_SUB_DICT_ADD;
    }
}
