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

/**
 * A sub-document get operation.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public class SubGetRequest extends AbstractSubdocRequest {

    private boolean xattr;
    private boolean accessDeleted;

    /**
     * Creates a new {@link SubGetRequest}.
     *
     * @param key      the key of the document.
     * @param path     the subdocument path to consider inside the document.
     * @param bucket   the bucket of the document.
     * @throws NullPointerException     if the path is null (see {@link #EXCEPTION_NULL_PATH})
     * @throws IllegalArgumentException if the path is empty (see {@link #EXCEPTION_EMPTY_PATH})
     */
    public SubGetRequest(String key, String path, String bucket) {
        super(key, path, bucket);
        if (path.isEmpty()) {
            cleanUpAndThrow(EXCEPTION_EMPTY_PATH);
        }
        this.xattr = false;
    }


    /**
     * {@inheritDoc}
     * @return {@link KeyValueHandler#OP_SUB_GET}
     */
    @Override
    public byte opcode() {
        return KeyValueHandler.OP_SUB_GET;
    }

    public boolean xattr() {
        return this.xattr;
    }

    public void xattr(boolean xattr) {
        this.xattr = xattr;
    }

    public boolean accessDeleted() {
        return this.accessDeleted;
    }

    public void accessDeleted(boolean accessDeleted) {
        if (!this.xattr && accessDeleted) {
            throw new IllegalArgumentException("Invalid to access document attributes with access deleted. It can be used only with xattr.");
        }
        this.accessDeleted = accessDeleted;
    }
}
