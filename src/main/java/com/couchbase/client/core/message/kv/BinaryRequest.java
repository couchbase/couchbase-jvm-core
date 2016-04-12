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

import com.couchbase.client.core.message.CouchbaseRequest;

/**
 * Common marker interface for all {@link BinaryRequest}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface BinaryRequest extends CouchbaseRequest {

    /**
     * The key of the document.
     *
     * @return the key of the document, if set.
     */
    String key();

    /**
     * The raw byte representation of the key.
     *
     * @return the bytes of the key.
     */
    byte[] keyBytes();

    /**
     * The partition (vbucket) to use for this request.
     *
     * @return the partition to use.
     */
    short partition();

    /**
     * Set the partition ID.
     *
     * @param id the id of the partition.
     * @return the {@link BinaryRequest} for proper chaining.
     */
    BinaryRequest partition(short id);

    /**
     * A opaque value representing this request.
     *
     * @return an automatically generated opaque value.
     */
    int opaque();
}
