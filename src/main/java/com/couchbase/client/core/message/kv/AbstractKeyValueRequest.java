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

import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import io.netty.util.CharsetUtil;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

/**
 * Default implementation of a {@link BinaryRequest}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public abstract class AbstractKeyValueRequest extends AbstractCouchbaseRequest implements BinaryRequest {

    /**
     * The opaque identifier used in the binary protocol to track requests/responses.
     *
     * No overflow control is applied, since once it overflows it starts with negative values again.
     */
    private static volatile int GLOBAL_OPAQUE = 0;

    protected static final short DEFAULT_PARTITION = -1;

    /**
     * The key of the document, should be null if not tied to any.
     */
    private final byte[] keyBytes;
    private final String key;

    /**
     * The partition (vbucket) of the document.
     */
    private short partition = DEFAULT_PARTITION;

    private final int opaque;

    /**
     * Creates a new {@link AbstractKeyValueRequest}.
     *
     * @param key      the key of the document.
     * @param bucket   the bucket of the document.
     * @param password the optional password of the bucket.
     */
    protected AbstractKeyValueRequest(String key, String bucket, String password) {
        this(key, bucket, password, AsyncSubject.<CouchbaseResponse>create());
    }

    /**
     * Creates a new {@link AbstractKeyValueRequest}.
     *
     * @param key        the key of the document.
     * @param bucket     the bucket of the document.
     * @param password   the optional password of the bucket.
     * @param observable the observable which receives responses.
     */
    protected AbstractKeyValueRequest(String key, String bucket, String password,
                                      Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        super(bucket, password, observable);
        this.key = key;
        this.keyBytes = key == null || key.isEmpty() ? new byte[] {} : key.getBytes(CharsetUtil.UTF_8);
        opaque = GLOBAL_OPAQUE++;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public byte[] keyBytes() {
        return keyBytes;
    }

    @Override
    public short partition() {
        if (partition == -1) {
            throw new IllegalStateException("Partition requested but not set beforehand");
        }
        return partition;
    }

    @Override
    public BinaryRequest partition(short partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Partition must be larger than or equal to zero");
        }
        this.partition = partition;
        return this;
    }

    @Override
    public int opaque() {
        return opaque;
    }
}
