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

import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.logging.RedactionLevel;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.tracing.ThresholdLogReporter;
import io.netty.util.CharsetUtil;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.util.concurrent.atomic.AtomicInteger;

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
    private static final AtomicInteger GLOBAL_OPAQUE = new AtomicInteger(0);

    protected static final short DEFAULT_PARTITION = -1;

    /**
     * Cache the redaction level for performance.
     */
    private static final RedactionLevel REDACTION_LEVEL = CouchbaseLoggerFactory.getRedactionLevel();

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
     * Indicates whether this request has seen a not my vbucket response from KV already.
     */
    private volatile boolean seenNotMyVbucket = false;

    /**
     * Creates a new {@link AbstractKeyValueRequest}.
     *
     * @param key      the key of the document.
     * @param bucket   the bucket of the document.
     */
    protected AbstractKeyValueRequest(String key, String bucket) {
        this(key, bucket, null, null, AsyncSubject.<CouchbaseResponse>create());
    }

    /**
     * Creates a new {@link AbstractKeyValueRequest}.
     *
     * @param key      the key of the document.
     * @param bucket   the bucket of the document.
     * @param password the password for the bucket.
     */
    protected AbstractKeyValueRequest(String key, String bucket, String password) {
        this(key, bucket, bucket, password, AsyncSubject.<CouchbaseResponse>create());
    }

    /**
     * Creates a new {@link AbstractKeyValueRequest}.
     *
     * @param key      the key of the document.
     * @param bucket   the bucket of the document.
     * @param username the user authorized for bucket access.
     * @param password the password for the user.
     */
    protected AbstractKeyValueRequest(String key, String bucket, String username, String password) {
        this(key, bucket, username, password, AsyncSubject.<CouchbaseResponse>create());
    }

    /**
     * Creates a new {@link AbstractKeyValueRequest}.
     *
     * @param key        the key of the document.
     * @param bucket     the bucket of the document.
     * @param username   the user authorized for bucket access.
     * @param password   the optional password of the user.
     * @param observable the observable which receives responses.
     */
    protected AbstractKeyValueRequest(String key, String bucket, String username, String password,
                                      Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        super(bucket, username, password, observable);
        this.key = key;
        this.keyBytes = key == null || key.isEmpty() ? new byte[] {} : key.getBytes(CharsetUtil.UTF_8);
        opaque = GLOBAL_OPAQUE.getAndIncrement();
    }

    @Override
    protected void afterSpanSet(Span span) {
        span.setTag(Tags.PEER_SERVICE.getKey(), ThresholdLogReporter.SERVICE_KV);
        if (REDACTION_LEVEL == RedactionLevel.NONE) {
            span.setTag("couchbase.document_key", key);
        }
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

    @Override
    public String operationId() {
        return "0x" + Integer.toHexString(opaque);
    }

    @Override
    public boolean hasSeenNotMyVbucket() {
        return seenNotMyVbucket;
    }

    @Override
    public void sawNotMyVbucket() {
        seenNotMyVbucket = true;
    }
}
