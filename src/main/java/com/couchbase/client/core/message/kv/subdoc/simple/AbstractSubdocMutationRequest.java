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
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMutationRequest;
import io.netty.buffer.ByteBuf;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

/**
 * Base class for all {@link BinarySubdocMutationRequest}.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public abstract class AbstractSubdocMutationRequest extends AbstractSubdocRequest implements BinarySubdocMutationRequest {

    private final int expiration;

    private final ByteBuf fragment;

    private boolean createIntermediaryPath;

    private boolean xattr;

    private long cas;

    /**
     * Creates a new {@link AbstractSubdocMutationRequest}.
     *
     * @param key      the key of the document.
     * @param path     the subdocument path to consider inside the document.
     * @param fragment   the fragment of valid JSON to mutate into at the site denoted by the path.
     * @param bucket   the bucket of the document.
     * @param expiration the TTL of the whole enclosing document.
     * @param cas the cas value for the operation
     * @throws NullPointerException     if the path is null (see {@link #EXCEPTION_NULL_PATH})
     */
    protected AbstractSubdocMutationRequest(String key, String path, ByteBuf fragment, String bucket,
                                         int expiration, long cas) {
        this(key, path, fragment, bucket, expiration, cas, AsyncSubject.<CouchbaseResponse>create());
    }

    /**
     * Creates a new {@link AbstractSubdocMutationRequest}.
     *
     * @param key        the key of the document.
     * @param path       the subdocument path to consider inside the document.
     * @param fragment   the fragment of valid JSON to mutate into at the site denoted by the path.
     * @param bucket     the bucket of the document.
     * @param expiration the TTL of the whole enclosing document.
     * @param cas the cas value for the operation
     * @param observable the observable which receives responses.
     * @throws NullPointerException     if the path is null (see {@link #EXCEPTION_NULL_PATH})
     */
    protected AbstractSubdocMutationRequest(String key, String path, ByteBuf fragment, String bucket,
                                            int expiration, long cas, Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        super(key, path, bucket, observable, fragment);
        this.expiration = expiration;
        this.fragment = fragment;
        this.cas = cas;
    }

    @Override
    public int expiration() {
        return expiration;
    }

    @Override
    public ByteBuf fragment() {
        return fragment;
    }

    @Override
    public boolean createIntermediaryPath() {
        return this.createIntermediaryPath;
    }

    /**
     * Modifies the request so that it requires the creation of missing intermediary nodes in the path if set to true.
     * @param createIntermediaryPath true if missing intermediary nodes in the path should be created, false if they
     *                               should be considered as errors.
     */
    public void createIntermediaryPath(boolean createIntermediaryPath) {
        this.createIntermediaryPath = createIntermediaryPath;
    }

    @Override
    public boolean xattr() {
        return this.xattr;
    }


    public void xattr(boolean xattr) {
        this.xattr = xattr;
    }

    @Override
    public long cas() {
        return this.cas;
    }
}
