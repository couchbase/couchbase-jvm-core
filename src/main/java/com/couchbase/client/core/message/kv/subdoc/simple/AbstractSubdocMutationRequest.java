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
        this.createIntermediaryPath = false;
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
    public long cas() {
        return this.cas;
    }
}
