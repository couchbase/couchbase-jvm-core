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
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.AbstractKeyValueResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMutationRequest;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocRequest;
import io.netty.buffer.ByteBuf;

/**
 * A general-purpose response suitable for most "simple" sub-document operations ({@link BinarySubdocRequest} and
 * {@link BinarySubdocMutationRequest}), as opposed to "multi-specification" sub-document operations.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class SimpleSubdocResponse extends AbstractKeyValueResponse {

    private final long cas;

    private final MutationToken mutationToken;

    public SimpleSubdocResponse(ResponseStatus status, short serverStatusCode, String bucket, ByteBuf content,
                                BinarySubdocRequest request, long cas, MutationToken mutationToken) {
        super(status, serverStatusCode, bucket, content, request);
        this.cas = cas;
        this.mutationToken = mutationToken;
    }

    @Override
    public BinarySubdocRequest request() {
        return (BinarySubdocRequest) super.request();
    }

    /**
     * @return the CAS value of the whole document in case a mutation was applied.
     */
    public long cas() {
        return this.cas;
    }

    /**
     * @return the {@link MutationToken} corresponding to a mutation of the document, if it was mutated and tokens are activated.
     */
    public MutationToken mutationToken() {
        return mutationToken;
    }
}
