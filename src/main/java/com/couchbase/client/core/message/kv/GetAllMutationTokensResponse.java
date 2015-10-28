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

package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

import java.util.Arrays;

/**
 * Represents response to {@link GetAllMutationTokensRequest}.
 *
 * @author Sergey Avseyev
 * @since 1.2.2
 */
@InterfaceStability.Experimental
public class GetAllMutationTokensResponse extends AbstractKeyValueResponse {
    final MutationToken[] mutationTokens;

    public GetAllMutationTokensResponse(final MutationToken[] mutationTokens, final ResponseStatus status,
                                        final short serverStatusCode, final String bucket,
                                        final CouchbaseRequest request) {
        super(status, serverStatusCode, bucket, null, request);
        this.mutationTokens = mutationTokens;
    }

    /**
     * @return list of mutation tokens for partitions in requested state.
     */
    public MutationToken[] mutationTokens() {
        return mutationTokens;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("GetAllMutationTokensResponse{")
                .append("bucket='").append(bucket()).append('\'')
                .append(", status=").append(status()).append(" (").append(serverStatusCode()).append(')')
                .append(", mutationTokens=").append(Arrays.toString(mutationTokens))
                .append(", request=").append(request())
                .append('}').toString();
    }
}
