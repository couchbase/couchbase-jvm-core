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

package com.couchbase.client.core.message.kv.subdoc.multi;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.AbstractKeyValueResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiMutationRequest;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * The response for a {@link BinarySubdocMultiMutationRequest}.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
//TODO make public again once mutateIn protocol has been stabilized
class MultiMutationResponse extends AbstractKeyValueResponse {

    private final long cas;

    private final MutationToken mutationToken;
    private final int firstErrorIndex;
    private final ResponseStatus firstErrorStatus;

    /**
     * Creates a partially successful {@link MultiMutationResponse}. The status, expected to be
     * {@link ResponseStatus#SUBDOC_MULTI_PATH_FAILURE}, denotes that at least one {@link MutationCommand} failed.
     *
     * @param status the status of the request (SUBDOC_MULTI_PATH_FAILURE).
     * @param serverStatusCode the status code of the whole request.
     * @param bucket the bucket on which the request happened.
     * @param firstErrorIndex the zero-based index of the first {@link MutationCommand} that failed (in case failure is
     *                        due to one or more commands).
     * @param firstErrorStatusCode the status code for the first {@link MutationCommand} that failed (in case failure is
     *                        due to one or more commands).
     * @param request the original {@link BinarySubdocMultiMutationRequest}.
     * @param cas the CAS value of the document after mutations.
     * @param mutationToken the {@link MutationToken} of the document after mutations, if available. Null otherwise.
     */
    public MultiMutationResponse(ResponseStatus status, short serverStatusCode, String bucket, int firstErrorIndex, short firstErrorStatusCode,
                                BinarySubdocMultiMutationRequest request, long cas, MutationToken mutationToken) { //do cas and muto make sense here?
        super(status, serverStatusCode, bucket, Unpooled.EMPTY_BUFFER, request);
        this.cas = cas;
        this.mutationToken = mutationToken;
        this.firstErrorIndex = firstErrorIndex;
        if (firstErrorIndex == -1) {
            this.firstErrorStatus = ResponseStatus.FAILURE;
        } else {
            this.firstErrorStatus = ResponseStatusConverter.fromBinary(firstErrorStatusCode);
        }
    }

    /**
     * Creates a unsuccessful {@link MultiMutationResponse}.
     *
     * First error index is set to -1 and first error status is set to {@link ResponseStatus#FAILURE}.
     *
     * @param status the failed status of the request.
     * @param serverStatusCode the status code of the whole request.
     * @param bucket the bucket on which the request happened.
     * @param request the original {@link BinarySubdocMultiMutationRequest}.
     * @param cas the CAS value of the document after mutations.
     * @param mutationToken the {@link MutationToken} of the document after mutations, if available. Null otherwise.
     */
    public MultiMutationResponse(ResponseStatus status, short serverStatusCode, String bucket,
                                 BinarySubdocMultiMutationRequest request, long cas, MutationToken mutationToken) { //do cas and muto make sense here?
        super(status, serverStatusCode, bucket, Unpooled.EMPTY_BUFFER, request);
        this.cas = cas;
        this.mutationToken = mutationToken;
        this.firstErrorIndex = -1;
        this.firstErrorStatus = ResponseStatus.FAILURE;
    }

    /**
     * Creates an successful {@link MultiMutationResponse}.
     *
     * @param bucket the bucket on which the request happened.
     * @param request the original {@link BinarySubdocMultiMutationRequest}.
     * @param cas the CAS value of the document after mutations.
     * @param token the {@link MutationToken} of the document after mutations, if available. Null otherwise.
     */
    public MultiMutationResponse(String bucket, BinarySubdocMultiMutationRequest request, long cas, MutationToken token) {
        super(ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(), bucket, Unpooled.EMPTY_BUFFER, request);
        this.cas = cas;
        this.mutationToken = token;
        this.firstErrorIndex = -1;
        this.firstErrorStatus = ResponseStatus.SUCCESS;
    }

    @Override
    public BinarySubdocMultiMutationRequest request() {
        return (BinarySubdocMultiMutationRequest) super.request();
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

    /**
     * @return the zero-based index of the first {@link MutationCommand} that failed, or -1 if none failed or the whole
     * request failed due to another factor (eg. key doesn't exist).
     */
    public int firstErrorIndex() {
        return firstErrorIndex;
    }

    /**
     * @return the {@link ResponseStatus} of the first {@link MutationCommand} that failed,
     * {@link ResponseStatus#SUCCESS} if none failed
     * or {@link ResponseStatus#FAILURE} if the whole request failed due to another factor (eg. key doesn't exist).
     */
    public ResponseStatus firstErrorStatus() {
        return firstErrorStatus;
    }
}
