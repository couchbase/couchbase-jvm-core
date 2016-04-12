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

package com.couchbase.client.core.message.kv.subdoc.multi;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.AbstractKeyValueResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiMutationRequest;
import io.netty.buffer.Unpooled;

import java.util.Collections;
import java.util.List;

/**
 * The response for a {@link BinarySubdocMultiMutationRequest}. Error status other than
 * {@link ResponseStatus#SUBDOC_MULTI_PATH_FAILURE} denote an error at document level, while the later denotes
 * an error at sub-document level. In this case, look at {@link #firstErrorStatus()} to determine which sub-document
 * error happened.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class MultiMutationResponse extends AbstractKeyValueResponse {

    private final long cas;
    private final MutationToken mutationToken;
    private final List<MultiResult<Mutation>> responses;
    private final int firstErrorIndex;
    private final ResponseStatus firstErrorStatus;

    /**
     * Creates a {@link MultiMutationResponse} that failed at subdocument level. The status, expected to be
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
        this.responses = Collections.emptyList();
    }

    /**
     * Creates a unsuccessful {@link MultiMutationResponse} that failed at document level.
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
        this.responses = Collections.emptyList();
    }

    /**
     * Creates an successful {@link MultiMutationResponse}.
     *
     * @param bucket the bucket on which the request happened.
     * @param request the original {@link BinarySubdocMultiMutationRequest}.
     * @param cas the CAS value of the document after mutations.
     * @param token the {@link MutationToken} of the document after mutations, if available. Null otherwise.
     * @param responses the list of {@link MultiResult MultiResult&lt;Mutation&gt;} for each command. Some may include a value.
     */
    public MultiMutationResponse(String bucket, BinarySubdocMultiMutationRequest request, long cas, MutationToken token,
                                 List<MultiResult<Mutation>> responses) {
        super(ResponseStatus.SUCCESS, KeyValueStatus.SUCCESS.code(), bucket, Unpooled.EMPTY_BUFFER, request);
        this.cas = cas;
        this.mutationToken = token;
        this.firstErrorIndex = -1;
        this.firstErrorStatus = ResponseStatus.SUCCESS;
        this.responses = responses;
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

    /**
     * @return a list of {@link MultiResult MultiResult&lt;Mutation&gt;}, giving the individual result of each {@link MutationCommand}.
     */
    public List<MultiResult<Mutation>> responses() {
        return responses;
    }
}
