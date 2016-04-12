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
