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
