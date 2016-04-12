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
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.AbstractKeyValueResponse;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiLookupRequest;
import io.netty.buffer.Unpooled;

import java.util.List;

/**
 * The response for a {@link BinarySubdocMultiLookupRequest}.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class MultiLookupResponse extends AbstractKeyValueResponse {

    private final List<MultiResult<Lookup>> responses;

    public MultiLookupResponse(ResponseStatus status, short serverStatusCode, String bucket, List<MultiResult<Lookup>> responses,
                               BinarySubdocMultiLookupRequest request) {
        super(status, serverStatusCode, bucket, Unpooled.EMPTY_BUFFER, request);
        this.responses = responses;
    }

    @Override
    public BinarySubdocMultiLookupRequest request() {
        return (BinarySubdocMultiLookupRequest) super.request();
    }

    /**
     * @return a list of {@link MultiResult MultiResult&lt;Lookup&gt;}, giving the individual result of each {@link LookupCommand}.
     */
    public List<MultiResult<Lookup>> responses() {
        return responses;
    }
}
