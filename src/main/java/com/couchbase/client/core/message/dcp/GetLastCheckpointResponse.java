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

package com.couchbase.client.core.message.dcp;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * @author Sergey Avseyev
 * @since 1.2.6
 */
@Deprecated
public class GetLastCheckpointResponse extends AbstractDCPResponse {
    private final long sequenceNumber;

    /**
     * Creates {@link GetLastCheckpointResponse}.
     *
     * @param status         the status of the response.
     * @param sequenceNumber the most recent sequence number.
     * @param request
     */
    public GetLastCheckpointResponse(final ResponseStatus status, final long sequenceNumber,
                                     final CouchbaseRequest request) {
        super(status, request);
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Each mutation that occurs on a vBucket is assigned a number, which strictly
     * increases as events are assigned sequence numbers, that can be used to order
     * that event against other mutations within the same vBucket.
     */
    public long sequenceNumber() {
        return sequenceNumber;
    }
}
