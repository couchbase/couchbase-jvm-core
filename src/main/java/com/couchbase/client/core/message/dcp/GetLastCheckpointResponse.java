/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.message.dcp;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * @author Sergey Avseyev
 * @since 1.2.6
 */
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
