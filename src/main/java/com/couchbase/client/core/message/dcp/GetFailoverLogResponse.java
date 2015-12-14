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

package com.couchbase.client.core.message.dcp;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

import java.util.List;

/**
 * @author Sergey Avseyev
 */
public class GetFailoverLogResponse extends AbstractDCPResponse {
    private final List<FailoverLogEntry> failoverLog;

    /**
     * Creates {@link GetFailoverLogResponse}.
     *
     * @param status      the status of the response.
     * @param failoverLog the list of failover log entries or null if response status is not success
     * @param request
     */
    public GetFailoverLogResponse(final ResponseStatus status, final List<FailoverLogEntry> failoverLog,
                                  final CouchbaseRequest request) {
        super(status, request);
        this.failoverLog = failoverLog;
    }

    public List<FailoverLogEntry> failoverLog() {
        return failoverLog;
    }
}
