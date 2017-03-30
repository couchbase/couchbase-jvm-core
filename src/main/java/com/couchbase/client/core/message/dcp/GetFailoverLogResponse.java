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

import java.util.List;

/**
 * @author Sergey Avseyev
 */
@Deprecated
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
