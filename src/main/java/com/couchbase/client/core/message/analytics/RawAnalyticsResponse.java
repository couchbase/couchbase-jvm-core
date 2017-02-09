/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.core.message.analytics;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

/**
 * The response to a {@link RawAnalyticsRequest}, which only contains the raw JSON payload returned by
 * the Analytics service, as a {@link ByteBuf}.
 *
 * The response is only made available once all of the data has been emitted by the server (no
 * streaming of rows as they come like in {@link GenericAnalyticsResponse}.
 *
 * @author Simon Baslé
 * @since 1.4.3
 */
@InterfaceStability.Uncommitted
@InterfaceAudience.Public
public class RawAnalyticsResponse extends AbstractCouchbaseResponse {

    private final ByteBuf jsonResponse;
    private final int httpStatusCode;
    private final String httpStatusMsg;

    public RawAnalyticsResponse(ResponseStatus status, CouchbaseRequest request, ByteBuf jsonResponse,
        int httpStatusCode, String httpStatusMsg) {
        super(status, request);
        this.jsonResponse = jsonResponse;
        this.httpStatusCode = httpStatusCode;
        this.httpStatusMsg = httpStatusMsg;
    }

    public ByteBuf jsonResponse() {
        return this.jsonResponse;
    }

    public int httpStatusCode() {
        return this.httpStatusCode;
    }

    public String httpStatusMsg() {
        return this.httpStatusMsg;
    }
}
