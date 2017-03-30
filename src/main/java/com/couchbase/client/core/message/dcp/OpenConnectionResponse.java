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

import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;

@Deprecated
public class OpenConnectionResponse extends AbstractCouchbaseResponse {
    private final DCPConnection connection;

    public OpenConnectionResponse(DCPConnection connection, ResponseStatus status) {
        super(status, null);
        this.connection = connection;
    }

    public DCPConnection connection() {
        return connection;
    }
}
