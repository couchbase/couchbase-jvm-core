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

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

public class CounterResponse extends AbstractKeyValueResponse {

    private final long value;
    private final long cas;
    private final MutationToken mutationToken;

    public CounterResponse(ResponseStatus status, short serverStatusCode, String bucket, long value, long cas,
        MutationToken mutationToken, CouchbaseRequest request) {
        super(status, serverStatusCode, bucket, null, request);
        this.value = value;
        this.cas = cas;
        this.mutationToken = mutationToken;
    }

    public long value() {
        return value;
    }

    public long cas() {
        return cas;
    }

    public MutationToken mutationToken() {
        return mutationToken;
    }

}
