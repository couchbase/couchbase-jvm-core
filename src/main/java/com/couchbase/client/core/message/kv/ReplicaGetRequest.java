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

/**
 * Fetch a document from one or more and/or active nodes replicas.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ReplicaGetRequest extends AbstractKeyValueRequest {

    private final short replica;

    public ReplicaGetRequest(String key, String bucket, short replica) {
        super(key, bucket);
        this.replica = replica;
    }

    public short replica() {
        return replica;
    }
}
