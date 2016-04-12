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
 * Fetch a document from the cluster and return it if found.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class GetRequest extends AbstractKeyValueRequest {

    private final boolean lock;
    private final boolean touch;
    private final int expiry;

    public GetRequest(final String key, final String bucket) {
        this(key, bucket, false, false, 0);
    }

    public GetRequest(final String key, final String bucket, final boolean lock, final boolean touch, final int expiry) {
        super(key, bucket, null);
        if (lock && touch) {
            throw new IllegalArgumentException("Locking and touching in the same request is not supported.");
        }
        if (!lock && !touch && expiry > 0) {
            throw new IllegalArgumentException("Expiry/Lock time cannot be set for a regular get request.");
        }
        this.lock = lock;
        this.touch = touch;
        this.expiry = expiry;
    }

    public boolean lock() {
        return lock;
    }

    public boolean touch() {
        return touch;
    }

    public int expiry() {
        return expiry;
    }
}
