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
 * Request to handle increment/decrement of a counter.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CounterRequest extends AbstractKeyValueRequest {

    private final long initial;
    private final long delta;
    private final int expiry;

    public CounterRequest(String key, long initial, long delta, int expiry, String bucket) {
        super(key, bucket);
        if (initial < 0) {
            throw new IllegalArgumentException("The initial needs to be >= 0");
        }
        this.initial = initial;
        this.delta = delta;
        this.expiry = expiry;
    }

    public long initial() {
        return initial;
    }

    public long delta() {
        return delta;
    }

    public int expiry() {
        return expiry;
    }

}
