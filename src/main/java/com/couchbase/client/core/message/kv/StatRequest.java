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

import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.utils.NetworkAddress;
import rx.subjects.ReplaySubject;

/**
 * @author Sergey Avseyev
 * @since 1.2.1
 */
public class StatRequest extends AbstractKeyValueRequest {
    private final NetworkAddress hostname;

    public StatRequest(final String key, final NetworkAddress hostname, final String bucket) {
        super(key, bucket, null, null, ReplaySubject.<CouchbaseResponse>create());
        this.hostname = hostname;
    }

    public void add(final StatResponse response) {
        if (response.key() == null) {
            // Skip NULL-terminator for successful response
            if (!response.status().isSuccess()) {
                observable().onNext(response);
            }
            observable().onCompleted();
        } else {
            observable().onNext(response);
        }
    }

    public NetworkAddress hostname() {
        return hostname;
    }

    @Override
    public short partition() {
        return DEFAULT_PARTITION;
    }
}
