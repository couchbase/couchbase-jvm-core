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
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * @author Sergey Avseyev
 * @since 1.2.1
 */
public class StatResponse extends AbstractKeyValueResponse {
    private final String hostname;
    private final String key;
    private final String value;

    public StatResponse(ResponseStatus status, short serverStatusCode, String hostname, String key, String value,
                        String bucket, CouchbaseRequest request) {
        super(status, serverStatusCode, bucket, null, request);
        this.hostname = hostname;
        this.key = key;
        this.value = value;
    }

    public String hostname() {
        return hostname;
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("StatResponse{")
                .append("bucket='").append(bucket()).append('\'')
                .append(", status=").append(status()).append(" (").append(serverStatusCode()).append(')')
                .append(", hostname=").append(hostname)
                .append(", key=").append(key)
                .append(", value=").append(value)
                .append(", request=").append(request())
                .append('}').toString();
    }

}
