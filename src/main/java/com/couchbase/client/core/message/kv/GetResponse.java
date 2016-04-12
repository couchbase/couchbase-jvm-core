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
 * Represents a response to a {@link GetRequest}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class GetResponse extends AbstractKeyValueResponse {

    private final long cas;
    private final int flags;

    public GetResponse(final ResponseStatus status, final short serverStatusCode, final long cas, final int flags,
                       final String bucket, final ByteBuf content, final CouchbaseRequest request) {
        super(status, serverStatusCode, bucket, content, request);
        this.cas = cas;
        this.flags = flags;
    }

    public long cas() {
        return cas;
    }

    public int flags() {
        return flags;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("GetResponse{")
                .append("bucket='").append(bucket()).append('\'')
                .append(", status=").append(status()).append(" (").append(serverStatusCode()).append(')')
                .append(", cas=").append(cas)
                .append(", flags=").append(flags)
                .append(", request=").append(request())
                .append(", content=").append(content().toString(CharsetUtil.UTF_8))
                .append('}').toString();
    }
}
