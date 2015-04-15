/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
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
