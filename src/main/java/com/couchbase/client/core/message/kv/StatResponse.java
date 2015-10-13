/*
 * Copyright (c) 2015 Couchbase, Inc.
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
