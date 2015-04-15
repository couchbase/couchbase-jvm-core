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

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public abstract class AbstractKeyValueResponse extends AbstractCouchbaseResponse implements BinaryResponse {

    private final ByteBuf content;
    private final String bucket;
    private final short serverStatusCode;

    protected AbstractKeyValueResponse(ResponseStatus status, short serverStatusCode, String bucket, ByteBuf content,
                                       CouchbaseRequest request) {
        super(status, request);
        this.content = content == null ? Unpooled.EMPTY_BUFFER : content;
        this.bucket = bucket;
        this.serverStatusCode = serverStatusCode;
    }

    @Override
    public ByteBuf content() {
        return content;
    }

    @Override
    public String bucket() {
        return bucket;
    }

    @Override
    public short serverStatusCode() {
        return serverStatusCode;
    }

    @Override
    public int refCnt() {
        return content.refCnt();
    }

    @Override
    public BinaryResponse retain() {
        content.retain();
        return this;
    }

    @Override
    public BinaryResponse retain(int increment) {
        content.retain(increment);
        return this;
    }

    @Override
    public boolean release() {
        return content.release();
    }

    @Override
    public boolean release(int decrement) {
        return content.release(decrement);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BinaryResponse{");
        sb.append("bucket='").append(bucket).append('\'');
        sb.append(", status=").append(status()).append(" (").append(serverStatusCode()).append(')');
        sb.append(", request=").append(request());
        sb.append(", content=").append(content);
        sb.append('}');
        return sb.toString();
    }
}
