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
