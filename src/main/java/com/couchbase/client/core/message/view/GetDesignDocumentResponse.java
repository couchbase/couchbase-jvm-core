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
package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

public class GetDesignDocumentResponse extends AbstractCouchbaseResponse implements ReferenceCounted {

    private final String name;
    private final boolean development;
    private final ByteBuf content;

    public GetDesignDocumentResponse(String name, boolean development, ByteBuf content, ResponseStatus status, CouchbaseRequest request) {
        super(status, request);
        if (content == null) {
            throw new IllegalArgumentException("Content cannot be null. Consider using an empty buffer instead.");
        }
        this.name = name;
        this.development = development;
        this.content = content;
    }

    public String name() {
        return name;
    }

    public boolean development() {
        return development;
    }

    public ByteBuf content() {
        return content;
    }

    @Override
    public int refCnt() {
        return content.refCnt();
    }

    @Override
    public GetDesignDocumentResponse retain() {
        content.retain();
        return this;
    }

    @Override
    public GetDesignDocumentResponse retain(int increment) {
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
}
