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

public class RemoveDesignDocumentResponse extends AbstractCouchbaseResponse implements ReferenceCounted {

    private final ByteBuf content;

    public RemoveDesignDocumentResponse(ResponseStatus status, ByteBuf content, CouchbaseRequest request) {
        super(status, request);
        if (content == null) {
            throw new IllegalArgumentException("Content cannot be null. Consider using an empty buffer instead.");
        }
        this.content = content;
    }

    public ByteBuf content() {
        return content;
    }

    @Override
    public int refCnt() {
        return content.refCnt();
    }

    @Override
    public RemoveDesignDocumentResponse retain() {
        content.retain();
        return this;
    }

    @Override
    public RemoveDesignDocumentResponse retain(int increment) {
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
    public RemoveDesignDocumentResponse touch() {
        content.touch();
        return this;
    }

    @Override
    public RemoveDesignDocumentResponse touch(Object hint) {
        content.touch(hint);
        return this;
    }
}
