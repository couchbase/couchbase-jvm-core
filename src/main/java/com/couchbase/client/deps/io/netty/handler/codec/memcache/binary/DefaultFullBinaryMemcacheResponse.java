/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.couchbase.client.deps.io.netty.handler.codec.memcache.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.couchbase.client.core.lang.backport.java.util.Objects.requireNonNull;

/**
 * The default implementation of a {@link FullBinaryMemcacheResponse}.
 */
public class DefaultFullBinaryMemcacheResponse extends DefaultBinaryMemcacheResponse
    implements FullBinaryMemcacheResponse {

    private ByteBuf content;

    /**
     * Create a new {@link DefaultFullBinaryMemcacheResponse} with the header, key and extras.
     *
     * @param key    the key to use.
     * @param extras the extras to use.
     */
    public DefaultFullBinaryMemcacheResponse(byte[] key, ByteBuf extras) {
        this(key, extras, Unpooled.buffer(0));
    }

    /**
     * Create a new {@link DefaultFullBinaryMemcacheResponse} with the header, key, extras and content.
     *
     * @param key     the key to use.
     * @param extras  the extras to use.
     * @param content the content of the full request.
     */
    public DefaultFullBinaryMemcacheResponse(byte[] key, ByteBuf extras, ByteBuf content) {
        super(key, extras);
        this.content = requireNonNull(content, "Supplied content is null.");
    }

    @Override
    public ByteBuf content() {
        return content;
    }

    @Override
    public int refCnt() {
        return content.refCnt();
    }

    @Override
    public FullBinaryMemcacheResponse retain() {
        super.retain();
        content.retain();
        return this;
    }

    @Override
    public FullBinaryMemcacheResponse retain(int increment) {
        super.retain(increment);
        content.retain(increment);
        return this;
    }

    @Override
    public boolean release() {
        super.release();
        return content.release();
    }

    @Override
    public boolean release(int decrement) {
        super.release(decrement);
        return content.release(decrement);
    }

    @Override
    public FullBinaryMemcacheResponse copy() {
        ByteBuf extras = getExtras();
        if (extras != null) {
            extras = extras.copy();
        }
        return new DefaultFullBinaryMemcacheResponse(getKey(), extras, content().copy());
    }

    @Override
    public FullBinaryMemcacheResponse duplicate() {
        return replace(content().duplicate());
    }

    @Override
    public FullBinaryMemcacheResponse setContent(ByteBuf content) {
        this.content = content;
        return this;
    }

    @Override
    public FullBinaryMemcacheResponse retainedDuplicate() {
        return duplicate().retain();
    }

    @Override
    public FullBinaryMemcacheResponse replace(ByteBuf content) {
        ByteBuf extras = getExtras();
        if (extras != null) {
            extras = extras.duplicate();
        }
        return new DefaultFullBinaryMemcacheResponse(getKey(), extras, content);
    }

    @Override
    public FullBinaryMemcacheResponse touch() {
        super.touch();
        return this;
    }

    @Override
    public FullBinaryMemcacheResponse touch(Object hint) {
        super.touch(hint);
        return this;
    }
}
