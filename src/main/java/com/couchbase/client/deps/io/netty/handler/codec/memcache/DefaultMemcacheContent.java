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
package com.couchbase.client.deps.io.netty.handler.codec.memcache;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.StringUtil;

import static com.couchbase.client.core.lang.backport.java.util.Objects.requireNonNull;

/**
 * The default {@link MemcacheContent} implementation.
 */
public class DefaultMemcacheContent extends AbstractMemcacheObject implements MemcacheContent {

    private final ByteBuf content;

    /**
     * Creates a new instance with the specified content.
     */
    public DefaultMemcacheContent(ByteBuf content) {
        this.content = requireNonNull(content, "Content cannot be null.");
    }

    @Override
    public ByteBuf content() {
        return content;
    }

    @Override
    public MemcacheContent copy() {
        return new DefaultMemcacheContent(content.copy());
    }

    @Override
    public MemcacheContent duplicate() {
        return new DefaultMemcacheContent(content.duplicate());
    }

    @Override
    public int refCnt() {
        return content.refCnt();
    }

    @Override
    public MemcacheContent retain() {
        content.retain();
        return this;
    }

    @Override
    public MemcacheContent retain(int increment) {
        content.retain(increment);
        return this;
    }

    @Override
    public MemcacheContent retainedDuplicate() {
        return duplicate().retain();
    }

    @Override
    public MemcacheContent touch() {
        content.touch();
        return this;
    }

    @Override
    public MemcacheContent touch(Object o) {
        content.touch(o);
        return this;
    }

    @Override
    public MemcacheContent replace(ByteBuf content) {
        return new DefaultMemcacheContent(content);
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
        return StringUtil.simpleClassName(this)
            + "(data: " + content() + ", getDecoderResult: " + getDecoderResult() + ')';
    }
}
