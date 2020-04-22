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
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderResult;

/**
 * The {@link MemcacheContent} which signals the end of the content batch.
 * <p></p>
 * Note that by design, even when no content is emitted by the protocol, an
 * empty {@link LastMemcacheContent} is issued to make the upstream parsing
 * easier.
 */
public interface LastMemcacheContent extends MemcacheContent {

    LastMemcacheContent EMPTY_LAST_CONTENT = new LastMemcacheContent() {

        @Override
        public LastMemcacheContent copy() {
            return EMPTY_LAST_CONTENT;
        }

        @Override
        public LastMemcacheContent retain(int increment) {
            return this;
        }

        @Override
        public LastMemcacheContent retain() {
            return this;
        }

        @Override
        public LastMemcacheContent duplicate() {
            return this;
        }

        @Override
        public ByteBuf content() {
            return Unpooled.EMPTY_BUFFER;
        }

        @Override
        public DecoderResult getDecoderResult() {
            return DecoderResult.SUCCESS;
        }

        @Override
        public void setDecoderResult(DecoderResult result) {
            throw new UnsupportedOperationException("read only");
        }

        @Override
        public int refCnt() {
            return 1;
        }

        @Override
        public boolean release() {
            return false;
        }

        @Override
        public boolean release(int decrement) {
            return false;
        }

        @Override
        public LastMemcacheContent retainedDuplicate() {
            return this;
        }

        @Override
        public LastMemcacheContent replace(ByteBuf content) {
            throw new UnsupportedOperationException("replace");
        }

        @Override
        public LastMemcacheContent touch() {
            return this;
        }

        @Override
        public LastMemcacheContent touch(Object hint) {
            return this;
        }
    };

    @Override
    LastMemcacheContent copy();

    @Override
    LastMemcacheContent retain(int increment);

    @Override
    LastMemcacheContent retain();

    @Override
    LastMemcacheContent duplicate();
}
