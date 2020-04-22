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
import com.couchbase.client.deps.io.netty.handler.codec.memcache.AbstractMemcacheObject;

/**
 * Default implementation of a {@link BinaryMemcacheMessage}.
 */
public abstract class AbstractBinaryMemcacheMessage
    extends AbstractMemcacheObject
    implements BinaryMemcacheMessage {

    /**
     * Contains the optional key.
     */
    private byte[] key;

    /**
     * Contains the optional extras.
     */
    private ByteBuf extras;

    /**
     * Contains the optional framing extras.
     */
    private ByteBuf framingExtras;

    private byte magic;
    private byte opcode;
    private short keyLength;
    private byte extrasLength;
    private byte framingExtrasLength;
    private byte dataType;
    private int totalBodyLength;
    private int opaque;
    private long cas;

    /**
     * Create a new instance with all properties set.
     *
     * @param key    the message key.
     * @param extras the message extras.
     */
    protected AbstractBinaryMemcacheMessage(byte[] key, ByteBuf extras) {
        this.key = key;
        this.extras = extras;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public ByteBuf getExtras() {
        return extras;
    }

    @Override
    public BinaryMemcacheMessage setKey(byte[] key) {
        this.key = key;
        return this;
    }

    @Override
    public BinaryMemcacheMessage setExtras(ByteBuf extras) {
        this.extras = extras;
        return this;
    }

    @Override
    public byte getMagic() {
        return magic;
    }

    @Override
    public BinaryMemcacheMessage setMagic(byte magic) {
        this.magic = magic;
        return this;
    }

    @Override
    public long getCAS() {
        return cas;
    }

    @Override
    public BinaryMemcacheMessage setCAS(long cas) {
        this.cas = cas;
        return this;
    }

    @Override
    public int getOpaque() {
        return opaque;
    }

    @Override
    public BinaryMemcacheMessage setOpaque(int opaque) {
        this.opaque = opaque;
        return this;
    }

    @Override
    public int getTotalBodyLength() {
        return totalBodyLength;
    }

    @Override
    public BinaryMemcacheMessage setTotalBodyLength(int totalBodyLength) {
        this.totalBodyLength = totalBodyLength;
        return this;
    }

    @Override
    public byte getDataType() {
        return dataType;
    }

    @Override
    public BinaryMemcacheMessage setDataType(byte dataType) {
        this.dataType = dataType;
        return this;
    }

    @Override
    public byte getExtrasLength() {
        return extrasLength;
    }

    @Override
    public BinaryMemcacheMessage setExtrasLength(byte extrasLength) {
        this.extrasLength = extrasLength;
        return this;
    }

    @Override
    public short getKeyLength() {
        return keyLength;
    }

    @Override
    public BinaryMemcacheMessage setKeyLength(short keyLength) {
        this.keyLength = keyLength;
        return this;
    }

    @Override
    public byte getOpcode() {
        return opcode;
    }

    @Override
    public BinaryMemcacheMessage setOpcode(byte opcode) {
        this.opcode = opcode;
        return this;
    }

    @Override
    public ByteBuf getFramingExtras() {
        return framingExtras;
    }

    @Override
    public BinaryMemcacheMessage setFramingExtras(ByteBuf framingExtras) {
        this.framingExtras = framingExtras;
        return this;
    }

    @Override
    public byte getFramingExtrasLength() {
        return framingExtrasLength;
    }

    @Override
    public BinaryMemcacheMessage setFramingExtrasLength(byte framingExtrasLength) {
        this.framingExtrasLength = framingExtrasLength;
        return this;
    }

    @Override
    public int refCnt() {
        if (extras != null) {
            if (framingExtras != null && (framingExtras.refCnt() != extras.refCnt())) {
                throw new IllegalStateException("framing and extras have a different refCnt, ambiguous!");
            }
            return extras.refCnt();
        }
        if (framingExtras != null) {
            return framingExtras.refCnt();
        }
        return 1;
    }

    @Override
    public BinaryMemcacheMessage retain() {
        if (extras != null) {
            extras.retain();
        }
        if (framingExtras != null) {
            framingExtras.retain();
        }
        return this;
    }

    @Override
    public BinaryMemcacheMessage retain(int increment) {
        if (extras != null) {
            extras.retain(increment);
        }
        if (framingExtras != null) {
            framingExtras.retain(increment);
        }
        return this;
    }

    @Override
    public BinaryMemcacheMessage touch() {
        if (extras != null) {
            extras.touch();
        }
        if (framingExtras != null) {
            framingExtras.touch();
        }
        return this;
    }

    @Override
    public BinaryMemcacheMessage touch(Object hint) {
        if (extras != null) {
            extras.touch(hint);
        }
        if (framingExtras != null) {
            framingExtras.touch(hint);
        }
        return this;
    }

    @Override
    public boolean release() {
        boolean result = false;
        if (extras != null) {
            result = extras.release();
        }
        if (framingExtras != null) {
            result = framingExtras.release();
        }
        return result;
    }

    @Override
    public boolean release(int decrement) {
        boolean result = false;
        if (extras != null) {
            result = extras.release(decrement);
        }
        if (framingExtras != null) {
            result = framingExtras.release(decrement);
        }
        return result;
    }

}
