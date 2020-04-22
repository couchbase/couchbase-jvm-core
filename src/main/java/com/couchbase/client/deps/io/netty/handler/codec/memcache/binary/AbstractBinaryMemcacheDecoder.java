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

import com.couchbase.client.deps.io.netty.handler.codec.memcache.AbstractMemcacheObjectDecoder;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.DefaultLastMemcacheContent;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.DefaultMemcacheContent;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.LastMemcacheContent;
import com.couchbase.client.deps.io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderResult;

import java.util.List;

import static io.netty.buffer.ByteBufUtil.readBytes;

/**
 * Decoder for both {@link BinaryMemcacheRequest} and {@link BinaryMemcacheResponse}.
 *
 * <p>The difference in the protocols (header) is implemented by the subclasses.</p>
 */
public abstract class AbstractBinaryMemcacheDecoder<M extends BinaryMemcacheMessage>
    extends AbstractMemcacheObjectDecoder {

    public static final int DEFAULT_MAX_CHUNK_SIZE = 8192;

    private final int chunkSize;

    private M currentMessage;
    private int alreadyReadChunkSize;

    private State state = State.READ_HEADER;

    /**
     * Create a new {@link AbstractBinaryMemcacheDecoder} with default settings.
     */
    protected AbstractBinaryMemcacheDecoder() {
        this(DEFAULT_MAX_CHUNK_SIZE);
    }

    /**
     * Create a new {@link AbstractBinaryMemcacheDecoder} with custom settings.
     *
     * @param chunkSize the maximum chunk size of the payload.
     */
    protected AbstractBinaryMemcacheDecoder(int chunkSize) {
        if (chunkSize < 0) {
            throw new IllegalArgumentException("chunkSize must be a positive integer: " + chunkSize);
        }

        this.chunkSize = chunkSize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state) {
            case READ_HEADER:
                try {
                    if (in.readableBytes() < 24) {
                        return;
                    }
                    resetDecoder();

                    currentMessage = decodeHeader(in);
                    state = State.READ_FRAMING;
                } catch (Exception e) {
                    resetDecoder();
                    out.add(invalidMessage(e));
                    return;
                }
                // falls through
            case READ_FRAMING:
                try {
                    byte framingLength = currentMessage.getFramingExtrasLength();
                    if (framingLength > 0) {
                        if (in.readableBytes() < framingLength) {
                            return;
                        }

                        currentMessage.setFramingExtras(readBytes(ctx.alloc(), in, framingLength));
                    }

                    state = State.READ_EXTRAS;
                } catch (Exception e) {
                    resetDecoder();
                    out.add(invalidMessage(e));
                    return;
                }
                // falls through
            case READ_EXTRAS:
                try {
                    byte extrasLength = currentMessage.getExtrasLength();
                    if (extrasLength > 0) {
                        if (in.readableBytes() < extrasLength) {
                            return;
                        }

                        currentMessage.setExtras(readBytes(ctx.alloc(), in, extrasLength));
                    }

                    state = State.READ_KEY;
                } catch (Exception e) {
                    resetDecoder();
                    out.add(invalidMessage(e));
                    return;
                }
                // falls through
            case READ_KEY:
                try {
                    short keyLength = currentMessage.getKeyLength();
                    if (keyLength > 0) {
                        if (in.readableBytes() < keyLength) {
                            return;
                        }

                        byte[] key = new byte[keyLength];
                        in.readBytes(key);
                        currentMessage.setKey(key);
                    }

                    out.add(currentMessage.retain());
                    state = State.READ_CONTENT;
                } catch (Exception e) {
                    resetDecoder();
                    out.add(invalidMessage(e));
                    return;
                }
                // falls through
            case READ_CONTENT:
                try {
                    int valueLength = currentMessage.getTotalBodyLength()
                        - currentMessage.getKeyLength()
                        - currentMessage.getExtrasLength()
                        - currentMessage.getFramingExtrasLength();
                    int toRead = in.readableBytes();
                    if (valueLength > 0) {
                        if (toRead == 0) {
                            return;
                        }

                        if (toRead > chunkSize) {
                            toRead = chunkSize;
                        }

                        int remainingLength = valueLength - alreadyReadChunkSize;
                        if (toRead > remainingLength) {
                            toRead = remainingLength;
                        }

                        ByteBuf chunkBuffer = readBytes(ctx.alloc(), in, toRead);

                        MemcacheContent chunk;
                        if ((alreadyReadChunkSize += toRead) >= valueLength) {
                            chunk = new DefaultLastMemcacheContent(chunkBuffer);
                        } else {
                            chunk = new DefaultMemcacheContent(chunkBuffer);
                        }

                        out.add(chunk);
                        if (alreadyReadChunkSize < valueLength) {
                            return;
                        }
                    } else {
                        out.add(LastMemcacheContent.EMPTY_LAST_CONTENT);
                    }

                    resetDecoder();
                    state = State.READ_HEADER;
                    return;
                } catch (Exception e) {
                    resetDecoder();
                    out.add(invalidChunk(e));
                    return;
                }
            case BAD_MESSAGE:
                in.skipBytes(actualReadableBytes());
                return;
            default:
                throw new Error("Unknown state reached: " + state);
        }
    }

    /**
     * Helper method to create a message indicating a invalid decoding result.
     *
     * @param cause the cause of the decoding failure.
     * @return a valid message indicating failure.
     */
    private M invalidMessage(Exception cause) {
        state = State.BAD_MESSAGE;
        M message = buildInvalidMessage();
        message.setDecoderResult(DecoderResult.failure(cause));
        return message;
    }

    /**
     * Helper method to create a content chunk indicating a invalid decoding result.
     *
     * @param cause the cause of the decoding failure.
     * @return a valid content chunk indicating failure.
     */
    private MemcacheContent invalidChunk(Exception cause) {
        state = State.BAD_MESSAGE;
        MemcacheContent chunk = new DefaultLastMemcacheContent(Unpooled.EMPTY_BUFFER);
        chunk.setDecoderResult(DecoderResult.failure(cause));
        return chunk;
    }

    /**
     * When the channel goes inactive, release all frames to prevent data leaks.
     *
     * @param ctx handler context
     * @throws Exception if something goes wrong during channel inactive notification.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        resetDecoder();
    }

    /**
     * Prepare for next decoding iteration.
     */
    protected void resetDecoder() {
        if (currentMessage != null) {
            currentMessage.release();
            currentMessage = null;
        }
        alreadyReadChunkSize = 0;
    }

    /**
     * Decode and return the parsed {@link BinaryMemcacheMessage}.
     *
     * @param in the incoming buffer.
     * @return the decoded header.
     */
    protected abstract M decodeHeader(ByteBuf in);

    /**
     * Helper method to create a upstream message when the incoming parsing did fail.
     *
     * @return a message indicating a decoding failure.
     */
    protected abstract M buildInvalidMessage();

    /**
     * Contains all states this decoder can possibly be in.
     *
     * <p>Note that most of the states can be optional, the only one required is reading
     * the header ({@link #READ_HEADER}. All other steps depend on the length fields
     * in the header and will be executed conditionally.</p>
     */
    enum State {
        /**
         * Currently reading the header portion.
         */
        READ_HEADER,

        /**
         * Read the framing extras (optional).
         */
        READ_FRAMING,

        /**
         * Currently reading the extras portion (optional).
         */
        READ_EXTRAS,

        /**
         * Currently reading the key portion (optional).
         */
        READ_KEY,

        /**
         * Currently reading the value chunks (optional).
         */
        READ_CONTENT,

        /**
         * Something went wrong while decoding the message or chunks.
         */
        BAD_MESSAGE
    }

}
