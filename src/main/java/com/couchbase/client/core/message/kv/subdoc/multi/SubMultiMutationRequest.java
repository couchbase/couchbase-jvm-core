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

package com.couchbase.client.core.message.kv.subdoc.multi;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.endpoint.kv.KeyValueAuthHandler;
import com.couchbase.client.core.endpoint.kv.KeyValueHandler;
import com.couchbase.client.core.message.kv.AbstractKeyValueRequest;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiMutationRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.util.Arrays;
import java.util.List;

/**
 * Concrete implementation of a {@link BinarySubdocMultiMutationRequest}.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class SubMultiMutationRequest extends AbstractKeyValueRequest implements BinarySubdocMultiMutationRequest {

    private final List<MutationCommand> commands;
    private final ByteBuf encoded;
    private final int expiration;
    private final long cas;

    /**
     * Create a new {@link SubMultiMutationRequest}.
     *
     * @param key        the key of the document to mutate into.
     * @param bucket     the bucket of the document.
     * @param expiration the expiration (or TTL) to apply to the whole document additionally to the mutations.
     * @param cas        the CAS value to check for when applying the whole set of mutations.
     * @param commands   the set of internal mutations to apply to the document.
     */
    public SubMultiMutationRequest(String key, String bucket, int expiration, long cas, List<MutationCommand> commands) {
        super(key, bucket, null);
        if (commands == null || commands.isEmpty()) {
            throw new IllegalArgumentException("At least one mutation command is necessary");
        }
        this.commands = commands;
        this.encoded = encode(commands);
        this.expiration = expiration;
        this.cas = cas;
    }

    /**
     * Create a new {@link SubMultiMutationRequest}.
     *
     * @param key        the key of the document to mutate into.
     * @param bucket     the bucket of the document.
     * @param expiration the expiration (or TTL) to apply to the whole document additionally to the mutations.
     * @param cas        the CAS value to check for when applying the whole set of mutations.
     * @param commands   the set of internal mutations to apply to the document.
     */
    public SubMultiMutationRequest(String key, String bucket, int expiration, long cas, MutationCommand... commands) {
        this(key, bucket, expiration, cas, commands == null ? null : Arrays.asList(commands));
    }

    /**
     * Create a new {@link SubMultiMutationRequest}.
     *
     * @param key        the key of the document to mutate into.
     * @param bucket     the bucket of the document.
     * @param commands   the set of internal mutations to apply to the document.
     */
    public SubMultiMutationRequest(String key, String bucket, List<MutationCommand> commands) {
        this(key, bucket, 0, 0L, commands);
    }

    /**
     * Create a new {@link SubMultiMutationRequest}.
     *
     * @param key        the key of the document to mutate into.
     * @param bucket     the bucket of the document.
     * @param commands   the set of internal mutations to apply to the document.
     */
    public SubMultiMutationRequest(String key, String bucket, MutationCommand... commands) {
        this(key, bucket, 0, 0L, commands);
    }

    private static ByteBuf encode(List<MutationCommand> commands) {
        //FIXME a way of using the pooled allocator?
        CompositeByteBuf compositeBuf = Unpooled.compositeBuffer(commands.size());
        for (MutationCommand command : commands) {
            byte[] pathBytes = command.path().getBytes(CharsetUtil.UTF_8);
            short pathLength = (short) pathBytes.length;

            ByteBuf commandBuf = Unpooled.buffer(4 + pathLength + command.fragment().readableBytes());
            commandBuf.writeByte(command.opCode());
            if (command.createIntermediaryPath()) {
                commandBuf.writeByte(KeyValueHandler.SUBDOC_BITMASK_MKDIR_P); //0 | SUBDOC_BITMASK_MKDIR_P
            } else {
                commandBuf.writeByte(0);
            }
            commandBuf.writeShort(pathLength);
            commandBuf.writeInt(command.fragment().readableBytes());
            commandBuf.writeBytes(pathBytes);

            //copy the fragment but don't move indexes (in case it is retained and reused)
            commandBuf.writeBytes(command.fragment(), command.fragment().readerIndex(), command.fragment().readableBytes());
            //eagerly release the fragment once it's been copied
            command.fragment().release();

            //add the command to the composite buffer
            compositeBuf.addComponent(commandBuf);
            compositeBuf.writerIndex(compositeBuf.writerIndex() + commandBuf.readableBytes());
        }
        return compositeBuf;
    }

    /**
     * @return the expiration (or TTL) to apply to the document along the mutations, 0 for no TTL.
     */
    @Override
    public int expiration() {
        return this.expiration;
    }

    /**
     * @return the CAS to use for the mutations (if needed) or 0L to ignore
     */
    @Override
    public long cas() {
        return this.cas;
    }

    /**
     * @return a list of the {@link MutationCommand} describing the multiple operations to apply.
     */
    @Override
    public List<MutationCommand> commands() {
        return this.commands;
    }

    @Override
    public ByteBuf content() {
        return this.encoded;
    }
}
