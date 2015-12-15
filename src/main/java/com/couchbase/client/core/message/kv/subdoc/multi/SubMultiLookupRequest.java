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
import com.couchbase.client.core.message.kv.AbstractKeyValueRequest;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiLookupRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.util.Arrays;
import java.util.List;

/**
 * Concrete implementation of a {@link BinarySubdocMultiLookupRequest}.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class SubMultiLookupRequest extends AbstractKeyValueRequest implements BinarySubdocMultiLookupRequest {

    private final List<LookupCommand> commands;
    private final ByteBuf encoded;

    /**
     * Creates a new {@link SubMultiLookupRequest}.
     *
     * @param key      the key of the document to look into.
     * @param bucket   the bucket of the document.
     */
    public SubMultiLookupRequest(String key, String bucket, LookupCommand... commands) {
        super(key, bucket, null);
        if (commands == null) {
            throw new NullPointerException("At least one lookup command is necessary");
        }
        this.commands = Arrays.asList(commands);
        this.encoded = encode(this.commands);
    }

    private static ByteBuf encode(List<LookupCommand> commands) {
        CompositeByteBuf compositeBuf = Unpooled.compositeBuffer(commands.size()); //FIXME pooled allocator?
        for (LookupCommand command : commands) {
            byte[] pathBytes = command.path().getBytes(CharsetUtil.UTF_8);
            short pathLength = (short) pathBytes.length;

            ByteBuf commandBuf = Unpooled.buffer(4 + pathLength); //FIXME a way of using the pooled allocator?
            commandBuf.writeByte(command.opCode());
            commandBuf.writeByte(0); //no flags supported for lookup
            commandBuf.writeShort(pathLength);
            //no value length
            commandBuf.writeBytes(pathBytes);

            compositeBuf.addComponent(commandBuf);
            compositeBuf.writerIndex(compositeBuf.writerIndex() + commandBuf.readableBytes());
        }
        return compositeBuf;
    }

    /**
     * @return a list of the {@link LookupCommand} describing the multiple operations to apply.
     */
    @Override
    public List<LookupCommand> commands() {
        return this.commands;
    }

    @Override
    public ByteBuf content() {
        return this.encoded;
    }
}
