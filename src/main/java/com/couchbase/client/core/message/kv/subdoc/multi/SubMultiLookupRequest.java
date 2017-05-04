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

import static com.couchbase.client.core.endpoint.kv.KeyValueHandler.SUBDOC_FLAG_XATTR_PATH;

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
     * @param commands {@link LookupCommand} commands
     */
    public SubMultiLookupRequest(String key, String bucket, LookupCommand... commands) {
        super(key, bucket);
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
            //flags
            if (command.xattr()) {
                commandBuf.writeByte(SUBDOC_FLAG_XATTR_PATH);
            } else {
                commandBuf.writeByte(0);
            }
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
