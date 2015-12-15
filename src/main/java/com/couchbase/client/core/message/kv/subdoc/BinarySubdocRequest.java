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

package com.couchbase.client.core.message.kv.subdoc;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.endpoint.kv.KeyValueHandler;
import com.couchbase.client.core.message.kv.BinaryRequest;
import io.netty.buffer.ByteBuf;

/**
 * A type of {@link BinaryRequest} that deals with getting or mutating parts of
 * a JSON document, aka a Sub-Document. The part that is to be considered is
 * represented by the {@link #path()}.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public interface BinarySubdocRequest extends BinaryRequest {

    /**
     * Returns the **path** inside a JSON document where values will be obtained/mutated. Some sub-document operations
     * authorize the use of an empty path while other don't.
     *
     * @return the path to work from inside the JSON document.
     */
    String path();

    /**
     * @return the length of the path as encoded in the {@link #content()} (can serve as an offset from 0 to find the path in the content).
     */
    int pathLength();

    /**
     * @return the opcode of the operation
     * @see KeyValueHandler#OP_SUB_GET KeyValueHandler.OP_SUB_GET and other OP_SUB_ constants for the list of opcodes
     */
    byte opcode();

    /**
     * The {@link ByteBuf} bearing the full content for this request. The content is at a minimum comprised of the
     * {@link #path()} as UTF8 bytes, and can also have any other relevant payload appended (eg. a JSON fragment for
     * mutative operations, see {@link BinarySubdocMutationRequest#fragment()}).
     *
     * This buffer is to be automatically released once the message has been written on the wire.
     *
     * @return the ByteBuf to serve as a memcached protocol message body.
     */
    ByteBuf content();

}
