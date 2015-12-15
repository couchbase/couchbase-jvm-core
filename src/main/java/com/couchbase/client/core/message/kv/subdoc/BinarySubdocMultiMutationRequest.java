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
import com.couchbase.client.core.message.kv.BinaryRequest;
import com.couchbase.client.core.message.kv.subdoc.multi.MutationCommand;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * A {@link BinarySubdocRequest} that describes multiple mutations on a single document.
 * The mutations are applied atomically, so they either all succeed or none is applied.
 *
 * Each {@link MutationCommand} can act on a different path inside the document and be of a different nature.
 *
 * A multi-mutation request can also alter the enclosing document's expiry and flags.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public interface BinarySubdocMultiMutationRequest extends BinaryRequest {

    /**
     * @return the expiration (or TTL) to apply to the document along the mutations, 0 for no TTL.
     */
    int expiration();

    /**
     * @return the CAS to use for the mutations (if needed) or 0L to ignore
     */
    long cas();

    /**
     * @return a list of the {@link MutationCommand} describing the multiple mutations to apply.
     */
    List<MutationCommand> commands();

    /**
     * The {@link ByteBuf} representing the whole list of {@link #commands()}.
     *
     * This buffer is to be automatically released once the message has been written on the wire.
     *
     * @return the ByteBuf to serve as a memcached protocol message body.
     */
    ByteBuf content();
}
