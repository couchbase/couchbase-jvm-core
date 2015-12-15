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
import io.netty.buffer.ByteBuf;

/**
 * A {@link BinarySubdocRequest} that describes a mutation operation. Mutations work on
 * a {@link #fragment()} of the enclosing JSON document, at the site denoted by the {@link #path()}.
 *
 * If the path given doesn't exist in its entirety, some mutation operations can optionally offer to
 * create intermediary nodes in the JSON if valid and {@link #createIntermediaryPath()} returns
 * <code>true</code>.
 *
 * Note that fragments should always be valid JSON. A sub-document mutation can also alter the enclosing
 * document's expiry and flags.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public interface BinarySubdocMutationRequest extends BinarySubdocRequest {

    /**
     * @return the expiration (or TTL) to apply to the document along the mutation, 0 for no TTL.
     */
    int expiration();

    /**
     * A {@link ByteBuf} containing the JSON fragment for the mutation. It is appended to the {@link #content()}.
     *
     * This buffer is to be automatically released once the content has been written on the wire.
     *
     * @return the target value for the mutation (a fragment of the whole JSON document, valid JSON itself)
     */
    ByteBuf fragment();

    /**
     * Sets whether missing nodes in the {@link #path()} should be created as part of this mutation, when possible.
     * This is represented as an additional flag on the wire.
     *
     * @return true if missing JSON nodes in the path should be created, false otherwise.
     */
    boolean createIntermediaryPath();

    /**
     * @return the CAS to use for the mutation (if needed) or 0L to ignore
     */
    long cas();
}
