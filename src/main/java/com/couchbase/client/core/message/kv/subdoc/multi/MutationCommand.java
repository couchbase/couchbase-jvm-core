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
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiMutationRequest;
import io.netty.buffer.ByteBuf;

/**
 * A single mutation description inside a {@link BinarySubdocMultiMutationRequest}.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class MutationCommand {

    private final Mutation mutation;
    private final String path;
    private final ByteBuf fragment;
    private final boolean createIntermediaryPath;

    /**
     * Create a multi-mutation command.
     *
     * @param mutation the mutation type.
     * @param path the path to mutate inside the document.
     * @param fragment the target value for the mutation. This will be released when the request is sent.
     * @param createIntermediaryPath true if missing parts of the path should be created if possible, false otherwise.
     */
    public MutationCommand(Mutation mutation, String path, ByteBuf fragment, boolean createIntermediaryPath) {
        this.mutation = mutation;
        this.path = path;
        this.fragment = fragment;
        this.createIntermediaryPath = createIntermediaryPath;
    }

    /**
     * Create a multi-mutation command, without attempting to create intermediary paths.
     *
     * @param mutation the mutation type.
     * @param path the path to mutate inside the document.
     * @param fragment the target value for the mutation. This will be released when the request is sent.
     */
    public MutationCommand(Mutation mutation, String path, ByteBuf fragment) {
        this(mutation, path, fragment, false);
    }

    public Mutation mutation() {
        return mutation;
    }

    public String path() {
        return path;
    }

    /*package*/ ByteBuf fragment() {
        return fragment;
    }

    public byte opCode() {
        return mutation.opCode();
    }

    public boolean createIntermediaryPath() {
        return createIntermediaryPath;
    }
}
