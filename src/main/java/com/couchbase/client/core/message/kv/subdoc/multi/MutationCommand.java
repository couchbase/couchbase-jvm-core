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
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiMutationRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

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
    private boolean createIntermediaryPath;
    private boolean attributeAccess;

    /**
     * Create a multi-mutation command.
     *
     * @param mutation               the mutation type.
     * @param path                   the path to mutate inside the document.
     * @param fragment               the target value for the mutation. This will be released when the request is sent.
     * @param createIntermediaryPath true if missing parts of the path should be created if possible, false otherwise.
     **/
    @Deprecated
    public MutationCommand(Mutation mutation, String path, ByteBuf fragment, boolean createIntermediaryPath) {
        this.mutation = mutation;
        this.path = path;
        this.fragment = (fragment == null) ? Unpooled.EMPTY_BUFFER : fragment;
        this.createIntermediaryPath = createIntermediaryPath;
    }

    /**
     * Create a multi-mutation command.
     *
     * @param mutation the mutation type.
     * @param path     the path to mutate inside the document.
     * @param fragment the target value for the mutation. This will be released when the request is sent.
     */
    @Deprecated
    public MutationCommand(Mutation mutation, String path, ByteBuf fragment) {
        this.mutation = mutation;
        this.path = path;
        this.fragment = (fragment == null) ? Unpooled.EMPTY_BUFFER : fragment;
    }

    /**
     * Create a multi-mutation without a fragment (should be restricted to DELETE, not to be confused with
     * an empty string fragment where ByteBuf contains "<code>""</code>", or the null fragment where
     * ByteBuf contains "<code>NULL</code>").
     *
     * @param path the path to delete inside the document.
     */
    @Deprecated
    public MutationCommand(Mutation mutation, String path) {
        this(mutation, path, Unpooled.EMPTY_BUFFER, false);
    }

    /**
     * Create a multi-mutation command.
     *
     * @param builder {@link MutationCommandBuilder}
     */
    protected MutationCommand(MutationCommandBuilder builder) {
        this.mutation = builder.mutation();
        this.path = builder.path();
        this.fragment = builder.fragment();
        this.createIntermediaryPath = builder.createIntermediaryPath();
        this.attributeAccess = builder.attributeAccess();
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

    public boolean attributeAccess() {
        return this.attributeAccess;
    }

}
