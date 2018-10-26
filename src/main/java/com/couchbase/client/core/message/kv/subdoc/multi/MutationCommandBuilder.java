/*
 * Copyright (c) 2017 Couchbase, Inc.
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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Builder for {@link MutationCommand}.
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.2
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public class MutationCommandBuilder {
    private Mutation mutation;
    private String path;
    private ByteBuf fragment;
    private boolean createIntermediaryPath;
    private boolean xattr;
    private boolean expandMacros;

    /**
     * Create a multi-mutation command.
     *
     * @param mutation the mutation type.
     * @param path the path to mutate inside the document.
     */
    public MutationCommandBuilder(Mutation mutation, String path) {
        this.mutation = mutation;
        this.path = path;
        this.fragment = Unpooled.EMPTY_BUFFER;
    }

    /**
     * Create a multi-mutation command.
     *
     * @param mutation the mutation type.
     * @param path     the path to mutate inside the document.
     * @param fragment the target value for the mutation. This will be released when the request is sent.
     */
    public MutationCommandBuilder(Mutation mutation, String path, ByteBuf fragment) {
        this.mutation = mutation;
        this.path = path;
        this.fragment = (fragment == null) ? Unpooled.EMPTY_BUFFER : fragment;
    }

    public MutationCommand build() {
        return new MutationCommand(this);
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

    public boolean xattr() { return xattr; }

    public boolean expandMacros() { return expandMacros; }

    public MutationCommandBuilder createIntermediaryPath(boolean createIntermediaryPath) {
        this.createIntermediaryPath = createIntermediaryPath;
        return this;
    }

    public MutationCommandBuilder fragment(ByteBuf fragment) {
        this.fragment = fragment;
        return this;
    }

    public MutationCommandBuilder xattr(boolean xattr) {
        this.xattr = xattr;
        return this;
    }

    public MutationCommandBuilder expandMacros(boolean expandMacros) {
        this.expandMacros = expandMacros;
        return this;
    }
}
