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

/**
 * A single lookup description inside a TODO.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public class LookupCommand {

    private final Lookup lookup;
    private final String path;
    private boolean xattr;

    /**
     * Create a multi-lookup command.
     *
     * @param lookup the lookup type.
     * @param path the path to look-up inside the document.
     */
    @Deprecated
    public LookupCommand(Lookup lookup, String path) {
        this.lookup = lookup;
        this.path = path;
    }

    protected LookupCommand(LookupCommandBuilder builder) {
        this.lookup = builder.lookup();
        this.path = builder.path();
        this.xattr = builder.xattr();
    }

    public Lookup lookup() {
        return lookup;
    }

    public String path() {
        return path;
    }

    public byte opCode() {
        return lookup.opCode();
    }

    public boolean xattr() {
        return this.xattr;
    }

}
