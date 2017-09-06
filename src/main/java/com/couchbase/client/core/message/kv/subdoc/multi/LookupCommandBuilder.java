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
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocMultiLookupRequest;
/**
 * Builder for {@link LookupCommand}
 *
 * @author Subhashni Balakrishnan
 * @since 1.4.2
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public class LookupCommandBuilder {

    private final Lookup lookup;
    private final String path;
    private boolean xattr;

    /**
     * Create a multi-lookup command.
     *
     * @param lookup the lookup type.
     * @param path the path to look-up inside the document.
     */
    public LookupCommandBuilder(Lookup lookup, String path) {
        this.lookup = lookup;
        this.path = path;
    }

    public LookupCommand build(){
        return new LookupCommand(this);
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

    /**
     * Access to extended attribute section of the couchbase document
     *
     * @return true if accessing extended attribute section
     */
    public boolean xattr() { return this.xattr; }

    /**
     * Access to extended attribute section of the couchbase document
     *
     * @return builder with true set for attribute access
     */
    public LookupCommandBuilder xattr(boolean xattr) {
        this.xattr = xattr;
        return this;
    }

}
