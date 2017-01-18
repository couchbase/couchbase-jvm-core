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

    /**
     * Access to extended attribute section of the couchbase document
     *
     * @return true if accessing extended attribute section
     */
    boolean attributeAccess();
}
