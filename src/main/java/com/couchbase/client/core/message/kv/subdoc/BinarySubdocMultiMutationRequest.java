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
     * @return the document flags for the Request
     */
    byte docFlags();

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
