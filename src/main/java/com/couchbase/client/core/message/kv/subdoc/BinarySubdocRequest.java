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
