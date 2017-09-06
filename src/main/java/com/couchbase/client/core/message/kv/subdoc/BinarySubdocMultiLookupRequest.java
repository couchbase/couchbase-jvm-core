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
import com.couchbase.client.core.message.kv.subdoc.multi.LookupCommand;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * A {@link BinarySubdocRequest} that describes multiple lookup operations on a single document.
 *
 * Each {@link LookupCommand} can act on a different path inside the document.
 * Such a multi-operation can partially fail, as some lookup are valid while others are not.
 *
 * @author Simon Baslé
 * @since 1.2
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public interface BinarySubdocMultiLookupRequest extends BinaryRequest {

    /**
     * @return a list of the {@link LookupCommand} describing the multiple operations to apply.
     */
    List<LookupCommand> commands();

    /**
     * @return the document flags for the Request
     */
    byte docFlags();

    /**
     * The {@link ByteBuf} representing the whole list of {@link #commands()}.
     *
     * This buffer is to be automatically released once the message has been written on the wire.
     *
     * @return the ByteBuf to serve as a memcached protocol message body.
     */
    ByteBuf content();
}
