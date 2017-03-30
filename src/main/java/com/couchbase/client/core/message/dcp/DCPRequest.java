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

package com.couchbase.client.core.message.dcp;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.CouchbaseRequest;

/**
 * Common interface for all DCP requests.
 *
 * Note that they can flow in both directions. For example, {@link ConnectionType#CONSUMER}
 * connection, means that messages will flow from server to client.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
@Deprecated
public interface DCPRequest extends CouchbaseRequest {
    /**
     * The partition (vBucket) to use for this request.
     *
     * @return the partition to use.
     */
    short partition();

    /**
     * Set the partition ID.
     *
     * @param id the id of the partition.
     * @return the {@link DCPRequest} for proper chaining.
     */
    DCPRequest partition(short id);
}
