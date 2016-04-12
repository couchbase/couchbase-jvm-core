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

/**
 * Failover log entry.
 *
 * It is used by the consumer to choose the point in stream to continue from.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
public class FailoverLogEntry {
    /**
     * A unique identifier that is generated that is assigned to each vBucket.
     * This number is generated on an unclean shutdown or when a vBucket becomes active.
     */
    private final long vbucketUUID;
    /**
     * Each mutation that occurs on a vBucket is assigned a number, which strictly
     * increases as events are assigned sequence numbers, that can be used to order
     * that event against other mutations within the same vBucket.
     */
    private final long sequenceNumber;

    public FailoverLogEntry(final long vbucketUUID, final long sequenceNumber) {
        this.vbucketUUID = vbucketUUID;
        this.sequenceNumber = sequenceNumber;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public long vbucketUUID() {
        return vbucketUUID;
    }
}
