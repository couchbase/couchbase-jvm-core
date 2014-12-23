/*
 * Copyright (c) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
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
