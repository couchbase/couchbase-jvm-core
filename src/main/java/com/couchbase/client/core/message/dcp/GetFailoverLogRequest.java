/*
 * Copyright (c) 2015 Couchbase, Inc.
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

/**
 * Get the current failover logs for partition.
 *
 * Failover log entry is a VBucket UUID and sequence number pair associated with a VBucket.
 * A failover log entry is assigned to an active VBucket any time there might have been a
 * history branch. The VBucket UUID is a randomly generated number used to denote a history
 * branch and the sequence number is the last sequence number processed by the VBucket at
 * the time the failover log entry was created.
 *
 * @author Sergey Avseyev
 * @since 1.2.3
 */
public class GetFailoverLogRequest extends AbstractDCPRequest {
    public GetFailoverLogRequest(final short partition, final String bucket) {
        super(bucket, null);
        partition(partition);
    }

    public GetFailoverLogRequest(final short partition, final String bucket, final String password) {
        super(bucket, password);
        partition(partition);
    }
}
