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

package com.couchbase.client.core.dcp;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * @author Sergey Avseyev
 * @since 1.2.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class BucketStreamState {
    /**
     * Default state, which matches all changes in the stream.
     */
    public static final BucketStreamState BLANK = new BucketStreamState(0, 0, 0xffffffff, 0, 0xffffffff);

    /**
     * A unique identifier that is generated that is assigned to each VBucket.
     * This number is generated on an unclean shutdown or when a VBucket becomes
     * active.
     */
    private final long vbucketUUID;

    /**
     * Specified the last by sequence number that has been seen by the consumer.
     */
    private final long startSequenceNumber;
    /**
     * Specifies that the stream should be closed when the sequence number with
     * this ID has been sent.
     */
    private final long endSequenceNumber;
    /**
     * Set to the same value as the start sequence number by default, in case it
     * is a retry because the stream request didn't return all expected results
     * use the start sequence of the last partial snapshot that was received.
     */
    private final long snapshotStartSequenceNumber;
    /**
     * Set to the same value as the start sequence number by default, in case
     * it is a retry because the stream request didn't return all expected results,
     * use the end sequence of the last partial snapshot that was received.
     */
    private final long snapshotEndSequenceNumber;


    public BucketStreamState(long vbucketUUID,
                             long startSequenceNumber, long endSequenceNumber,
                             long snapshotStartSequenceNumber, long snapshotEndSequenceNumber) {
        this.vbucketUUID = vbucketUUID;
        this.startSequenceNumber = startSequenceNumber;
        this.endSequenceNumber = endSequenceNumber;
        this.snapshotStartSequenceNumber = snapshotStartSequenceNumber;
        this.snapshotEndSequenceNumber = snapshotEndSequenceNumber;
    }

    public long vbucketUUID() {
        return vbucketUUID;
    }

    public long startSequenceNumber() {
        return startSequenceNumber;
    }

    public long endSequenceNumber() {
        return endSequenceNumber;
    }

    public long snapshotStartSequenceNumber() {
        return snapshotStartSequenceNumber;
    }

    public long snapshotEndSequenceNumber() {
        return snapshotEndSequenceNumber;
    }

    public String toString() {
        return "BucketStreamState{" +
                "vbucketUUID=" + vbucketUUID +
                ", startSequenceNumber=" + startSequenceNumber +
                ", endSequenceNumber=" + endSequenceNumber +
                ", snapshotStartSequenceNumber=" + snapshotStartSequenceNumber +
                ", snapshotEndSequenceNumber=" + snapshotEndSequenceNumber +
                '}';

    }
}
