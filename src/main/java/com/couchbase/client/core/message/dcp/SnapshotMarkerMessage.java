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
 * Snapshot marker.
 *
 * Sent by the producer to tell the consumer that a new snapshot is being sent.
 * A snapshot is simply a series of commands that is guaranteed to contain
 * a unique set of keys.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
public class SnapshotMarkerMessage extends AbstractDCPMessage {
    public static final int MEMORY = 0x01;
    public static final int DISK = 0x02;
    public static final int CHECKPOINT = 0x04;
    public static final int ACK = 0x08;

    private final long startSequenceNumber;
    private final long endSequenceNumber;

    /**
     * Specifies binary representation of flags.
     *
     * Note: use boolean accessors for clarity and readability
     */
    private final int flags;

    /**
     * Specifies that the snapshot contains in-memory items only.
     */
    private final boolean memory;
    /**
     * Specifies that the snapshot contains on-disk items only.
     */
    private final boolean disk;
    /**
     * An internally used flag for intra-cluster replication to
     * help to keep in-memory data structures look similar.
     */
    private final boolean checkpoint;
    /**
     * Specifies that this snapshot marker should return a response
     * once the entire snapshot is received.
     */
    private final boolean ack;

    public SnapshotMarkerMessage(short partition, long startSequenceNumber, long endSequenceNumber,
                                 int flags, String bucket) {
        this(partition, startSequenceNumber, endSequenceNumber, flags, bucket, null);
    }

    public SnapshotMarkerMessage(short partition, long startSequenceNumber, long endSequenceNumber,
                                 int flags, String bucket, String password) {
        super(partition, null, bucket, password);
        partition(partition);
        this.startSequenceNumber = startSequenceNumber;
        this.endSequenceNumber = endSequenceNumber;
        this.flags = flags;
        this.memory = (flags & MEMORY) == MEMORY;
        this.disk = (flags & DISK) == DISK;
        this.checkpoint = (flags & CHECKPOINT) == CHECKPOINT;
        this.ack = (flags & ACK) == ACK;
    }

    public long startSequenceNumber() {
        return startSequenceNumber;
    }

    public long endSequenceNumber() {
        return endSequenceNumber;
    }

    public int flags() {
        return flags;
    }

    public boolean memory() {
        return memory;
    }

    public boolean disk() {
        return disk;
    }

    public boolean checkpoint() {
        return checkpoint;
    }

    public boolean ack() {
        return ack;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SnapshotMarkerMessage{");
        sb.append("startSequenceNumber=").append(startSequenceNumber);
        sb.append(", endSequenceNumber=").append(endSequenceNumber);
        sb.append(", memory=").append(memory);
        sb.append(", disk=").append(disk);
        sb.append(", checkpoint=").append(checkpoint);
        sb.append(", ack=").append(ack);
        sb.append('}');
        return sb.toString();
    }
}
