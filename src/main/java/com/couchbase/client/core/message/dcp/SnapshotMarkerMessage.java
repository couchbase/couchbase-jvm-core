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

    @Deprecated
    public SnapshotMarkerMessage(int totalBodyLength, short partition, long startSequenceNumber, long endSequenceNumber,
                                 int flags, String bucket) {
        this(totalBodyLength, partition, startSequenceNumber, endSequenceNumber, flags, bucket, null);
    }

    @Deprecated
    public SnapshotMarkerMessage(int totalBodyLength, short partition, long startSequenceNumber, long endSequenceNumber,
                                 int flags, String bucket, String password) {
        this(totalBodyLength, partition, startSequenceNumber, endSequenceNumber, flags, bucket, bucket, password);
    }

    public SnapshotMarkerMessage(int totalBodyLength, short partition, long startSequenceNumber, long endSequenceNumber,
                                 int flags, String bucket, String username, String password) {
        super(totalBodyLength, partition, null, bucket, username, password);
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
