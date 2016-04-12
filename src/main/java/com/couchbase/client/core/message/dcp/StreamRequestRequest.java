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
import com.couchbase.client.core.endpoint.dcp.DCPConnection;

/**
 * Stream request.
 *
 * Sent by the consumer side to the producer specifying that the consumer
 * wants to create a vBucket stream. In order to initiate multiple stream
 * the consumer needs to send multiple commands.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
public class StreamRequestRequest extends AbstractDCPRequest {
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
    private final DCPConnection connection;


    public StreamRequestRequest(short partition, long vbucketUUID, long startSequenceNumber, long endSequenceNumber,
                                long snapshotStartSequenceNumber, long snapshotEndSequenceNumber,
                                String bucket, String password, DCPConnection connection) {
        super(bucket, password);
        this.partition(partition);
        this.vbucketUUID = vbucketUUID;
        this.startSequenceNumber = startSequenceNumber;
        this.endSequenceNumber = endSequenceNumber;
        this.snapshotStartSequenceNumber = snapshotStartSequenceNumber;
        this.snapshotEndSequenceNumber = snapshotEndSequenceNumber;
        this.connection = connection;
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

    public DCPConnection connection() {
        return connection;
    }
}
