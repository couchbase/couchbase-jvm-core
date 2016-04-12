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
