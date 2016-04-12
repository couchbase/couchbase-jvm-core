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
package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;

/**
 * Represents a observe seqno response.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class FailoverObserveSeqnoResponse extends AbstractKeyValueResponse {

    private final boolean master;
    private final short vbucketID;
    private final long vbucketUUID;
    private final long lastPersistedSeqNo;
    private final long currentSeqNo;
    private final long oldVbucketUUID;
    private final long lastSeqNoReceived;

    public FailoverObserveSeqnoResponse(boolean master, short vbucketID, long vbucketUUID, long lastPersistedSeqNo,
        long currentSeqNo, long oldVbucketUUID, long lastSeqNoReceived, ResponseStatus status, short serverStatusCode,
        String bucket, CouchbaseRequest request) {
        super(status, serverStatusCode, bucket, null, request);

        this.master = master;
        this.vbucketID = vbucketID;
        this.vbucketUUID = vbucketUUID;
        this.lastPersistedSeqNo = lastPersistedSeqNo;
        this.currentSeqNo = currentSeqNo;
        this.oldVbucketUUID = oldVbucketUUID;
        this.lastSeqNoReceived = lastSeqNoReceived;
    }

    public boolean master() {
        return master;
    }

    public short vbucketID() {
        return vbucketID;
    }

    public long newVbucketUUID() {
        return vbucketUUID;
    }

    public long lastPersistedSeqNo() {
        return lastPersistedSeqNo;
    }

    public long currentSeqNo() {
        return currentSeqNo;
    }

    public long lastSeqNoReceived() {
        return lastSeqNoReceived;
    }

    public long oldVbucketUUID() {
        return oldVbucketUUID;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FailoverObserveSeqnoResponse{");
        sb.append("master=").append(master);
        sb.append(", vbucketID=").append(vbucketID);
        sb.append(", vbucketUUID=").append(vbucketUUID);
        sb.append(", lastPersistedSeqNo=").append(lastPersistedSeqNo);
        sb.append(", currentSeqNo=").append(currentSeqNo);
        sb.append(", oldVbucketUUID=").append(oldVbucketUUID);
        sb.append(", lastSeqNoReceived=").append(lastSeqNoReceived);
        sb.append('}');
        return sb.toString();
    }
}
