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
