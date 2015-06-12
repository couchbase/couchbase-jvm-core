/**
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

/**
 * Value object to contain vbucket UUID and sequence number.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class MutationToken {

    private final long vbucketID;
    private final long vbucketUUID;
    private final long sequenceNumber;

    public MutationToken(long vbucketID, long vbucketUUID, long sequenceNumber) {
        this.vbucketID = vbucketID;
        this.vbucketUUID = vbucketUUID;
        this.sequenceNumber = sequenceNumber;
    }

    public long vbucketUUID() {
        return vbucketUUID;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public long vbucketID() {
        return vbucketID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutationToken that = (MutationToken) o;

        if (vbucketID != that.vbucketID) return false;
        if (vbucketUUID != that.vbucketUUID) return false;
        return sequenceNumber == that.sequenceNumber;
    }

    @Override
    public int hashCode() {
        int result = (int) (vbucketID ^ (vbucketID >>> 32));
        result = 31 * result + (int) (vbucketUUID ^ (vbucketUUID >>> 32));
        result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("mt{");
        sb.append("vbID=").append(vbucketID);
        sb.append(", vbUUID=").append(vbucketUUID);
        sb.append(", seqno=").append(sequenceNumber);
        sb.append('}');
        return sb.toString();
    }
}
