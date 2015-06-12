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
public class MutationDescriptor {

    private final long vbucketUUID;
    private final long seqNo;

    public MutationDescriptor(long vbucketUUID, long seqNo) {
        this.vbucketUUID = vbucketUUID;
        this.seqNo = seqNo;
    }

    public long vbucketUUID() {
        return vbucketUUID;
    }

    public long seqNo() {
        return seqNo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MutationDescriptor that = (MutationDescriptor) o;

        if (vbucketUUID != that.vbucketUUID) return false;
        return seqNo == that.seqNo;

    }

    @Override
    public int hashCode() {
        int result = (int) (vbucketUUID ^ (vbucketUUID >>> 32));
        result = 31 * result + (int) (seqNo ^ (seqNo >>> 32));
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("md{");
        sb.append("vbuuid=").append(vbucketUUID);
        sb.append(", seqno=").append(seqNo);
        sb.append('}');
        return sb.toString();
    }
}
