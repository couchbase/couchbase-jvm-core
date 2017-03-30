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
import io.netty.buffer.ByteBuf;

/**
 * A message representing event that creates or updates a document.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
@Deprecated
@InterfaceStability.Experimental
@InterfaceAudience.Private
public class MutationMessage extends AbstractDCPMessage {
    private final ByteBuf content;
    private final int expiration;
    private final int flags;
    private final int lockTime;
    private final long cas;
    private final long bySequenceNumber;
    private final long revisionSequenceNumber;

    @Deprecated
    public MutationMessage(int totalBodyLength, short partition, String key, ByteBuf content, int expiration,
                           long bySequenceNumber, long revisionSequenceNumber,
                           int flags, int lockTime, long cas, String bucket) {
        this(totalBodyLength, partition, key, content, expiration, bySequenceNumber, revisionSequenceNumber,
                flags, lockTime, cas, bucket, null);
    }

    @Deprecated
    public MutationMessage(int totalBodyLength, short partition, String key, ByteBuf content, int expiration,
                           long bySequenceNumber, long revisionSequenceNumber,
                           int flags, int lockTime, long cas, String bucket, String password) {
        this(totalBodyLength, partition, key, content, expiration, bySequenceNumber, revisionSequenceNumber,
                flags, lockTime, cas, bucket, bucket, password);
    }

    public MutationMessage(int totalBodyLength, short partition, String key, ByteBuf content, int expiration,
                           long bySequenceNumber, long revisionSequenceNumber,
                           int flags, int lockTime, long cas, String bucket, String username, String password) {
        super(totalBodyLength, partition, key, bucket, username, password);
        this.content = content;
        this.expiration = expiration;
        this.flags = flags;
        this.lockTime = lockTime;
        this.cas = cas;
        this.bySequenceNumber = bySequenceNumber;
        this.revisionSequenceNumber = revisionSequenceNumber;
    }

    public ByteBuf content() {
        return content;
    }

    public int expiration() {
        return expiration;
    }

    public int lockTime() {
        return lockTime;
    }

    public int flags() {
        return flags;
    }

    public long cas() {
        return cas;
    }

    public long bySequenceNumber() {
        return bySequenceNumber;
    }

    public long revisionSequenceNumber() {
        return revisionSequenceNumber;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MutationMessage{");
        sb.append("key='").append(key()).append('\'');
        sb.append(", content=").append(content);
        sb.append(", expiration=").append(expiration);
        sb.append(", flags=").append(flags);
        sb.append(", lockTime=").append(lockTime);
        sb.append(", cas=").append(cas);
        sb.append('}');
        return sb.toString();
    }
}
