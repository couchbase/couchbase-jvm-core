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
import io.netty.buffer.ByteBuf;

/**
 * A message representing event that creates or updates a document.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
public class MutationMessage extends AbstractDCPRequest {
    private final String key;
    private final ByteBuf content;
    private final int expiration;
    private final int flags;
    private final int lockTime;
    private final long cas;

    public MutationMessage(short partition, String key, ByteBuf content, int expiration,
                           int flags, int lockTime, long cas, String bucket) {
        this(partition, key, content, expiration, flags, lockTime, cas, bucket, null);
    }

    public MutationMessage(short partition, String key, ByteBuf content, int expiration,
                           int flags, int lockTime, long cas, String bucket, String password) {
        super(bucket, password);
        this.partition(partition);
        this.key = key;
        this.content = content;
        this.expiration = expiration;
        this.flags = flags;
        this.lockTime = lockTime;
        this.cas = cas;
    }

    public String key() {
        return key;
    }

    public ByteBuf content() {
        return content;
    }

    public int expiration() {
        return expiration;
    }

    public int flags() {
        return flags;
    }

    public long cas() {
        return cas;
    }
}
