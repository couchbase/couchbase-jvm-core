/*
 * Copyright (c) 2016 Couchbase, Inc.
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
 * A message representing event that removes or expires a document.
 *
 * @author Sergey Avseyev
 * @since 1.2.6
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
public class ExpirationMessage extends AbstractDCPMessage {
    private final long cas;
    private final long bySequenceNumber;
    private final long revisionSequenceNumber;

    public ExpirationMessage(int totalBodyLength, short partition, String key, long cas, long bySequenceNumber, long revisionSequenceNumber, String bucket) {
        this(totalBodyLength, partition, key, cas, bySequenceNumber, revisionSequenceNumber, bucket, null);
    }

    public ExpirationMessage(int totalBodyLength, short partition, String key, long cas, long bySequenceNumber, long revisionSequenceNumber, String bucket, String password) {
        super(totalBodyLength, partition, key, bucket, password);
        this.cas = cas;
        this.bySequenceNumber = bySequenceNumber;
        this.revisionSequenceNumber = revisionSequenceNumber;
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
}
