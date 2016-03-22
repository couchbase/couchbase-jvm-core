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
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.subjects.Subject;

/**
 * Default implementation of {@link DCPRequest}.
 *
 * @author Sergey Avseyev
 * @since 1.2.6
 */
@InterfaceStability.Experimental
@InterfaceAudience.Private
public abstract class AbstractDCPMessage extends AbstractDCPRequest implements DCPMessage {
    private final String key;
    private final int totalBodyLength;

    /**
     * Creates a new {@link AbstractDCPMessage}.
     *
     * @param totalBodyLength
     * @param partition
     * @param key
     * @param bucket          the bucket of the document.
     * @param password        the optional password of the bucket.
     */
    public AbstractDCPMessage(int totalBodyLength, short partition, String key, final String bucket, final String password) {
        super(bucket, password);
        this.partition(partition);
        this.key = key;
        this.totalBodyLength = totalBodyLength;
    }

    public AbstractDCPMessage(int totalBodyLength, short partition, String key, final String bucket, final String password,
                              final Subject<CouchbaseResponse, CouchbaseResponse> observable) {
        super(bucket, password, observable);
        this.partition(partition);
        this.key = key;
        this.totalBodyLength = totalBodyLength;
    }

    @Override
    public int totalBodyLength() {
        return totalBodyLength;
    }

    @Override
    public String key() {
        return key;
    }
}
