/**
 * Copyright (C) 2014 Couchbase, Inc.
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
 * Fetch a document from the cluster and return it if found.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class GetRequest extends AbstractKeyValueRequest {

    private final boolean lock;
    private final boolean touch;
    private final int expiry;

    public GetRequest(final String key, final String bucket) {
        this(key, bucket, false, false, 0);
    }

    public GetRequest(final String key, final String bucket, final boolean lock, final boolean touch, final int expiry) {
        super(key, bucket, null);
        if (lock && touch) {
            throw new IllegalArgumentException("Locking and touching in the same request is not supported.");
        }
        if (!lock && !touch && expiry > 0) {
            throw new IllegalArgumentException("Expiry/Lock time cannot be set for a regular get request.");
        }
        this.lock = lock;
        this.touch = touch;
        this.expiry = expiry;
    }

    public boolean lock() {
        return lock;
    }

    public boolean touch() {
        return touch;
    }

    public int expiry() {
        return expiry;
    }
}
