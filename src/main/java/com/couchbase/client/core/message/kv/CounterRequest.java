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
 * Request to handle increment/decrement of a counter.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CounterRequest extends AbstractKeyValueRequest {

    private final long initial;
    private final long delta;
    private final int expiry;

    public CounterRequest(String key, long initial, long delta, int expiry, String bucket) {
        super(key, bucket, null);
        if (initial < 0) {
            throw new IllegalArgumentException("The initial needs to be >= 0");
        }
        this.initial = initial;
        this.delta = delta;
        this.expiry = expiry;
    }

    public long initial() {
        return initial;
    }

    public long delta() {
        return delta;
    }

    public int expiry() {
        return expiry;
    }

}
