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
package com.couchbase.client.core.service;

/**
 * Represents the different {@link ServiceType}s and how they map onto buckets.
 */
public enum ServiceType {

    /**
     * Views and Design Documents.
     */
    VIEW(BucketServiceMapping.ONE_FOR_ALL),

    /**
     * Memcache type operations.
     */
    BINARY(BucketServiceMapping.ONE_BY_ONE),

    /**
     * UPR and TAP stream operations.
     */
    STREAM(BucketServiceMapping.ONE_BY_ONE),

    /**
     * 8091 config operations.
     */
    CONFIG(BucketServiceMapping.ONE_FOR_ALL);

    private final BucketServiceMapping mapping;

    private ServiceType(BucketServiceMapping mapping) {
        this.mapping = mapping;
    }

    public BucketServiceMapping mapping() {
        return mapping;
    }
}
