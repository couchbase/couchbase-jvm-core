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

package com.couchbase.client.core.document;

/**
 * Abstract {@link Document} which implements needed functionality across actual implementations.
 */
public abstract class AbstractDocument implements Document {

    /**
     * Do not use an expiration time if not explicitly set.
     */
    public static final int DEFAULT_EXPIRATION = 0;

    /**
     * Do not use a CAS value if not explicitly set.
     */
    public static final long DEFAULT_CAS  = 0;

    /**
     * The ID of the document.
     */
    private final String id;

    /**
     * The partition ID of the document.
     */
    private final String partitionId;

    /**
     * The expiration time of the document.
     */
    private final int expiration;

    /**
     * The CAS value of the document.
     */
    private final long cas;

    /**
     * Create a new {@link AbstractDocument} with just the ID and default values.
     *
     * @param id the document ID.
     */
    protected AbstractDocument(String id) {
        this(id, DEFAULT_EXPIRATION, DEFAULT_CAS);
    }

    /**
     * Create a new {@link AbstractDocument} where the partition id is the same as the document id.
     *
     * @param id the document ID.
     * @param expiration the expiration time.
     * @param cas the CAS value.
     */
    protected AbstractDocument(String id, int expiration, long cas) {
        this(id, id, expiration, cas);
    }

    /**
     * Create a new {@link AbstractDocument} with all possible values.
     *
     * @param id the document ID.
     * @param partitionId the partition ID of the document.
     * @param expiration the expiration time.
     * @param cas the CAS value.
     */
    protected AbstractDocument(String id, String partitionId, int expiration, long cas) {
        this.id = id;
        this.partitionId = partitionId;
        this.expiration = expiration;
        this.cas = cas;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public long cas() {
        return cas;
    }

    @Override
    public int expiration() {
        return expiration;
    }

    @Override
    public String partitionId() {
        return partitionId;
    }
}
