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
 * Represents a Couchbase Document with both the content and the meta information associated.
 */
public interface Document {

    /**
     * The bucket-unique document ID (also referred to as the document key).
     *
     * @return the document ID.
     */
    String id();

    /**
     * The actual content of the document.
     *
     * @return the document content.
     */
    Object content();

    /**
     * The CAS document opaque identifier.
     *
     * This value should not be interpreted in any way and passed back to the mutation methods exactly
     * like it was received from the server. While it might look like a monotonically increasing clock,
     * this is not the defined behaviour and the returned value can change in other ways.
     *
     * @return the document CAS value at the time of retrieval.
     */
    long cas();

    /**
     * The expiration time of the document.
     *
     * @return the document expiration time (0 if it never expires).
     */
    int expiration();

    /**
     * The ID which is used to find the correct server partition.
     *
     * If not specified explicitly, the partition ID is the same as the regular document ID. If the partition
     * ID is overriden, it will be used to find the parition instead of the regular document ID.
     *
     * @return the document partition id.
     */
    String partitionId();

}
