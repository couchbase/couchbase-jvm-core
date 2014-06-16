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
package com.couchbase.client.core.endpoint.binary;

/**
 * Contains the supported server datatypes.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class SupportedDatatypes {

    /**
     * If JSON is supported.
     */
    private final boolean json;

    /**
     * If Compression is supported.
     */
    private final boolean compression;

    public SupportedDatatypes(final boolean json, final boolean compression) {
        this.json = json;
        this.compression = compression;
    }

    /**
     * If JSON Is supported.
     *
     * @return true if it is, false otherwise.
     */
    public boolean json() {
        return json;
    }

    /**
     * If compression is supported.
     *
     * @return true if it is, false otherwise.
     */
    public boolean compression() {
        return compression;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SupportedDatatypes{");
        sb.append("json=").append(json);
        sb.append(", compression=").append(compression);
        sb.append('}');
        return sb.toString();
    }
}
