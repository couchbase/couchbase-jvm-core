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
package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseRequest;

public class ViewQueryRequest extends AbstractCouchbaseRequest implements ViewRequest {

    private final String design;
    private final String view;
    private final String query;
    private final boolean spatial;
    private final boolean development;

    public ViewQueryRequest(String design, String view, boolean development, String bucket, String password) {
        this(design, view, development, false, null, bucket, password);
    }

    public ViewQueryRequest(String design, String view, boolean development, String query, String bucket, String password) {
        this(design, view, development, false, query, bucket, password);
    }

    public ViewQueryRequest(String design, String view, boolean development, boolean spatial, String query, String bucket, String password) {
        super(bucket, password);
        this.design = design;
        this.view = view;
        this.query = query;
        this.development = development;
        this.spatial = spatial;
    }

    public String design() {
        return design;
    }

    public String view() {
        return view;
    }

    public String query() {
        return query;
    }

    public boolean development() {
        return development;
    }

    public boolean spatial() {
        return spatial;
    }
}
