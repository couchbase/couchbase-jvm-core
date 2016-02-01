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
package com.couchbase.client.core.node.locate;

import com.couchbase.client.core.ServiceNotAvailableException;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.node.Node;

import java.util.List;

public class ViewLocator implements Locator {

    private static final ServiceNotAvailableException NOT_AVAILABLE =
        new ServiceNotAvailableException("Views are not available on this bucket type.");

    private long counter = 0;

    @Override
    public Node[] locate(CouchbaseRequest request, List<Node> nodes, ClusterConfig config) {
        BucketConfig bucketConfig = config.bucketConfig(request.bucket());
        if (!(bucketConfig instanceof CouchbaseBucketConfig)) {
            request.observable().onError(NOT_AVAILABLE);
            return null;
        }

        int item = (int) counter++ % nodes.size();
        int i = 0;
        for (Node node : nodes) {
            if (i++ == item && ((CouchbaseBucketConfig) bucketConfig).hasPrimaryPartitionsOnNode(node.hostname())) {
                return new Node[] { node };
            }
        }

        return new Node[] {};
    }
}
