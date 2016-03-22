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

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.retry.RetryHelper;
import com.lmax.disruptor.RingBuffer;

import java.util.List;

/**
 * This {@link Locator} finds the proper {@link Node}s for every incoming {@link DCPRequest}.
 *
 * @author Sergey Avseyev
 * @since 1.1.0
 */
public class DCPLocator implements Locator {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DCPLocator.class);


    @Override
    public void locateAndDispatch(final CouchbaseRequest request, final List<Node> nodes, final ClusterConfig cluster,
                                  CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {
        BucketConfig bucket = cluster.bucketConfig(request.bucket());
        if (!(bucket instanceof CouchbaseBucketConfig && request instanceof DCPRequest)) {
            throw new IllegalStateException("Unsupported Bucket Type: for request " + request);
        }
        CouchbaseBucketConfig config = (CouchbaseBucketConfig) bucket;
        DCPRequest dcpRequest = (DCPRequest) request;

        int nodeId = config.nodeIndexForMaster(dcpRequest.partition());
        if (nodeId == -2) {
            return;
        }
        if (nodeId == -1) {
            RetryHelper.retryOrCancel(env, request, responseBuffer);
            return;
        }

        NodeInfo nodeInfo = config.nodeAtIndex(nodeId);

        if (config.nodes().size() != nodes.size()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Node list and configuration's partition hosts sizes : {} <> {}, rescheduling",
                        nodes.size(), config.nodes().size());
            }
            RetryHelper.retryOrCancel(env, request, responseBuffer);
            return;
        }

        for (Node node : nodes) {
            if (node.hostname().equals(nodeInfo.hostname())) {
                node.send(request);
                return;
            }
        }

        throw new IllegalStateException("Node not found for request: " + request);
    }
}
