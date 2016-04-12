/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
