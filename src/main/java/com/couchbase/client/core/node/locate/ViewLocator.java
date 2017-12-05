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
import com.couchbase.client.core.ServiceNotAvailableException;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.MathUtils;
import com.lmax.disruptor.RingBuffer;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.core.logging.RedactableArgument.system;
import static com.couchbase.client.core.logging.RedactableArgument.user;

public class ViewLocator implements Locator {

    /**
     * The Logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ViewLocator.class);

    private static final ServiceNotAvailableException NOT_AVAILABLE =
        new ServiceNotAvailableException("Views are not available on this bucket type.");

    private volatile long counter = 0;

    @Override
    public void locateAndDispatch(CouchbaseRequest request, List<Node> nodes, ClusterConfig config,
        CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {
        BucketConfig bucketConfig = config.bucketConfig(request.bucket());
        if (!(bucketConfig instanceof CouchbaseBucketConfig)) {
            request.observable().onError(NOT_AVAILABLE);
            return;
        }

        nodes = filterNodes(nodes, (CouchbaseBucketConfig) bucketConfig);
        if (nodes.isEmpty()) {
            RetryHelper.retryOrCancel(env, request, responseBuffer);
            return;
        }

        int nodeSize = nodes.size();
        int offset = (int) MathUtils.floorMod(counter++, nodeSize);
        Node node = nodes.get(offset);
        if (node != null) {
            node.send(request);
        } else {
            LOGGER.warn("Locator found selected node to be null, this is a bug. {}, {}",
                user(request),
                system(nodes)
            );
            RetryHelper.retryOrCancel(env, request, responseBuffer);
        }
    }

    private List<Node> filterNodes(final List<Node> allNodes, final CouchbaseBucketConfig cfg) {
        List<Node> result = new ArrayList<Node>(allNodes.size());
        for (Node n : allNodes) {
            if (checkNode(n, cfg)) {
                result.add(n);
            }
        }
        return result;
    }

    protected boolean checkNode(final Node node, CouchbaseBucketConfig config) {
        return node.serviceEnabled(ServiceType.VIEW) && config.hasPrimaryPartitionsOnNode(node.hostname());
    }
}
