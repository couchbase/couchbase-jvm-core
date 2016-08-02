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
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.GetDesignDocumentsRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.ServiceType;
import com.lmax.disruptor.RingBuffer;

import java.net.InetAddress;
import java.util.List;

public class ConfigLocator implements Locator {

    private long counter = 0;

    @Override
    public void locateAndDispatch(final CouchbaseRequest request, final List<Node> nodes, final ClusterConfig config,
        CoreEnvironment env, RingBuffer<ResponseEvent> responseBuffer) {
        if (request instanceof BucketConfigRequest) {
            BucketConfigRequest req = (BucketConfigRequest) request;
            InetAddress hostname = req.hostname();
            for (Node node : nodes) {
                if (hostname == null || node.hostname().equals(hostname)) {
                    node.send(request);
                    return;
                }
            }
        } else {
            int nodeSize = nodes.size();
            int offset = (int) counter++ % nodeSize;

            for (int i = offset; i < nodeSize; i++) {
                Node node = nodes.get(i);
                if (checkNode(node, request)) {
                    node.send(request);
                    return;
                }
            }

            for (int i = 0; i < offset; i++) {
                Node node = nodes.get(i);
                if (checkNode(node, request)) {
                    node.send(request);
                    return;
                }
            }
        }

        RetryHelper.retryOrCancel(env, request, responseBuffer);
    }

    protected boolean checkNode(final Node node, final CouchbaseRequest request) {
        return !(request instanceof GetDesignDocumentsRequest)
                || node.serviceEnabled(ServiceType.VIEW);
    }

}
