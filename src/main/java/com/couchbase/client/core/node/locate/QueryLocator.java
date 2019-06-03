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
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.PrelocatedRequest;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.retry.RetryHelper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.utils.MathUtils;
import com.lmax.disruptor.RingBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.core.logging.RedactableArgument.system;
import static com.couchbase.client.core.logging.RedactableArgument.user;

public class QueryLocator implements Locator {

    /**
     * The Logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(QueryLocator.class);

    /**
     * The Counter used to distribute query request.
     */
    private final AtomicLong counter;

    /**
     * Generates the random initial value for the round robin counter used.
     *
     * This will generate a random number between 0 and 1023 which is probably
     * enough distribution to not make all queries hit the same first server
     * all the time.
     */
    public QueryLocator() {
        this(new Random().nextInt(1024));
    }

    /**
     * Constructor which allows to set the initial value to something specific
     * so it can be tested in a deterministic fashion. Should only be used
     * for testing though.
     *
     * @param initialValue the initial value to use.
     */
    QueryLocator(final long initialValue) {
        counter = new AtomicLong(initialValue);
    }

    @Override
    public void locateAndDispatch(CouchbaseRequest request, List<Node> nodes, ClusterConfig config, CoreEnvironment env,
        RingBuffer<ResponseEvent> responseBuffer) {

        nodes = filterNodes(nodes);
        if (nodes.isEmpty()) {
            RetryHelper.retryOrCancel(env, request, responseBuffer);
            return;
        }

        if (request instanceof PrelocatedRequest && ((PrelocatedRequest) request).sendTo() != null) {
            String target = ((PrelocatedRequest) request).sendTo().getHostAddress();
            for (Node node : nodes) {
                if (node.hostname().equals(target)) {
                    node.send(request);
                    return;
                }
            }

            RetryHelper.retryOrCancel(env, request, responseBuffer);
            return;
        }

        int nodeSize = nodes.size();
        int offset = (int) MathUtils.floorMod(counter.getAndIncrement(), nodeSize);
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

    private List<Node> filterNodes(final List<Node> allNodes) {
        List<Node> result = new ArrayList<Node>(allNodes.size());
        for (Node n : allNodes) {
            if (checkNode(n)) {
                result.add(n);
            }
        }
        return result;
    }

    protected boolean checkNode(final Node node) {
        return node.serviceEnabled(ServiceType.QUERY);
    }
}
