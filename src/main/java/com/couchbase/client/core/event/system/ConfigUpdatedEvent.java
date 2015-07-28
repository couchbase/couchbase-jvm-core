/**
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.client.core.event.system;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.utils.Events;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Event published when a new bucket config is applied to the core.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class ConfigUpdatedEvent implements CouchbaseEvent {

    private final Set<String> bucketNames;
    private final Set<InetAddress> clusterNodes;

    public ConfigUpdatedEvent(final ClusterConfig clusterConfig) {
        this.bucketNames = clusterConfig.bucketConfigs().keySet();

        Set<InetAddress> nodes = new HashSet<InetAddress>();
        for (Map.Entry<String, BucketConfig> cfg : clusterConfig.bucketConfigs().entrySet()) {
            for (NodeInfo node : cfg.getValue().nodes()) {
                nodes.add(node.hostname());
            }
        }
        this.clusterNodes = nodes;
    }

    /**
     * Returns all open bucket names.
     */
    public Set<String> openBuckets() {
        return bucketNames;
    }

    /**
     * Returns the {@link InetAddress} of all nodes that are part of the cluster config.
     */
    public Set<InetAddress> clusterNodes() {
        return clusterNodes;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = Events.identityMap(this);
        result.put("openBuckets", openBuckets());

        Set<String> clusterNodes = new HashSet<String>();
        for (InetAddress node : clusterNodes()) {
            clusterNodes.add(node.toString());
        }
        result.put("clusterNodes", clusterNodes);

        return result;
    }

}
