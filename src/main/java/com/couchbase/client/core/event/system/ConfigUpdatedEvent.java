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
package com.couchbase.client.core.event.system;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.utils.Events;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
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
    private final Set<String> clusterNodes;

    public ConfigUpdatedEvent(final ClusterConfig clusterConfig) {
        this.bucketNames = clusterConfig.bucketConfigs().keySet();

        Set<String> nodes = new HashSet<>();
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
     *
     * @deprecated please use {@link #clusterNodesAsStrings()} instead.
     */
    @Deprecated
    public Set<InetAddress> clusterNodes() {
        Set<InetAddress> nodes = new HashSet<InetAddress>();
        for (String na : clusterNodes) {
            try {
                nodes.add(InetAddress.getByName(na));
            } catch (UnknownHostException e) {
                throw new IllegalStateException(e);
            }

        }
        return nodes;
    }

    /**
     * Returns the hostname of all nodes that are part of the cluster config.
     */
    public Set<String> clusterNodesAsStrings() {
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
        result.put("clusterNodes", Collections.unmodifiableSet(clusterNodes));
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConfigUpdatedEvent{");
        sb.append("bucketNames=").append(bucketNames);
        sb.append(", clusterNodes=").append(clusterNodes);
        sb.append('}');
        return sb.toString();
    }
}
