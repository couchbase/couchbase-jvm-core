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
package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * **Set up the bootstrap nodes for a {@link ClusterFacade}.**
 *
 * For stability reasons, it is advised to always provide more than one seed node (but not necessarily all nodes from
 * the cluster) so that the cluster can correctly bootstrap the bucket, even if one of the hosts in the list is
 * currently not available.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class SeedNodesRequest extends AbstractCouchbaseRequest implements ClusterRequest {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(SeedNodesRequest.class);

    /**
     * The default hostname which will be used if the default constructor is used.
     */
    private static final String DEFAULT_HOSTNAME = "localhost";

    /**
     * The list of hostnames/IPs.
     */
    private Set<InetAddress> nodes;

    /**
     * Creates a {@link SeedNodesRequest} with the default hostname ("localhost").
     */
    public SeedNodesRequest() {
        this(DEFAULT_HOSTNAME);
    }

    /**
     * Creates a {@link SeedNodesRequest} with the given hostnames.
     *
     * @param nodes the seed node hostnames.
     */
    public SeedNodesRequest(final String... nodes) {
        this(Arrays.asList(nodes));
    }

    /**
     * Creates a {@link SeedNodesRequest} with the given list of hostnames.
     *
     * @param nodes the seed node hostnames.
     */
    public SeedNodesRequest(final List<String> nodes) {
        super(null, null);

        if (nodes == null || nodes.isEmpty()) {
            throw new ConfigurationException("Empty or null bootstrap list provided.");
        }
        Set<InetAddress> parsedNodes = new HashSet<InetAddress>();
        for (String node : nodes) {
            if (node == null || node.isEmpty()) {
                LOGGER.info("Empty or null host in bootstrap list.");
                continue;
            }

            try {
                parsedNodes.add(InetAddress.getByName(node));
            } catch (UnknownHostException e) {
                LOGGER.info("Unknown host " + node + " in bootstrap list.", e);
            }
        }

        if (parsedNodes.isEmpty()) {
            throw new ConfigurationException("No valid node found to bootstrap from. "
                + "Please check your network configuration.");
        }
        this.nodes = parsedNodes;
    }

    /**
     * Returns the set list of seed hostnames.
     *
     * @return the list of hostnames.
     */
    public Set<InetAddress> nodes() {
        return nodes;
    }
}
