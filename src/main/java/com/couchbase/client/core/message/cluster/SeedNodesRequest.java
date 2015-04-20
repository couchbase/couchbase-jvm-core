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
