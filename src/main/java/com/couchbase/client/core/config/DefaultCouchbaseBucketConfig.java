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
package com.couchbase.client.core.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultCouchbaseBucketConfig extends AbstractBucketConfig implements CouchbaseBucketConfig {

    private final CouchbasePartitionInfo partitionInfo;
    private final List<NodeInfo> partitionHosts;
    private final Set<InetAddress> nodesWithPrimaryPartitions;

    private final boolean tainted;
    private final long rev;

    /**
     * Creates a new {@link CouchbaseBucketConfig}.
     *
     * @param rev the revision of the config.
     * @param name the name of the bucket.
     * @param uri the URI for this bucket.
     * @param streamingUri the streaming URI for this bucket.
     * @param partitionInfo partition info for this bucket.
     * @param nodeInfos related node information.
     * @param portInfos port info for the nodes, including services.
     */
    @JsonCreator
    public DefaultCouchbaseBucketConfig(
        @JsonProperty("rev") long rev,
        @JsonProperty("name") String name,
        @JsonProperty("uri") String uri,
        @JsonProperty("streamingUri") String streamingUri,
        @JsonProperty("vBucketServerMap") CouchbasePartitionInfo partitionInfo,
        @JsonProperty("nodes") List<NodeInfo> nodeInfos,
        @JsonProperty("nodesExt") List<PortInfo> portInfos) {
        super(name, BucketNodeLocator.VBUCKET, uri, streamingUri, nodeInfos, portInfos);
        this.partitionInfo = partitionInfo;
        this.tainted = partitionInfo.tainted();
        this.partitionHosts = buildPartitionHosts(nodeInfos, partitionInfo);
        this.nodesWithPrimaryPartitions = buildNodesWithPrimaryPartitions(nodeInfos, partitionInfo.partitions());
        this.rev = rev;
    }

    /**
     * Pre-computes a set of nodes that have primary partitions active.
     *
     * @param nodeInfos the list of nodes.
     * @param partitions the partitions.
     * @return a set containing the addresses of nodes with primary partitions.
     */
    private static Set<InetAddress> buildNodesWithPrimaryPartitions(final List<NodeInfo> nodeInfos,
        final List<Partition> partitions) {
        Set<InetAddress> nodes = new HashSet<InetAddress>(nodeInfos.size());
        for (Partition partition : partitions) {
            int index = partition.master();
            if (index >= 0) {
                nodes.add(nodeInfos.get(index).hostname());
            }
        }
        return nodes;
    }

    /**
     * Helper method to reference the partition hosts from the raw node list.
     *
     * @param nodeInfos the node infos.
     * @param partitionInfo the partition info.
     * @return a ordered reference list for the partition hosts.
     */
    private static List<NodeInfo> buildPartitionHosts(List<NodeInfo> nodeInfos, CouchbasePartitionInfo partitionInfo) {
        List<NodeInfo> partitionHosts = new ArrayList<NodeInfo>();
        for (String rawHost : partitionInfo.partitionHosts()) {
            InetAddress convertedHost;
            try {
                convertedHost = InetAddress.getByName(rawHost);
            } catch (UnknownHostException e) {
                throw new ConfigurationException("Could not resolve " + rawHost + "on config building.");
            }
            for (NodeInfo nodeInfo : nodeInfos) {
                if (nodeInfo.hostname().equals(convertedHost)) {
                    partitionHosts.add(nodeInfo);
                }
            }
        }
        if (partitionHosts.size() != partitionInfo.partitionHosts().length) {
            throw new ConfigurationException("Partition size is not equal after conversion, this is a bug.");
        }
        return partitionHosts;
    }

    @Override
    public int numberOfReplicas() {
        return partitionInfo.numberOfReplicas();
    }

    @Override
    public boolean tainted() {
        return tainted;
    }

    @Override
    public boolean hasPrimaryPartitionsOnNode(final InetAddress hostname) {
        return nodesWithPrimaryPartitions.contains(hostname);
    }

    @Override
    public short nodeIndexForMaster(int partition) {
        return partitionInfo.partitions().get(partition).master();
    }

    @Override
    public short nodeIndexForReplica(int partition, int replica) {
        return partitionInfo.partitions().get(partition).replica(replica);
    }

    @Override
    public int numberOfPartitions() {
        return partitionInfo.partitions().size();
    }

    @Override
    public NodeInfo nodeAtIndex(int nodeIndex) {
        return partitionHosts.get(nodeIndex);
    }

    @Override
    public long rev() {
        return rev;
    }

    @Override
    public BucketType type() {
        return BucketType.COUCHBASE;
    }

    @Override
    public String toString() {
        return "DefaultCouchbaseBucketConfig{"
            + "name='" + name() + '\''
            + ", locator=" + locator()
            + ", uri='" + uri() + '\''
            + ", streamingUri='" + streamingUri() + '\''
            + ", nodeInfo=" + nodes()
            + ", partitionInfo=" + partitionInfo
            + ", tainted=" + tainted
            + ", rev=" + rev + '}';
    }
}
