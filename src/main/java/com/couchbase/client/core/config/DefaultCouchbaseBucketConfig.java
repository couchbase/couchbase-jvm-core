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

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultCouchbaseBucketConfig extends AbstractBucketConfig implements CouchbaseBucketConfig {

    private final PartitionInfo partitionInfo;
    private final boolean tainted;
    private final long rev;

    @JsonCreator
    public DefaultCouchbaseBucketConfig(
        @JsonProperty("rev") long rev,
        @JsonProperty("name") String name,
        @JsonProperty("nodeLocator") String locator,
        @JsonProperty("uri") String uri,
        @JsonProperty("streamingUri") String streamingUri,
        @JsonProperty("vBucketServerMap") PartitionInfo partitionInfo,
        @JsonProperty("nodes") List<NodeInfo> nodeInfos,
        @JsonProperty("nodesExt") List<PortInfo> portInfos) {
        super(name, BucketNodeLocator.fromConfig(locator), uri, streamingUri, nodeInfos, portInfos);
        this.partitionInfo = partitionInfo;
        this.tainted = !partitionInfo.forwardPartitions().isEmpty();
        this.rev = rev;
    }

    @Override
    public List<String> partitionHosts() {
        return partitionInfo.partitionHosts();
    }

    @Override
    public List<Partition> partitions() {
        return partitionInfo.partitions();
    }

    @Override
    public int numberOfReplicas() {
        return partitionInfo.numberOfReplicas();
    }

    @Override
    public boolean tainted() {
        return tainted;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class PartitionInfo {

        private final int numberOfReplicas;
        private final List<String> partitionHosts;
        private final List<Partition> partitions;
        private final List<Partition> forwardPartitions;

        PartitionInfo(
            @JsonProperty("numReplicas") int numberOfReplicas,
            @JsonProperty("serverList") List<String> partitionHosts,
            @JsonProperty("vBucketMap") List<List<Short>> partitions,
            @JsonProperty("vBucketMapForward") List<List<Short>> forwardPartitions) {
            this.numberOfReplicas = numberOfReplicas;
            trimPort(partitionHosts);
            this.partitionHosts = partitionHosts;
            this.partitions = fromPartitionList(partitions);
            this.forwardPartitions = fromPartitionList(forwardPartitions);
        }

        public int numberOfReplicas() {
            return numberOfReplicas;
        }

        public List<String> partitionHosts() {
            return partitionHosts;
        }

        public List<Partition> partitions() {
            return partitions;
        }

        public List<Partition> forwardPartitions() {
            return forwardPartitions;
        }

        private static void trimPort(List<String> input) {
            for (int i = 0; i < input.size(); i++) {
                String[] parts =  input.get(i).split(":");
                input.set(i, parts[0]);
            }
        }

        private static List<Partition> fromPartitionList(List<List<Short>> input) {
            List<Partition> partitions = new ArrayList<Partition>();
            if (input == null) {
                return partitions;
            }

            for (List<Short> partition : input) {
                short master = partition.remove(0);
                short[] replicas = new short[partition.size()];
                int i = 0;
                for (short replica : partition) {
                    replicas[i++] = replica;
                }
                partitions.add(new DefaultPartition(master, replicas));
            }
            return partitions;
        }

        @Override
        public String toString() {
            return "PartitionInfo{" +
                "numberOfReplicas=" + numberOfReplicas +
                ", partitionHosts=" + partitionHosts +
                ", partitions=" + partitions +
                ", forwardPartitions=" + forwardPartitions +
                '}';
        }
    }

    @Override
    public long rev() {
        return rev;
    }

    @Override
    public String toString() {
        return "DefaultCouchbaseBucketConfig{" +
            "name='" + name() + '\'' +
            ", locator=" + locator() +
            ", uri='" + uri() + '\'' +
            ", streamingUri='" + streamingUri() + '\'' +
            ", nodeInfo=" + nodes() +
            ", partitionInfo=" + partitionInfo +
            ", tainted=" + tainted +
            ", rev=" + rev +
            '}';
    }
}
