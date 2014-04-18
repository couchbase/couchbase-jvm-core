package com.couchbase.client.core.config;

import java.util.List;

public interface CouchbaseBucketConfig extends BucketConfig {

    /**
     * Returns the hosts for the partition map.
     *
     * @return list of hostnames.
     */
    List<String> partitionHosts();

    /**
     * All partitions, sorted by their partition index.
     *
     * @return all partitions.
     */
    List<Partition> partitions();

    /**
     * The number of configured replicas for this bucket.
     *
     * @return number of replicas.
     */
    int numberOfReplicas();
}
