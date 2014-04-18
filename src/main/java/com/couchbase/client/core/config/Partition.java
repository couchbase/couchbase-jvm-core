package com.couchbase.client.core.config;

public interface Partition {

    /**
     * Returns the master node index for this partition.
     *
     * @return the master node index.
     */
    short master();

    /**
     * Returns the replica index for this partition.
     *
     * @param num number of replica.
     * @return the replica node index, -1 if not set.
     */
    short replica(int num);
}
