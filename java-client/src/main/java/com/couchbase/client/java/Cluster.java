package com.couchbase.client.java;

import com.couchbase.client.java.bucket.Bucket;
import reactor.core.composable.Promise;

/**
 * The entry point when talking to a cluster.
 *
 * Defines all operations that can be performed against the whole cluster, including getting a reference to an actual
 * bucket where data operations are performed against.
 */
public interface Cluster {

    /**
     * Open a bucket for operations against it.
     *
     * @param name the name of the bucket.
     * @return the connected bucket reference.
     */
    Promise<Bucket> openBucket(String name);

    /**
     * Open a bucket for operations against it.
     *
     * @param name the name of the bucket.
     * @param password the password of the bucket.
     * @return the connected bucket reference.
     */
    Promise<Bucket> openBucket(String name, String password);

    /**
     * Disconnect from the cluster and close all opened buckets.
     *
     * @return a promise indicating shutdown has finished.
     */
    Promise<Void> disconnect();

}
