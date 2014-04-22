package com.couchbase.client.core.config;

/**
 * Represents a Couchbase Cluster Configuration.
 *
 * Depending on what buckets are used, a {@link ClusterConfig} has 0 to N {@link BucketConfig}s associated with it.
 */
public interface ClusterConfig {

    /**
     * Returns the {@link BucketConfig} for the given bucket name.
     *
     * @param bucketName name of the bucket.
     * @return a config, if set.
     */
    BucketConfig bucketConfig(String bucketName);

    void setBucketConfig(String bucketName, BucketConfig config);

    /**
     * True if there is a bucket config with the given name, false otherwise.
     *
     * @param bucketName name of the bucket.
     * @return true if bucket is there.
     */
    boolean hasBucket(String bucketName);
}
