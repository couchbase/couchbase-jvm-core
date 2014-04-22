package com.couchbase.client.core.config;

import java.util.HashMap;
import java.util.Map;

public class DefaultClusterConfig implements ClusterConfig {

    private final Map<String, BucketConfig> bucketConfigs;

    public DefaultClusterConfig() {
        bucketConfigs = new HashMap<String, BucketConfig>();
    }

    @Override
    public BucketConfig bucketConfig(String bucketName) {
        return bucketConfigs.get(bucketName);
    }

    @Override
    public void setBucketConfig(String bucketName, BucketConfig config) {
        bucketConfigs.put(bucketName, config);
    }

    @Override
    public boolean hasBucket(String bucketName) {
        return bucketConfigs.containsKey(bucketName);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DefaultClusterConfig{");
        sb.append("bucketConfigs=").append(bucketConfigs);
        sb.append('}');
        return sb.toString();
    }
}
