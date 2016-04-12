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
package com.couchbase.client.core.config;

import java.util.Map;

/**
 * Represents a Couchbase Cluster Configuration.
 *
 * Depending on what buckets are used, a {@link ClusterConfig} has 0 to N {@link BucketConfig}s associated with it.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface ClusterConfig {

    /**
     * Returns the {@link BucketConfig} for the given bucket name.
     *
     * @param bucketName name of the bucket.
     * @return a config, if set.
     */
    BucketConfig bucketConfig(String bucketName);

    /**
     * Set a bucket config for the given bucket name.
     *
     * @param bucketName the name of the bucket.
     * @param config the configuration associated with the bucket.
     */
    void setBucketConfig(String bucketName, BucketConfig config);

    void deleteBucketConfig(String bucketName);

    /**
     * True if there is a bucket config with the given name, false otherwise.
     *
     * @param bucketName name of the bucket.
     * @return true if bucket is there.
     */
    boolean hasBucket(String bucketName);

    Map<String, BucketConfig> bucketConfigs();
}
