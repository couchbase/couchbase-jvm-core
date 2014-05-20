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
