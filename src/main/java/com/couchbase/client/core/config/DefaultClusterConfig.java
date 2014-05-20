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

import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of a {@link ClusterConfig}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class DefaultClusterConfig implements ClusterConfig {

    /**
     * Holds all current bucket configurations.
     */
    private final Map<String, BucketConfig> bucketConfigs;

    /**
     * Creates a new {@link DefaultClusterConfig}.
     */
    public DefaultClusterConfig() {
        bucketConfigs = new HashMap<String, BucketConfig>();
    }

    @Override
    public BucketConfig bucketConfig(final String bucketName) {
        return bucketConfigs.get(bucketName);
    }

    @Override
    public void setBucketConfig(final String bucketName, final BucketConfig config) {
        bucketConfigs.put(bucketName, config);
    }

    @Override
    public void deleteBucketConfig(String bucketName) {
        bucketConfigs.remove(bucketName);
    }

    @Override
    public boolean hasBucket(final String bucketName) {
        return bucketConfigs.containsKey(bucketName);
    }

    @Override
    public Map<String, BucketConfig> bucketConfigs() {
        return bucketConfigs;
    }
}
