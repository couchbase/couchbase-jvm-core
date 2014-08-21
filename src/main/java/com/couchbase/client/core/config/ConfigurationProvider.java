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

import rx.Observable;

import java.net.InetAddress;
import java.util.Set;

public interface ConfigurationProvider {

    /**
     * Returns an {@link Observable}, which pushes a new {@link ClusterConfig} once available.
     *
     * @return the configuration.
     */
    Observable<ClusterConfig> configs();

    /**
     * Returns the current config or null if not set.
     *
     * @return returns the current cluster config.
     */
    ClusterConfig config();

    /**
     * Set the initial seed hosts for bootstrap.
     *
     * This should only be done as long as the {@link ConfigurationProvider} is not bootstrapped, otherwise it
     * might be ignored.
     *
     * @param hosts list of seed hosts.
     * @param shuffle shuffle seed host list.
     * @return true if host list updated, false otherwise.
     */
    boolean seedHosts(Set<InetAddress> hosts, boolean shuffle);

    /**
     * Start to fetch a config for the given bucket and also watch for changes, depending on the mechanism
     * used.
     *
     * @param name the name of the bucket.
     * @param password the name of the password.
     * @return an observable with the configuration if success, and failures otherwise.
     */
    Observable<ClusterConfig> openBucket(String name, String password);

    Observable<ClusterConfig> closeBucket(String name);

    Observable<Boolean> closeBuckets();

    void proposeBucketConfig(String bucket, String config);

    void signalOutdated();
}
