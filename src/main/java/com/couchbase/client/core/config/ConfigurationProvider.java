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

    /**
     * Start to fetch a config for the given bucket and also watch for changes, depending on the mechanism
     * used.
     *
     * @param name the name of the bucket.
     * @param username the user authorized for bucket access.
     * @param password the password of the user.
     * @return an observable with the configuration if success, and failures otherwise.
     */
    Observable<ClusterConfig> openBucket(String name, String username, String password);

    Observable<ClusterConfig> closeBucket(String name);

    Observable<Boolean> closeBuckets();

    void proposeBucketConfig(String bucket, String config);

    void signalOutdated();
}
