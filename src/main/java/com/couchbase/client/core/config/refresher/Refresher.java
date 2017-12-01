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
package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import rx.Observable;

/**
 * A {@link Refresher} keeps bucket configs up to date.
 *
 * The refresher is the companion behavior to the loader. The loader does the initial loading and the refresher does
 * its best to keep the {@link ConfigurationProvider} informed with an up-to-date configuration for a bucket.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public interface Refresher {

    /**
     * Returns the {@link Observable} which will push out new configuration updates.
     *
     * @return the config observable.
     */
    Observable<String> configs();

    /**
     * Registers a bucket to be watched for new configurations.
     *
     * @return true if it succeeded, a failing {@link Observable} otherwise with the cause.
     */
    @Deprecated
    Observable<Boolean> registerBucket(String name, String password);

    /**
     * Registers a bucket to be watched for new configurations.
     *
     * @return true if it succeeded, a failing {@link Observable} otherwise with the cause.
     */
    Observable<Boolean> registerBucket(String name, String username, String password);

    /**
     * De-registers a bucket from watching.
     *
     * @return true if succeeded, a failing {@link Observable} otherwise with the cause.
     */
    Observable<Boolean> deregisterBucket(String name);

    /**
     * Shuts down all open registration streams.
     *
     * @return true if succeeded, a failing {@link Observable} otherwise with the cause.
     */
    Observable<Boolean> shutdown();

    /**
     * Marks the given bucket as tainted.
     *
     * @param config the config of the bucket that should be marked.
     */
    void markTainted(BucketConfig config);

    /**
     * Mark the given bucket as not tainted.
     *
     * @param config the config of the bucket that should not be marked anymore.
     */
    void markUntainted(BucketConfig config);

    /**
     * If pull based, refresh configs for registered buckets.
     */
    void refresh(ClusterConfig config);

    void provider(ConfigurationProvider provider);

}
