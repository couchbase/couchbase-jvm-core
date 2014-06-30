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
    Observable<BucketConfig> configs();

    /**
     * Registers a bucket to be watched for new configurations.
     *
     * @return true if it succeeded, a failing {@link Observable} otherwise with the cause.
     */
    Observable<Boolean> registerBucket(String name, String password);

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
     * @param config
     */
    void markTainted(BucketConfig config);

    /**
     * Mark the given bucket as not tainted.
     *
     * @param config
     */
    void markUntainted(BucketConfig config);

    /**
     * If pull based, refresh configs for registered buckets.
     */
    void refresh(ClusterConfig config);

    void provider(ConfigurationProvider provider);

}
