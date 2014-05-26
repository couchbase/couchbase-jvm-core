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

import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.env.Environment;
import org.junit.Test;
import rx.Observable;

import static org.mockito.Mockito.mock;

/**
 * Verifies the correct functionality of the {@link DefaultConfigurationProvider}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class DefaultConfigurationProviderTest {

    @Test
    public void shouldOpenBucket() {
        Cluster cluster = mock(Cluster.class);
        Environment environment = new CouchbaseEnvironment();
        ConfigurationProvider provider = new DefaultConfigurationProvider(cluster, environment);

        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        System.out.println(configObservable.toBlockingObservable().first());
        // 1) open a bucket and check if the config looks good
    }

    @Test
    public void shouldEmitNewClusterConfig() {
        Cluster cluster = mock(Cluster.class);
        Environment environment = new CouchbaseEnvironment();
        ConfigurationProvider provider = new DefaultConfigurationProvider(cluster, environment);

        // 1) open a bucket
        // 2) check if it comes in over the observable
        // 3) check if the config returned and passed in are the same
    }

    @Test
    public void shouldFailOpeningBucketIfNoConfigLoaded() {

    }

    @Test
    public void shouldCloseBucket() {

    }

    @Test
    public void shouldCloseBuckets() {

    }

    @Test
    public void shouldAcceptProposedConfig() {

    }
}