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
import com.couchbase.client.core.config.loader.Loader;
import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.env.Environment;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.subjects.AsyncSubject;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link DefaultConfigurationProvider}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class DefaultConfigurationProviderTest {

    @Test
    @SuppressWarnings("unchecked")
    public void shouldOpenBucket() {
        Cluster cluster = mock(Cluster.class);
        Environment environment = new CouchbaseEnvironment();
        Loader loader = mock(Loader.class);
        when(loader.loadConfig(any(Set.class), anyString(), anyString()))
            .thenReturn(Observable.from(mock(BucketConfig.class)));

        ConfigurationProvider provider = new DefaultConfigurationProvider(cluster, environment, Arrays.asList(loader));

        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlockingObservable().first();
        assertTrue(config.hasBucket("bucket"));
        assertFalse(config.hasBucket("other"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateLoadingToSecondProviderIfFirstFails() {
        Cluster cluster = mock(Cluster.class);
        Environment environment = new CouchbaseEnvironment();

        Loader successLoader = mock(Loader.class);
        Loader errorLoader = mock(Loader.class);
        when(successLoader.loadConfig(any(Set.class), anyString(), anyString()))
            .thenReturn(Observable.from(mock(BucketConfig.class)));
        AsyncSubject<BucketConfig> errorSubject = AsyncSubject.create();
        when(errorLoader.loadConfig(any(Set.class), anyString(), anyString())).thenReturn((Observable) errorSubject);
        errorSubject.onError(new IllegalStateException());

        ConfigurationProvider provider = new DefaultConfigurationProvider(cluster, environment,
            Arrays.asList(errorLoader, successLoader));

        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlockingObservable().first();
        assertTrue(config.hasBucket("bucket"));
        assertFalse(config.hasBucket("other"));
    }

    @Test
    public void shouldEmitNewClusterConfig() throws Exception {
        final Cluster cluster = mock(Cluster.class);
        Environment environment = new CouchbaseEnvironment();
        Loader loader = mock(Loader.class);
        when(loader.loadConfig(any(Set.class), anyString(), anyString()))
            .thenReturn(Observable.from(mock(BucketConfig.class)));

        ConfigurationProvider provider = new DefaultConfigurationProvider(cluster, environment, Arrays.asList(loader));
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ClusterConfig> configReference = new AtomicReference<ClusterConfig>();
        provider.configs().subscribe(new Action1<ClusterConfig>() {
            @Override
            public void call(ClusterConfig clusterConfig) {
                configReference.set(clusterConfig);
                latch.countDown();
            }
        });

        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlockingObservable().first();
        assertTrue(config.hasBucket("bucket"));
        assertFalse(config.hasBucket("other"));
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(configReference.get(), config);
    }

    @Test
    public void shouldFailOpeningBucketIfNoConfigLoaded() {
        Cluster cluster = mock(Cluster.class);
        Environment environment = new CouchbaseEnvironment();
        Loader errorLoader = mock(Loader.class);
        AsyncSubject<BucketConfig> errorSubject = AsyncSubject.create();
        when(errorLoader.loadConfig(any(Set.class), anyString(), anyString())).thenReturn(errorSubject);
        errorSubject.onError(new IllegalStateException());
        ConfigurationProvider provider = new DefaultConfigurationProvider(cluster, environment,
            Arrays.asList(errorLoader));

        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        try {
            configObservable.toBlockingObservable().single();
            assertTrue(false);
        } catch(ConfigurationException ex) {
            assertEquals("Could not open bucket.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
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