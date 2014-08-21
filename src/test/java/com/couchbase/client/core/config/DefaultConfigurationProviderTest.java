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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.loader.Loader;
import com.couchbase.client.core.config.refresher.Refresher;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import rx.Observable;
import rx.functions.Action1;
import rx.subjects.AsyncSubject;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
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

    private static final CoreEnvironment environment = DefaultCoreEnvironment.create();

    @Test
    @SuppressWarnings("unchecked")
    public void shouldOpenBucket() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Loader loader = mock(Loader.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        when(loader.loadConfig(any(Set.class), anyString(), anyString()))
            .thenReturn(Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig)));

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<BucketConfig>empty());
        when(refresher.registerBucket(anyString(), anyString())).thenReturn(Observable.just(true));

        ConfigurationProvider provider = new DefaultConfigurationProvider(
            cluster,
            environment,
            Arrays.asList(loader),
            new HashMap<LoaderType, Refresher>() {{
                put(LoaderType.Carrier, refresher);
            }}
        );

        provider.seedHosts(Sets.newSet(InetAddress.getByName("localhost")));
        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlocking().first();
        assertTrue(config.hasBucket("bucket"));
        assertFalse(config.hasBucket("other"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateLoadingToSecondProviderIfFirstFails() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Loader successLoader = mock(Loader.class);
        Loader errorLoader = mock(Loader.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        when(successLoader.loadConfig(any(Set.class), anyString(), anyString()))
            .thenReturn(Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig)));
        AsyncSubject<BucketConfig> errorSubject = AsyncSubject.create();
        when(errorLoader.loadConfig(any(Set.class), anyString(), anyString())).thenReturn((Observable) errorSubject);
        errorSubject.onError(new IllegalStateException());

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<BucketConfig>empty());
        when(refresher.registerBucket(anyString(), anyString())).thenReturn(Observable.just(true));

        ConfigurationProvider provider = new DefaultConfigurationProvider(
            cluster,
            environment,
            Arrays.asList(errorLoader, successLoader),
            new HashMap<LoaderType, Refresher>() {{
                put(LoaderType.Carrier, refresher);
                put(LoaderType.HTTP, refresher);
            }}
        );

        provider.seedHosts(Sets.newSet(InetAddress.getByName("localhost")));
        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlocking().first();
        assertTrue(config.hasBucket("bucket"));
        assertFalse(config.hasBucket("other"));
    }

    @Test
    public void shouldEmitNewClusterConfig() throws Exception {
        final ClusterFacade cluster = mock(ClusterFacade.class);
        Loader loader = mock(Loader.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        when(loader.loadConfig(any(Set.class), anyString(), anyString()))
            .thenReturn(Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig)));


        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<BucketConfig>empty());
        when(refresher.registerBucket(anyString(), anyString())).thenReturn(Observable.just(true));

        ConfigurationProvider provider = new DefaultConfigurationProvider(
            cluster,
            environment,
            Arrays.asList(loader),
            new HashMap<LoaderType, Refresher>() {{
                put(LoaderType.Carrier, refresher);
            }}
        );
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ClusterConfig> configReference = new AtomicReference<ClusterConfig>();
        provider.configs().subscribe(new Action1<ClusterConfig>() {
            @Override
            public void call(ClusterConfig clusterConfig) {
                configReference.set(clusterConfig);
                latch.countDown();
            }
        });

        provider.seedHosts(Sets.newSet(InetAddress.getByName("localhost")));
        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlocking().first();
        assertTrue(config.hasBucket("bucket"));
        assertFalse(config.hasBucket("other"));
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(configReference.get(), config);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldFailOpeningBucketIfNoConfigLoaded() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Loader errorLoader = mock(Loader.class);
        AsyncSubject<Tuple2<LoaderType, BucketConfig>> errorSubject = AsyncSubject.create();
        when(errorLoader.loadConfig(any(Set.class), anyString(), anyString())).thenReturn(errorSubject);
        errorSubject.onError(new IllegalStateException());

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<BucketConfig>empty());
        when(refresher.registerBucket(anyString(), anyString())).thenReturn(Observable.just(true));

        ConfigurationProvider provider = new DefaultConfigurationProvider(
            cluster,
            environment,
            Arrays.asList(errorLoader),
            new HashMap<LoaderType, Refresher>() {{
                put(LoaderType.Carrier, refresher);
            }}
        );

        provider.seedHosts(Sets.newSet(InetAddress.getByName("localhost")));
        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        try {
            configObservable.toBlocking().single();
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