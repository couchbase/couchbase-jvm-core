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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.AsyncSubject;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
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
        when(loader.loadConfig(any(InetAddress.class), anyString(), anyString()))
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

        provider.seedHosts(Sets.newSet(InetAddress.getByName("localhost")), true);
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
        when(successLoader.loadConfig(any(InetAddress.class), anyString(), anyString()))
            .thenReturn(Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig)));
        AsyncSubject<BucketConfig> errorSubject = AsyncSubject.create();
        when(errorLoader.loadConfig(any(InetAddress.class), anyString(), anyString())).thenReturn((Observable) errorSubject);
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

        provider.seedHosts(Sets.newSet(InetAddress.getByName("localhost")), true);
        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlocking().first();
        assertTrue(config.hasBucket("bucket"));
        assertFalse(config.hasBucket("other"));
    }

    @Test
    public void shouldOpenBucketIfSubsetOfNodesIsFailing() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<BucketConfig>empty());
        when(refresher.registerBucket(anyString(), anyString())).thenReturn(Observable.just(true));

        Loader carrierLoader = mock(Loader.class);
        Loader httpLoader = mock(Loader.class);

        final InetAddress goodNode = InetAddress.getByName("5.6.7.8");
        InetAddress badNode = InetAddress.getByName("1.2.3.4");


        when(carrierLoader.loadConfig(any(InetAddress.class), any(String.class), any(String.class)))
            .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                @Override
                public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                    InetAddress target = (InetAddress) in.getArguments()[0];

                    if (target.equals(goodNode)) {
                        final BucketConfig bucketConfig = mock(BucketConfig.class);
                        when(bucketConfig.name()).thenReturn("bucket-carrier-"+target.getHostAddress());
                        return Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig));
                    } else {
                        return Observable.error(new Exception("Could not load config for some reason."));
                    }
                }
            });

        when(httpLoader.loadConfig(any(InetAddress.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        InetAddress target = (InetAddress) in.getArguments()[0];

                        if (target.equals(goodNode)) {
                            final BucketConfig bucketConfig = mock(BucketConfig.class);
                            when(bucketConfig.name()).thenReturn("bucket-http-"+target.getHostAddress());
                            return Observable.just(Tuple.create(LoaderType.HTTP, bucketConfig));
                        } else {
                            return Observable.error(new Exception("Could not load config for some reason."));
                        }
                    }
                });

        ConfigurationProvider provider = new DefaultConfigurationProvider(
                cluster,
                environment,
                Arrays.asList(carrierLoader, httpLoader),
                new HashMap<LoaderType, Refresher>() {{
                    put(LoaderType.Carrier, refresher);
                    put(LoaderType.HTTP, refresher);
                }}
        );

        provider.seedHosts(Sets.newSet(badNode, goodNode), true);

        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlocking().first();

        assertEquals(1, config.bucketConfigs().size());
        assertTrue(config.hasBucket("bucket-carrier-" + goodNode.getHostAddress()));
    }

    @Test
    public void shouldOpenBucketIfSubsetOfNodesIsFailingAndOnlyHttpAvailable() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<BucketConfig>empty());
        when(refresher.registerBucket(anyString(), anyString())).thenReturn(Observable.just(true));

        Loader carrierLoader = mock(Loader.class);
        Loader httpLoader = mock(Loader.class);

        final InetAddress goodNode = InetAddress.getByName("5.6.7.8");
        InetAddress badNode = InetAddress.getByName("1.2.3.4");

        when(carrierLoader.loadConfig(any(InetAddress.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        return Observable.error(new Exception("Could not load config for some reason."));
                    }
                });

        when(httpLoader.loadConfig(any(InetAddress.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        InetAddress target = (InetAddress) in.getArguments()[0];

                        if (target.equals(goodNode)) {
                            final BucketConfig bucketConfig = mock(BucketConfig.class);
                            when(bucketConfig.name()).thenReturn("bucket-http-"+target.getHostAddress());
                            return Observable.just(Tuple.create(LoaderType.HTTP, bucketConfig));
                        } else {
                            return Observable.error(new Exception("Could not load config for some reason."));
                        }
                    }
                });

        ConfigurationProvider provider = new DefaultConfigurationProvider(
                cluster,
                environment,
                Arrays.asList(carrierLoader, httpLoader),
                new HashMap<LoaderType, Refresher>() {{
                    put(LoaderType.Carrier, refresher);
                    put(LoaderType.HTTP, refresher);
                }}
        );

        provider.seedHosts(Sets.newSet(badNode, goodNode), true);

        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlocking().first();

        assertEquals(1, config.bucketConfigs().size());
        assertTrue(config.hasBucket("bucket-http-" + goodNode.getHostAddress()));
    }

    @Test
    public void shouldOpenBucketIfSubsetOfNodesIsNotResponding() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<BucketConfig>empty());
        when(refresher.registerBucket(anyString(), anyString())).thenReturn(Observable.just(true));

        Loader carrierLoader = mock(Loader.class);
        Loader httpLoader = mock(Loader.class);

        final InetAddress goodNode = InetAddress.getByName("5.6.7.8");
        InetAddress badNode = InetAddress.getByName("1.2.3.4");


        when(carrierLoader.loadConfig(any(InetAddress.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        InetAddress target = (InetAddress) in.getArguments()[0];

                        if (target.equals(goodNode)) {
                            final BucketConfig bucketConfig = mock(BucketConfig.class);
                            when(bucketConfig.name()).thenReturn("bucket-carrier-"+target.getHostAddress());
                            return Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig));
                        } else {
                            return Observable.timer(1, TimeUnit.MINUTES).map(new Func1<Long, Tuple2<LoaderType, BucketConfig>>() {
                                @Override
                                public Tuple2<LoaderType, BucketConfig> call(Long aLong) {
                                    throw new RuntimeException("Could not load config for some reason.");
                                }
                            });
                        }
                    }
                });

        when(httpLoader.loadConfig(any(InetAddress.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        InetAddress target = (InetAddress) in.getArguments()[0];

                        if (target.equals(goodNode)) {
                            final BucketConfig bucketConfig = mock(BucketConfig.class);
                            when(bucketConfig.name()).thenReturn("bucket-http-"+target.getHostAddress());
                            return Observable.just(Tuple.create(LoaderType.HTTP, bucketConfig));
                        } else {
                            return Observable.timer(1, TimeUnit.MINUTES).map(new Func1<Long, Tuple2<LoaderType, BucketConfig>>() {
                                @Override
                                public Tuple2<LoaderType, BucketConfig> call(Long aLong) {
                                    throw new RuntimeException("Could not load config for some reason.");
                                }
                            });
                        }
                    }
                });

        ConfigurationProvider provider = new DefaultConfigurationProvider(
                cluster,
                environment,
                Arrays.asList(carrierLoader, httpLoader),
                new HashMap<LoaderType, Refresher>() {{
                    put(LoaderType.Carrier, refresher);
                    put(LoaderType.HTTP, refresher);
                }}
        );

        provider.seedHosts(Sets.newSet(badNode, goodNode), true);

        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlocking().first();

        assertEquals(1, config.bucketConfigs().size());
        assertTrue(config.hasBucket("bucket-carrier-" + goodNode.getHostAddress()));
    }

    @Test
    public void shouldEmitNewClusterConfig() throws Exception {
        final ClusterFacade cluster = mock(ClusterFacade.class);
        Loader loader = mock(Loader.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        when(loader.loadConfig(any(InetAddress.class), anyString(), anyString()))
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

        provider.seedHosts(Sets.newSet(InetAddress.getByName("localhost")), true);
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
        when(errorLoader.loadConfig(any(InetAddress.class), anyString(), anyString())).thenReturn(errorSubject);
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

        provider.seedHosts(Sets.newSet(InetAddress.getByName("localhost")), true);
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
    @Ignore
    public void shouldCloseBucket() {

    }

    @Test
    @Ignore
    public void shouldCloseBuckets() {

    }

    @Test
    @Ignore
    public void shouldAcceptProposedConfig() {

    }
}