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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.loader.Loader;
import com.couchbase.client.core.config.refresher.Refresher;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.util.Resources;
import com.couchbase.client.core.utils.NetworkAddress;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observers.TestSubscriber;
import rx.subjects.AsyncSubject;

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
        when(loader.loadConfig(any(NetworkAddress.class), anyString(), anyString(), anyString()))
            .thenReturn(Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig)));

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<String>empty());
        when(refresher.registerBucket(anyString(), anyString(), anyString())).thenReturn(Observable.just(true));

        ConfigurationProvider provider = new DefaultConfigurationProvider(
            cluster,
            environment,
            Arrays.asList(loader),
            new HashMap<LoaderType, Refresher>() {{
                put(LoaderType.Carrier, refresher);
            }}
        );

        provider.seedHosts(Sets.newSet(NetworkAddress.localhost()), true);
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
        when(successLoader.loadConfig(any(NetworkAddress.class), anyString(), anyString(), anyString()))
            .thenReturn(Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig)));
        AsyncSubject<BucketConfig> errorSubject = AsyncSubject.create();
        when(errorLoader.loadConfig(any(NetworkAddress.class), anyString(), anyString(), anyString())).thenReturn((Observable) errorSubject);
        errorSubject.onError(new IllegalStateException());

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<String>empty());
        when(refresher.registerBucket(anyString(), anyString(), anyString())).thenReturn(Observable.just(true));

        ConfigurationProvider provider = new DefaultConfigurationProvider(
            cluster,
            environment,
            Arrays.asList(errorLoader, successLoader),
            new HashMap<LoaderType, Refresher>() {{
                put(LoaderType.Carrier, refresher);
                put(LoaderType.HTTP, refresher);
            }}
        );

        provider.seedHosts(Sets.newSet(NetworkAddress.localhost()), true);
        Observable<ClusterConfig> configObservable = provider.openBucket("bucket", "password");
        ClusterConfig config = configObservable.toBlocking().first();
        assertTrue(config.hasBucket("bucket"));
        assertFalse(config.hasBucket("other"));
    }

    @Test
    public void shouldOpenBucketIfSubsetOfNodesIsFailing() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<String>empty());
        when(refresher.registerBucket(anyString(), anyString(), anyString())).thenReturn(Observable.just(true));

        Loader carrierLoader = mock(Loader.class);
        Loader httpLoader = mock(Loader.class);

        final NetworkAddress goodNode = NetworkAddress.create("5.6.7.8");
        NetworkAddress badNode = NetworkAddress.create("1.2.3.4");


        when(carrierLoader.loadConfig(any(NetworkAddress.class), any(String.class), any(String.class), any(String.class)))
            .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                @Override
                public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                    NetworkAddress target = (NetworkAddress) in.getArguments()[0];

                    if (target.equals(goodNode)) {
                        final BucketConfig bucketConfig = mock(BucketConfig.class);
                        when(bucketConfig.name()).thenReturn("bucket-carrier-"+target.address());
                        return Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig));
                    } else {
                        return Observable.error(new Exception("Could not load config for some reason."));
                    }
                }
            });

        when(httpLoader.loadConfig(any(NetworkAddress.class), any(String.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        NetworkAddress target = (NetworkAddress) in.getArguments()[0];

                        if (target.equals(goodNode)) {
                            final BucketConfig bucketConfig = mock(BucketConfig.class);
                            when(bucketConfig.name()).thenReturn("bucket-http-"+target.address());
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
        assertTrue(config.hasBucket("bucket-carrier-" + goodNode.address()));
    }

    @Test
    public void shouldOpenBucketIfSubsetOfNodesIsFailingAndOnlyHttpAvailable() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<String>empty());
        when(refresher.registerBucket(anyString(), anyString(), anyString())).thenReturn(Observable.just(true));

        Loader carrierLoader = mock(Loader.class);
        Loader httpLoader = mock(Loader.class);

        final NetworkAddress goodNode = NetworkAddress.create("5.6.7.8");
        NetworkAddress badNode = NetworkAddress.create("1.2.3.4");

        when(carrierLoader.loadConfig(any(NetworkAddress.class), any(String.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        return Observable.error(new Exception("Could not load config for some reason."));
                    }
                });

        when(httpLoader.loadConfig(any(NetworkAddress.class), any(String.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        NetworkAddress target = (NetworkAddress) in.getArguments()[0];

                        if (target.equals(goodNode)) {
                            final BucketConfig bucketConfig = mock(BucketConfig.class);
                            when(bucketConfig.name()).thenReturn("bucket-http-"+target.address());
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
        assertTrue(config.hasBucket("bucket-http-" + goodNode.address()));
    }

    @Test
    public void shouldOpenBucketIfSubsetOfNodesIsNotResponding() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<String>empty());
        when(refresher.registerBucket(anyString(), anyString(), anyString())).thenReturn(Observable.just(true));

        Loader carrierLoader = mock(Loader.class);
        Loader httpLoader = mock(Loader.class);

        final NetworkAddress goodNode = NetworkAddress.create("5.6.7.8");
        NetworkAddress badNode = NetworkAddress.create("1.2.3.4");


        when(carrierLoader.loadConfig(any(NetworkAddress.class), any(String.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        NetworkAddress target = (NetworkAddress) in.getArguments()[0];

                        if (target.equals(goodNode)) {
                            final BucketConfig bucketConfig = mock(BucketConfig.class);
                            when(bucketConfig.name()).thenReturn("bucket-carrier-"+target.address());
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

        when(httpLoader.loadConfig(any(NetworkAddress.class), any(String.class), any(String.class), any(String.class)))
                .thenAnswer(new Answer<Observable<Tuple2<LoaderType, BucketConfig>>>() {
                    @Override
                    public Observable<Tuple2<LoaderType, BucketConfig>> answer(InvocationOnMock in) throws Throwable {
                        NetworkAddress target = (NetworkAddress) in.getArguments()[0];

                        if (target.equals(goodNode)) {
                            final BucketConfig bucketConfig = mock(BucketConfig.class);
                            when(bucketConfig.name()).thenReturn("bucket-http-"+target.address());
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
        assertTrue(config.hasBucket("bucket-carrier-" + goodNode.address()));
    }

    @Test
    public void shouldEmitNewClusterConfig() throws Exception {
        final ClusterFacade cluster = mock(ClusterFacade.class);
        Loader loader = mock(Loader.class);
        BucketConfig bucketConfig = mock(BucketConfig.class);
        when(bucketConfig.name()).thenReturn("bucket");
        when(loader.loadConfig(any(NetworkAddress.class), anyString(), anyString(), anyString()))
            .thenReturn(Observable.just(Tuple.create(LoaderType.Carrier, bucketConfig)));


        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<String>empty());
        when(refresher.registerBucket(anyString(), anyString(), anyString())).thenReturn(Observable.just(true));

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

        provider.seedHosts(Sets.newSet(NetworkAddress.localhost()), true);
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
        when(errorLoader.loadConfig(any(NetworkAddress.class), anyString(), anyString(), anyString())).thenReturn(errorSubject);
        errorSubject.onError(new IllegalStateException());

        final Refresher refresher = mock(Refresher.class);
        when(refresher.configs()).thenReturn(Observable.<String>empty());
        when(refresher.registerBucket(anyString(), anyString(), anyString())).thenReturn(Observable.just(true));

        ConfigurationProvider provider = new DefaultConfigurationProvider(
            cluster,
            environment,
            Arrays.asList(errorLoader),
            new HashMap<LoaderType, Refresher>() {{
                put(LoaderType.Carrier, refresher);
            }}
        );

        provider.seedHosts(Sets.newSet(NetworkAddress.localhost()), true);
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
    public void shouldAcceptProposedConfigIfNoneExists() {
        DefaultConfigurationProvider provider = new DefaultConfigurationProvider(
            mock(ClusterFacade.class),
            environment
        );

        assertTrue(provider.config().bucketConfigs().isEmpty());

        String raw = Resources.read("config_with_rev_placeholder.json", getClass());
        raw = raw.replace("$REV", "1");
        provider.proposeBucketConfig(null, raw);

        assertFalse(provider.config().bucketConfigs().isEmpty());
        assertEquals(1, provider.config().bucketConfig("default").rev());
    }

    @Test
    public void shouldAcceptProposedConfigIfNewer() {
        DefaultConfigurationProvider provider = new DefaultConfigurationProvider(
            mock(ClusterFacade.class),
            environment
        );

        String raw = Resources.read("config_with_rev_placeholder.json", getClass());
        String v1 = raw.replace("$REV", "1");
        provider.proposeBucketConfig(null, v1);

        assertFalse(provider.config().bucketConfigs().isEmpty());
        assertEquals(1, provider.config().bucketConfig("default").rev());

        String v2 = raw.replace("$REV", "2");
        provider.proposeBucketConfig(null, v2);

        assertFalse(provider.config().bucketConfigs().isEmpty());
        assertEquals(2, provider.config().bucketConfig("default").rev());
    }

    @Test
    public void shouldIgnoreConfigIfInvalid() {
        DefaultConfigurationProvider provider = new DefaultConfigurationProvider(
            mock(ClusterFacade.class),
            environment
        );

        assertTrue(provider.config().bucketConfigs().isEmpty());


        String raw = Resources.read("config_with_rev_placeholder.json", getClass());
        provider.proposeBucketConfig(null, raw);
        assertTrue(provider.config().bucketConfigs().isEmpty());


        String v1 = raw.replace("$REV", "1");
        provider.proposeBucketConfig(null, v1);
        assertFalse(provider.config().bucketConfigs().isEmpty());

        provider.proposeBucketConfig(null, raw);
        assertFalse(provider.config().bucketConfigs().isEmpty());

        String v2 = raw.replace("$REV", "2");
        provider.proposeBucketConfig(null, v2);
        assertFalse(provider.config().bucketConfigs().isEmpty());

        assertEquals(2, provider.config().bucketConfig("default").rev());
    }

    @Test
    public void shouldIgnoreConfigIfOlder() {
        DefaultConfigurationProvider provider = new DefaultConfigurationProvider(
            mock(ClusterFacade.class),
            environment
        );

        String raw = Resources.read("config_with_rev_placeholder.json", getClass());
        String v2 = raw.replace("$REV", "2");
        provider.proposeBucketConfig(null, v2);

        assertFalse(provider.config().bucketConfigs().isEmpty());
        assertEquals(2, provider.config().bucketConfig("default").rev());

        String v1 = raw.replace("$REV", "1");
        provider.proposeBucketConfig(null, v1);

        assertFalse(provider.config().bucketConfigs().isEmpty());
        assertEquals(2, provider.config().bucketConfig("default").rev());
    }

    @Test
    public void shouldIgnoreConfigIfSameRev() throws Exception {
        DefaultConfigurationProvider provider = new DefaultConfigurationProvider(
            mock(ClusterFacade.class),
            environment
        );

        TestSubscriber<ClusterConfig> subscriber = new TestSubscriber<ClusterConfig>();
        provider.configs().subscribe(subscriber);

        String raw = Resources.read("config_with_rev_placeholder.json", getClass());
        String v1 = raw.replace("$REV", "1");
        provider.proposeBucketConfig(null, v1);

        assertFalse(provider.config().bucketConfigs().isEmpty());
        assertEquals(1, provider.config().bucketConfig("default").rev());

        String v2 = raw.replace("$REV", "1");
        provider.proposeBucketConfig(null, v2);

        assertFalse(provider.config().bucketConfigs().isEmpty());
        assertEquals(1, provider.config().bucketConfig("default").rev());

        String v3 = raw.replace("$REV", "2");
        provider.proposeBucketConfig(null, v3);

        assertFalse(provider.config().bucketConfigs().isEmpty());
        assertEquals(2, provider.config().bucketConfig("default").rev());

        Thread.sleep(100);

        assertEquals(2, subscriber.getOnNextEvents().size());
    }

}