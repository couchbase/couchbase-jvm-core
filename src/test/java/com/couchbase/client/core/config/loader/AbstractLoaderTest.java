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
package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.AddServiceResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.Resources;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link AbstractLoader}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class AbstractLoaderTest {

    private final String localhostConfig = Resources.read("localhost.json", this.getClass());

    @Test
    public void shouldLoadConfigForOneSeedNode() throws Exception {
        Environment environment = new CouchbaseEnvironment();
        Cluster cluster = mock(Cluster.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddNodeResponse(ResponseStatus.SUCCESS, "localhost"))
        );
        when(cluster.send(isA(AddServiceRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddServiceResponse(ResponseStatus.SUCCESS, "localhost"))
        );
        Set<InetAddress> seedNodes = new HashSet<InetAddress>();
        seedNodes.add(InetAddress.getByName("localhost"));

        InstrumentedLoader loader = new InstrumentedLoader(99, localhostConfig, cluster, environment);
        Observable<BucketConfig> configObservable = loader.loadConfig(seedNodes, "default", "password");

        BucketConfig loadedConfig = configObservable.toBlockingObservable().single();
        assertEquals("default", loadedConfig.name());
        assertEquals(1, loadedConfig.nodes().size());
    }

    @Test
    public void shouldLoadConfigsFromMoreSeedNodes() throws Exception {
        Environment environment = new CouchbaseEnvironment();
        Cluster cluster = mock(Cluster.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddNodeResponse(ResponseStatus.SUCCESS, "localhost"))
        );
        when(cluster.send(isA(AddServiceRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddServiceResponse(ResponseStatus.SUCCESS, "localhost"))
        );
        Set<InetAddress> seedNodes = new HashSet<InetAddress>();
        seedNodes.add(InetAddress.getByName("10.1.1.1"));
        seedNodes.add(InetAddress.getByName("10.1.1.2"));
        seedNodes.add(InetAddress.getByName("10.1.1.3"));

        InstrumentedLoader loader = new InstrumentedLoader(99, localhostConfig, cluster, environment);
        Observable<BucketConfig> configObservable = loader.loadConfig(seedNodes, "default", "password");

        List<BucketConfig> loadedConfigs = configObservable.toList().toBlockingObservable().single();
        assertEquals(3, loadedConfigs.size());
    }

    @Test
    public void shouldFailIfNoConfigCouldBeLoaded() throws Exception {
        Environment environment = new CouchbaseEnvironment();
        Cluster cluster = mock(Cluster.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddNodeResponse(ResponseStatus.SUCCESS, "localhost"))
        );
        when(cluster.send(isA(AddServiceRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddServiceResponse(ResponseStatus.SUCCESS, "localhost"))
        );
        Set<InetAddress> seedNodes = new HashSet<InetAddress>();
        seedNodes.add(InetAddress.getByName("localhost"));

        InstrumentedLoader loader = new InstrumentedLoader(0, localhostConfig, cluster, environment);
        Observable<BucketConfig> configObservable = loader.loadConfig(seedNodes, "default", "password");

        try {
            configObservable.toBlockingObservable().single();
            assertTrue(false);
        } catch(IllegalStateException ex) {
            assertEquals("Bucket config response did not return with success.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }

    @Test
    public void shouldIgnoreFailingConfigOnManySeedNodes() throws Exception {
        Environment environment = new CouchbaseEnvironment();
        Cluster cluster = mock(Cluster.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddNodeResponse(ResponseStatus.SUCCESS, "localhost"))
        );
        when(cluster.send(isA(AddServiceRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddServiceResponse(ResponseStatus.SUCCESS, "localhost"))
        );
        Set<InetAddress> seedNodes = new HashSet<InetAddress>();
        seedNodes.add(InetAddress.getByName("10.1.1.1"));
        seedNodes.add(InetAddress.getByName("10.1.1.2"));
        seedNodes.add(InetAddress.getByName("10.1.1.3"));

        InstrumentedLoader loader = new InstrumentedLoader(2, localhostConfig, cluster, environment);
        Observable<BucketConfig> configObservable = loader.loadConfig(seedNodes, "default", "password");

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger success = new AtomicInteger();
        final AtomicInteger failure = new AtomicInteger();
        configObservable.subscribe(new Subscriber<BucketConfig>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
                failure.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onNext(BucketConfig bucketConfigs) {
                success.incrementAndGet();
            }
        });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(2, success.get());
        assertEquals(1, failure.get());
    }

    @Test
    public void shouldFailIfNodeCouldNotBeAdded() throws Exception {
        Environment environment = new CouchbaseEnvironment();
        Cluster cluster = mock(Cluster.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddNodeResponse(ResponseStatus.FAILURE, "localhost"))
        );
        Set<InetAddress> seedNodes = new HashSet<InetAddress>();
        seedNodes.add(InetAddress.getByName("localhost"));

        InstrumentedLoader loader = new InstrumentedLoader(0, localhostConfig, cluster, environment);
        Observable<BucketConfig> configObservable = loader.loadConfig(seedNodes, "default", "password");

        try {
            configObservable.toBlockingObservable().single();
            assertTrue(false);
        } catch(IllegalStateException ex) {
            assertEquals("Could not add node for config loading.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }

    @Test
    public void shouldFailIfServiceCouldNotBeAdded() throws Exception {
        Environment environment = new CouchbaseEnvironment();
        Cluster cluster = mock(Cluster.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddNodeResponse(ResponseStatus.SUCCESS, "localhost"))
        );
        when(cluster.send(isA(AddServiceRequest.class))).thenReturn(
                Observable.from((CouchbaseResponse) new AddServiceResponse(ResponseStatus.FAILURE, "localhost"))
        );
        Set<InetAddress> seedNodes = new HashSet<InetAddress>();
        seedNodes.add(InetAddress.getByName("localhost"));

        InstrumentedLoader loader = new InstrumentedLoader(0, localhostConfig, cluster, environment);
        Observable<BucketConfig> configObservable = loader.loadConfig(seedNodes, "default", "password");

        try {
            configObservable.toBlockingObservable().single();
            assertTrue(false);
        } catch(IllegalStateException ex) {
            assertEquals("Could not add service for config loading.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }

    /**
     * A loader implementation which returns with a successful raw config.
     */
    static class InstrumentedLoader extends AbstractLoader {
        private final String config;
        private final int failAfter;
        private volatile int failCounter = 0;

        InstrumentedLoader(int failAfter, String config, Cluster cluster, Environment environment) {
            super(ServiceType.BINARY, cluster, environment);
            this.config = config;
            this.failAfter = failAfter;
        }

        @Override
        protected int port() {
            return 1234;
        }

        @Override
        protected Observable<String> discoverConfig(String bucket, String password, String hostname) {
            IllegalStateException ex = new IllegalStateException("Bucket config response did not return with success.");
            if (failCounter++ >= failAfter) {
                return Observable.error(ex);
            }
            return Observable.from(config);
        }
    }

}