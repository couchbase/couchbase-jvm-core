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
package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.LoaderType;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.AddServiceResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.Resources;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link AbstractLoader}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class AbstractLoaderTest {

    private static final CoreEnvironment environment = DefaultCoreEnvironment.create();
    private final String localhostConfig = Resources.read("localhost.json", this.getClass());

    private static String host;

    @BeforeClass
    public static void setup() throws Exception {
        host = "127.0.0.1";
    }

    @AfterClass
    public static void cleanup() {
        environment.shutdown();
    }

    @Test
    public void shouldLoadConfigForOneSeedNode() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) new AddNodeResponse(ResponseStatus.SUCCESS, host))
        );
        when(cluster.send(isA(AddServiceRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) new AddServiceResponse(ResponseStatus.SUCCESS, host))
        );

        InstrumentedLoader loader = new InstrumentedLoader(99, localhostConfig, cluster, environment);
        Observable<Tuple2<LoaderType, BucketConfig>> configObservable =
            loader.loadConfig(host, "default", "password");

        BucketConfig loadedConfig = configObservable.toBlocking().single().value2();
        assertEquals("default", loadedConfig.name());
        assertEquals(1, loadedConfig.nodes().size());
    }

    @Test
    public void shouldFailIfNoConfigCouldBeLoaded() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) new AddNodeResponse(ResponseStatus.SUCCESS, host))
        );
        when(cluster.send(isA(AddServiceRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) new AddServiceResponse(ResponseStatus.SUCCESS, host))
        );

        InstrumentedLoader loader = new InstrumentedLoader(0, localhostConfig, cluster, environment);
        Observable<Tuple2<LoaderType, BucketConfig>> configObservable =
            loader.loadConfig(host, "default", "password");

        try {
            configObservable.toBlocking().single();
            assertTrue(false);
        } catch(IllegalStateException ex) {
            assertEquals("Bucket config response did not return with success.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }

    @Test
    public void shouldFailIfNodeCouldNotBeAdded() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) new AddNodeResponse(ResponseStatus.FAILURE, host))
        );

        InstrumentedLoader loader = new InstrumentedLoader(0, localhostConfig, cluster, environment);
        Observable<Tuple2<LoaderType, BucketConfig>> configObservable =
            loader.loadConfig(host, "default", "password");

        try {
            configObservable.toBlocking().single();
            assertTrue(false);
        } catch(IllegalStateException ex) {
            assertEquals("Could not add node for config loading.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }

    @Test
    public void shouldFailIfServiceCouldNotBeAdded() throws Exception {
        ClusterFacade cluster = mock(ClusterFacade.class);
        when(cluster.send(isA(AddNodeRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) new AddNodeResponse(ResponseStatus.SUCCESS, host))
        );
        when(cluster.send(isA(AddServiceRequest.class))).thenReturn(
                Observable.just((CouchbaseResponse) new AddServiceResponse(ResponseStatus.FAILURE, host))
        );

        InstrumentedLoader loader = new InstrumentedLoader(0, localhostConfig, cluster, environment);
        Observable<Tuple2<LoaderType, BucketConfig>> configObservable =
            loader.loadConfig(host, "default", "password");

        try {
            configObservable.toBlocking().single();
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

        InstrumentedLoader(int failAfter, String config, ClusterFacade cluster, CoreEnvironment environment) {
            super(LoaderType.Carrier, ServiceType.BINARY, cluster, environment);
            this.config = config;
            this.failAfter = failAfter;
        }

        @Override
        protected int port() {
            return 1234;
        }

        @Override
        protected Observable<String> discoverConfig(String bucket, String password, String username, String hostname) {
            IllegalStateException ex = new IllegalStateException("Bucket config response did not return with success.");
            if (failCounter++ >= failAfter) {
                return Observable.error(ex);
            }
            return Observable.just(config);
        }
    }

}