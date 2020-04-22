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
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.DefaultClusterConfig;
import com.couchbase.client.core.config.DefaultCouchbaseBucketConfigTest;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import com.couchbase.client.core.util.Resources;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import rx.Observable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link HttpLoader}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class HttpLoaderTest {

    private static String host;
    private static final CoreEnvironment environment = DefaultCoreEnvironment.create();

    @BeforeClass
    public static void setup() {
        host = "127.0.0.1";
    }

    @AfterClass
    public static void cleanup() {
        environment.shutdown();
    }

    @Test
    public void shouldUseDirectPortIfNotSSL() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        when(cluster.send(any(GetClusterConfigRequest.class))).thenReturn(Observable.just(
            (CouchbaseResponse) new GetClusterConfigResponse(new DefaultClusterConfig(), ResponseStatus.SUCCESS)
        ));
        HttpLoader loader = new HttpLoader(cluster, environment);
        assertEquals(environment.bootstrapHttpDirectPort(), loader.port("localhost"));
    }

    @Test
    public void shouldUseEncryptedPortIfSSL() {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.sslEnabled()).thenReturn(true);
        when(environment.bootstrapHttpSslPort()).thenReturn(12345);
        ClusterFacade cluster = mock(ClusterFacade.class);

        when(cluster.send(any(GetClusterConfigRequest.class))).thenReturn(Observable.just(
            (CouchbaseResponse) new GetClusterConfigResponse(new DefaultClusterConfig(), ResponseStatus.SUCCESS)
        ));
        HttpLoader loader = new HttpLoader(cluster, environment);
        assertEquals(environment.bootstrapHttpSslPort(), loader.port("localhost"));
    }

    @Test
    public void shouldDiscoverConfigFromTerse() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> response = Observable.just(
            (CouchbaseResponse) new BucketConfigResponse("myconfig", ResponseStatus.SUCCESS)
        );
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(response);

        HttpLoader loader = new HttpLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "bucket", "password", host);
        assertEquals("myconfig", configObservable.toBlocking().single());
    }

    @Test
    public void shouldDiscoverConfigFromVerboseAsFallback() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> terseResponse = Observable.just(
                (CouchbaseResponse) new BucketConfigResponse(null, ResponseStatus.FAILURE)
        );
        Observable<CouchbaseResponse> verboseResponse = Observable.just(
                (CouchbaseResponse) new BucketConfigResponse("verboseConfig", ResponseStatus.SUCCESS)
        );
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(terseResponse);
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(verboseResponse);

        HttpLoader loader = new HttpLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "bucket", "password", host);
        assertEquals("verboseConfig", configObservable.toBlocking().single());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotGoToVerboseIfTerseIsGenericFailure() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> terseResponse = Observable.just(
                (CouchbaseResponse) new BucketConfigResponse(null, ResponseStatus.FAILURE)
        );
        Observable<CouchbaseResponse> verboseResponse = Observable.just(
                (CouchbaseResponse) new BucketConfigResponse(null, ResponseStatus.FAILURE)
        );
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(terseResponse, verboseResponse);

        HttpLoader loader = new HttpLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "bucket", "password", host);
        try {
            configObservable.toBlocking().single();
            fail();
        } catch(IllegalStateException ex) {
            assertEquals("Could not load bucket configuration: FAILURE(null)", ex.getMessage());
        } catch(Exception ex) {
            fail();
        }

        verify(cluster, times(1)).send(isA(BucketConfigRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldFallBackToVerboseIfTerseNotFound() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> terseResponse = Observable.just(
            (CouchbaseResponse) new BucketConfigResponse(null, ResponseStatus.NOT_EXISTS)
        );
        Observable<CouchbaseResponse> verboseResponse = Observable.just(
            (CouchbaseResponse) new BucketConfigResponse(null, ResponseStatus.FAILURE)
        );
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(terseResponse, verboseResponse);

        HttpLoader loader = new HttpLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "bucket", "password", host);
        try {
            configObservable.toBlocking().single();
            fail();
        } catch(IllegalStateException ex) {
            assertEquals("Could not load bucket configuration: FAILURE(null)", ex.getMessage());
        } catch(Exception ex) {
            fail();
        }

        verify(cluster, times(2)).send(isA(BucketConfigRequest.class));
    }

    @Test
    public void shouldThrowIfDisabledThroughConfiguration() {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.bootstrapHttpEnabled()).thenReturn(false);
        ClusterFacade cluster = mock(ClusterFacade.class);

        HttpLoader loader = new HttpLoader(cluster, environment);
        try {
            loader.discoverConfig("bucket", "bucket", "password", host).toBlocking().single();
            fail();
        } catch(ConfigurationException ex) {
            assertEquals("HTTP Bootstrap disabled through configuration.", ex.getMessage());
        } catch(Exception ex) {
            fail();
        }
    }

    @Test
    public void verifyRightTersePath() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> terseResponse = Observable.just(
                (CouchbaseResponse) new BucketConfigResponse("terseConfig", ResponseStatus.SUCCESS)
        );
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(terseResponse);
        HttpLoader loader = new HttpLoader(cluster, environment);
        loader.discoverConfig("default", "bucket", "password", host).toBlocking().single();

        ArgumentCaptor<BucketConfigRequest> argument = ArgumentCaptor.forClass(BucketConfigRequest.class);
        verify(cluster).send(argument.capture());
        Assert.assertEquals("/pools/default/b/default", argument.getValue().path());
    }

    @Test
    public void shouldUsePortsFromConfigIfPresentWithAlternateNetwork() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        HttpLoader loader = new HttpLoader(cluster, environment);

        ClusterConfig clusterConfig = new DefaultClusterConfig();

        String raw = Resources.read("config_with_external.json", DefaultCouchbaseBucketConfigTest.class);
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, mock(CoreEnvironment.class), "127.0.0.1");

        config.useAlternateNetwork("external");

        clusterConfig.setBucketConfig("foo", config);

        when(cluster.send(any(GetClusterConfigRequest.class))).thenReturn(Observable.just(
            (CouchbaseResponse) new GetClusterConfigResponse(clusterConfig, ResponseStatus.SUCCESS)
        ));

        assertEquals(32790, loader.port("172.17.0.2"));
    }

    @Test
    public void shouldUsePortsFromConfigIfPresentWithoutAlternateNetwork() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        HttpLoader loader = new HttpLoader(cluster, environment);

        ClusterConfig clusterConfig = new DefaultClusterConfig();

        String raw = Resources.read("config_with_external.json", DefaultCouchbaseBucketConfigTest.class);
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, mock(CoreEnvironment.class), "127.0.0.1");

        clusterConfig.setBucketConfig("foo", config);

        when(cluster.send(any(GetClusterConfigRequest.class))).thenReturn(Observable.just(
            (CouchbaseResponse) new GetClusterConfigResponse(clusterConfig, ResponseStatus.SUCCESS)
        ));

        assertEquals(8091, loader.port("172.17.0.2"));
    }
}