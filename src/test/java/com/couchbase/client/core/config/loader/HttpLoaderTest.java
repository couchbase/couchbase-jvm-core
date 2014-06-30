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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.env.CouchbaseEnvironment;
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.config.BucketConfigRequest;
import com.couchbase.client.core.message.config.BucketConfigResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the correct functionality of the {@link HttpLoader}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class HttpLoaderTest {

    private static InetAddress host;

    @BeforeClass
    public static void setup() throws Exception {
        host = InetAddress.getLocalHost();
    }

    @Test
    public void shouldUseDirectPortIfNotSSL() {
        Environment environment = new CouchbaseEnvironment();
        ClusterFacade cluster = mock(ClusterFacade.class);

        HttpLoader loader = new HttpLoader(cluster, environment);
        assertEquals(environment.bootstrapHttpDirectPort(), loader.port());
    }

    @Test
    public void shouldUseEncryptedPortIfSSL() {
        Environment environment = mock(Environment.class);
        when(environment.sslEnabled()).thenReturn(true);
        when(environment.bootstrapHttpSslPort()).thenReturn(12345);
        ClusterFacade cluster = mock(ClusterFacade.class);

        HttpLoader loader = new HttpLoader(cluster, environment);
        assertEquals(environment.bootstrapHttpSslPort(), loader.port());
    }

    @Test
    public void shouldDiscoverConfigFromTerse() {
        Environment environment = new CouchbaseEnvironment();
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> response = Observable.from(
            (CouchbaseResponse) new BucketConfigResponse("myconfig", ResponseStatus.SUCCESS)
        );
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(response);

        HttpLoader loader = new HttpLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "password", host);
        assertEquals("myconfig", configObservable.toBlocking().single());
    }

    @Test
    public void shouldDiscoverConfigFromVerboseAsFallback() {
        Environment environment = new CouchbaseEnvironment();
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> terseResponse = Observable.from(
                (CouchbaseResponse) new BucketConfigResponse(null, ResponseStatus.FAILURE)
        );
        Observable<CouchbaseResponse> verboseResponse = Observable.from(
                (CouchbaseResponse) new BucketConfigResponse("verboseConfig", ResponseStatus.SUCCESS)
        );
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(terseResponse);
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(verboseResponse);

        HttpLoader loader = new HttpLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "password", host);
        assertEquals("verboseConfig", configObservable.toBlocking().single());
    }

    @Test
    public void shouldThrowExceptionIfTerseAndVerboseCouldNotBeDiscovered() {
        Environment environment = new CouchbaseEnvironment();
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> terseResponse = Observable.from(
                (CouchbaseResponse) new BucketConfigResponse(null, ResponseStatus.FAILURE)
        );
        Observable<CouchbaseResponse> verboseResponse = Observable.from(
                (CouchbaseResponse) new BucketConfigResponse(null, ResponseStatus.FAILURE)
        );
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(terseResponse);
        when(cluster.send(isA(BucketConfigRequest.class))).thenReturn(verboseResponse);

        HttpLoader loader = new HttpLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "password", host);
        try {
            configObservable.toBlocking().single();
            assertTrue(false);
        } catch(IllegalStateException ex) {
            assertEquals("Could not load bucket configuration: FAILURE(null)", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }

    @Test
    public void shouldThrowIfDisabledThroughConfiguration() {
        Environment environment = mock(Environment.class);
        when(environment.bootstrapHttpEnabled()).thenReturn(false);
        ClusterFacade cluster = mock(ClusterFacade.class);

        HttpLoader loader = new HttpLoader(cluster, environment);
        try {
            loader.discoverConfig("bucket", "password", host).toBlocking().single();
            assertTrue(false);
        } catch(ConfigurationException ex) {
            assertEquals("Http Bootstrap disabled through configuration.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }

}