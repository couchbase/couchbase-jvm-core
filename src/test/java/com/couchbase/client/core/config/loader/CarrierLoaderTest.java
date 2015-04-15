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
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.GetBucketConfigRequest;
import com.couchbase.client.core.message.kv.GetBucketConfigResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
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
 * Verifies the correct functionality of the {@link CarrierLoader}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CarrierLoaderTest {

    private static final CoreEnvironment environment = DefaultCoreEnvironment.create();
    private static InetAddress host;

    @BeforeClass
    public static void setup() throws Exception {
        host = InetAddress.getLocalHost();
    }

    @Test
    public void shouldUseDirectPortIfNotSSL() {
        ClusterFacade cluster = mock(ClusterFacade.class);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        assertEquals(environment.bootstrapCarrierDirectPort(), loader.port());
    }

    @Test
    public void shouldUseEncryptedPortIfSSL() {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.sslEnabled()).thenReturn(true);
        when(environment.bootstrapCarrierSslPort()).thenReturn(12345);
        ClusterFacade cluster = mock(ClusterFacade.class);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        assertEquals(environment.bootstrapCarrierSslPort(), loader.port());
    }

    @Test
    public void shouldDiscoverConfig() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        ByteBuf content = Unpooled.copiedBuffer("myconfig", CharsetUtil.UTF_8);
        Observable<CouchbaseResponse> response = Observable.just(
                (CouchbaseResponse) new GetBucketConfigResponse(ResponseStatus.SUCCESS,
                        ResponseStatusConverter.BINARY_SUCCESS, "bucket", content, host)
        );
        when(cluster.send(isA(GetBucketConfigRequest.class))).thenReturn(response);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "password", host);
        assertEquals("myconfig", configObservable.toBlocking().single());
    }

    @Test
    public void shouldThrowExceptionIfNotDiscovered() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> response = Observable.just(
                (CouchbaseResponse) new GetBucketConfigResponse(ResponseStatus.FAILURE,
                        ResponseStatusConverter.BINARY_ERR_NOT_FOUND, "bucket", Unpooled.EMPTY_BUFFER, host)
        );
        when(cluster.send(isA(GetBucketConfigRequest.class))).thenReturn(response);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "password", host);
        try {
            configObservable.toBlocking().single();
            assertTrue(false);
        } catch(IllegalStateException ex) {
            assertEquals("Bucket config response did not return with success.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue("Unexpected exception: " + ex, false);
        }
    }

    @Test
    public void shouldThrowIfDisabledThroughConfiguration() {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.bootstrapHttpEnabled()).thenReturn(false);
        ClusterFacade cluster = mock(ClusterFacade.class);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        try {
            loader.discoverConfig("bucket", "password", host).toBlocking().single();
            assertTrue(false);
        } catch(ConfigurationException ex) {
            assertEquals("Carrier Bootstrap disabled through configuration.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }
}