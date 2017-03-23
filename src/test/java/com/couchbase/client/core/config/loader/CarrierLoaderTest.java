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
import com.couchbase.client.core.config.ConfigurationException;
import com.couchbase.client.core.endpoint.ResponseStatusConverter;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
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
                        KeyValueStatus.SUCCESS.code(), "bucket", content, host)
        );
        when(cluster.send(isA(GetBucketConfigRequest.class))).thenReturn(response);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "bucket", "password", host);
        assertEquals("myconfig", configObservable.toBlocking().single());
    }

    @Test
    public void shouldThrowExceptionIfNotDiscovered() {
        ClusterFacade cluster = mock(ClusterFacade.class);
        Observable<CouchbaseResponse> response = Observable.just(
                (CouchbaseResponse) new GetBucketConfigResponse(ResponseStatus.FAILURE,
                        KeyValueStatus.ERR_NOT_FOUND.code(), "bucket", Unpooled.EMPTY_BUFFER, host)
        );
        when(cluster.send(isA(GetBucketConfigRequest.class))).thenReturn(response);

        CarrierLoader loader = new CarrierLoader(cluster, environment);
        Observable<String> configObservable = loader.discoverConfig("bucket", "bucket", "password", host);
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
            loader.discoverConfig("bucket", "password", "password", host).toBlocking().single();
            assertTrue(false);
        } catch(ConfigurationException ex) {
            assertEquals("Carrier Bootstrap disabled through configuration.", ex.getMessage());
        } catch(Exception ex) {
            assertTrue(false);
        }
    }
}