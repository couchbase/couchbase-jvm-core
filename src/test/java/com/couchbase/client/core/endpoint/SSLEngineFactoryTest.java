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
package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.env.CoreEnvironment;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslEngine;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import javax.net.ssl.SSLEngine;

import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link SSLEngineFactory}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class SSLEngineFactoryTest {

    @Test(expected = SSLException.class)
    public void shouldFailOnEmptyKeystoreFile() {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        SSLEngineFactory factory = new SSLEngineFactory(environment);
        factory.get();
    }

    @Test(expected = SSLException.class)
    public void shouldFailOnKeystoreFileNotFound() {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.sslKeystoreFile()).thenReturn("somefile");

        SSLEngineFactory factory = new SSLEngineFactory(environment);
        factory.get();
    }

    @Test
    public void shouldLoadSSLEngine() throws Exception {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.sslKeystoreFile()).thenReturn(getKeystorePath());
        when(environment.sslKeystorePassword()).thenReturn("keystore");

        SSLEngineFactory factory = new SSLEngineFactory(environment);
        SSLEngine engine = factory.get();
        assertTrue(engine.getUseClientMode());
    }

    @Test(expected = SSLException.class)
    public void shouldFailWithSSLOverride() throws Exception {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.sslKeystoreFile()).thenReturn(getKeystorePath());
        when(environment.sslKeystorePassword()).thenReturn("keystore");

        SSLEngineFactory factory = new SSLEngineFactory(environment, "SSLv3");
        factory.get();
    }

    private String getKeystorePath() throws URISyntaxException {
        return getClass().getResource("keystore.jks").toURI().getPath();
    }

    @Test
    public void testOpenSSLTLS() {
        if(OpenSsl.isAvailable()) {
            CoreEnvironment environment = mock(CoreEnvironment.class);
            when(environment.openSslEnabled()).thenReturn(true);
            when(environment.sslKeystoreFile()).thenReturn(this.getClass().getResource("keystore.jks").getPath());
            when(environment.sslKeystorePassword()).thenReturn("keystore");
            SSLEngineFactory factory = new SSLEngineFactory(environment, "TLS");
            SSLEngine engine = factory.get();

            assertTrue(engine instanceof OpenSslEngine);
            Set<String> enabledProtocols = new HashSet<String>(Arrays.asList(engine.getEnabledProtocols()));
            assertEquals(enabledProtocols.size(), 5L);

            //https://github.com/netty/netty/issues/7935
            //There is no way to disable SSLv2, SSLv2Hello on the netty 4.0.56
            assertTrue(enabledProtocols.contains("SSLv2Hello"));
            assertTrue(enabledProtocols.contains("SSLv2"));
            assertTrue(enabledProtocols.contains("TLSv1"));
            assertTrue(enabledProtocols.contains("TLSv1.1"));
            assertTrue(enabledProtocols.contains("TLSv1.2"));
            assertTrue(engine.getUseClientMode());
        }
    }

    @Test
    public void testOpenSSLTLSv_1_2() {
        if(OpenSsl.isAvailable()) {
            CoreEnvironment environment = mock(CoreEnvironment.class);
            when(environment.openSslEnabled()).thenReturn(true);
            when(environment.sslKeystoreFile()).thenReturn(this.getClass().getResource("keystore.jks").getPath());
            when(environment.sslKeystorePassword()).thenReturn("keystore");
            SSLEngineFactory factory = new SSLEngineFactory(environment, "TLSv1.2");
            SSLEngine engine = factory.get();
            assertTrue(engine instanceof OpenSslEngine);

            //https://github.com/netty/netty/issues/7935
            //There is no way to disable SSLv2, SSLv2Hello on the netty 4.0.56

            Set<String> enabledProtocols = new HashSet<String>(Arrays.asList(engine.getEnabledProtocols()));
            assertEquals(enabledProtocols.size(), 3L);
            assertTrue(enabledProtocols.contains("SSLv2Hello"));
            assertTrue(enabledProtocols.contains("TLSv1.2"));
            assertTrue(enabledProtocols.contains("SSLv2"));
            assertTrue(engine.getUseClientMode());
        }
    }

    @Test(expected = SSLException.class)
    public void shouldFailWithSSLOverrideWithOpenSSL() {
        if(OpenSsl.isAvailable()) {
            CoreEnvironment environment = mock(CoreEnvironment.class);
            when(environment.openSslEnabled()).thenReturn(true);
            when(environment.sslKeystoreFile()).thenReturn(this.getClass().getResource("keystore.jks").getPath());
            when(environment.sslKeystorePassword()).thenReturn("keystore");

            SSLEngineFactory factory = new SSLEngineFactory(environment, "SSLv3");
            factory.get();
        }
    }
}
