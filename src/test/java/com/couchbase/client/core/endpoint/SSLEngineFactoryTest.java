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
import org.junit.Test;

import javax.net.ssl.SSLEngine;

import static org.junit.Assert.assertTrue;
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
    public void shouldLoadSSLEngine() {
        CoreEnvironment environment = mock(CoreEnvironment.class);
        when(environment.sslKeystoreFile()).thenReturn(this.getClass().getResource("keystore.jks").getPath());
        when(environment.sslKeystorePassword()).thenReturn("keystore");

        SSLEngineFactory factory = new SSLEngineFactory(environment);
        SSLEngine engine = factory.get();
        assertTrue(engine.getUseClientMode());
    }
}