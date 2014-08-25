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
package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.env.CoreEnvironment;
import org.junit.Test;

import javax.net.ssl.SSLEngine;

import java.util.Arrays;

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