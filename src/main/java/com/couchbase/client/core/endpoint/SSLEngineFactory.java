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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * Creates a {@link SSLEngine} which will be passed into the handler if SSL is enabled.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class SSLEngineFactory {

    /**
     * The global environment which is shared.
     */
    private final CoreEnvironment env;

    /**
     * Create a new engine factory.
     *
     * @param env the config environment.
     */
    public SSLEngineFactory(CoreEnvironment env) {
        this.env = env;
    }

    /**
     * Returns a new {@link SSLEngine} constructed from the config settings.
     *
     * @return a {@link SSLEngine} ready to be used.
     */
    public SSLEngine get() {
        try {
            String pass = env.sslKeystorePassword();
            char[] password = pass == null || pass.isEmpty() ? null : pass.toCharArray();
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            String ksFile = env.sslKeystoreFile();
            if (ksFile == null || ksFile.isEmpty()) {
                throw new IllegalArgumentException("Path to Keystore File must not be null or empty.");
            }
            ks.load(new FileInputStream(ksFile), password);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            kmf.init(ks, password);
            tmf.init(ks);

            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

            SSLEngine engine = ctx.createSSLEngine();
            engine.setUseClientMode(true);
            return engine;
        } catch(Exception ex) {
            throw new SSLException("Could not create SSLEngine.", ex);
        }
    }
}
