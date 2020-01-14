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

import com.couchbase.client.core.env.SecureEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
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
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(SSLEngineFactory.class);

    /**
     * Allows to modify the chosen ssl context protocol.
     *
     * Needs to follow the terminology in
     * https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext
     */
    private static final String SSL_CONTEXT_PROTOCOL = System.getProperty(
        "com.couchbase.sslProtocol",
        "TLS"
    );

    /**
     * The global environment which is shared.
     */
    private final SecureEnvironment env;

    /**
     * The ssl context protocol that should be used.
     */
    private final String sslContextProtocol;

    /**
     * The remote hostname to use (for hostname verification).
     */
    private final String hostname;

    /**
     * The remote port to use (for hostname verification).
     */
    private final int port;

    /**
     * Create a new engine factory.
     *
     * @param env the config environment.
     */
    public SSLEngineFactory(SecureEnvironment env, String hostname, int port) {
        this(env, SSL_CONTEXT_PROTOCOL.trim(), hostname, port);
    }

    /**
     * Create a new engine factory.
     *
     * @param env the config environment.
     * @param sslContextProtocol the ssl context protocol used.
     */
    SSLEngineFactory(SecureEnvironment env, String sslContextProtocol, String hostname, int port) {
        this.env = env;
        this.sslContextProtocol = sslContextProtocol;
        this.hostname = hostname;
        this.port = port;
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

            KeyStore ks = env.sslKeystore();
            if (ks == null) {
                String ksFile = env.sslKeystoreFile();
                if (ksFile != null && !ksFile.isEmpty()) {
                    ks = KeyStore.getInstance(KeyStore.getDefaultType());
                    ks.load(new FileInputStream(ksFile), password);
                }
            }

            KeyStore ts = env.sslTruststore();
            if (ts == null) {
                String tsFile = env.sslTruststoreFile();
                if (tsFile != null && !tsFile.isEmpty()) {
                    // filepath found, open and init
                    String tsPassword = env.sslTruststorePassword();
                    char[] tspass = tsPassword == null || tsPassword.isEmpty() ? null : tsPassword.toCharArray();
                    ts = KeyStore.getInstance(KeyStore.getDefaultType());
                    ts.load(new FileInputStream(tsFile), tspass);
                }
            }

            if (ks == null && ts == null) {
                throw new IllegalStateException("Either a KeyStore or a TrustStore " +
                    "need to be provided (or both).");
            } else if (ks == null) {
                ks = ts;
                LOGGER.debug("No KeyStore provided, using provided TrustStore to initialize both factories.");
            } else if (ts == null) {
                ts = ks;
                LOGGER.debug("No TrustStore provided, using provided KeyStore to initialize both factories.");
            }

            String defaultAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(defaultAlgorithm);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(defaultAlgorithm);
            kmf.init(ks, password);
            tmf.init(ts);

            if (!sslContextProtocol.startsWith("TLS")) {
                throw new IllegalArgumentException(
                    "SSLContext Protocol does not start with TLS, this is to prevent "
                        + "insecure protocols (Like SSL*) to be used. Potential candidates "
                        + "are TLS (default), TLSv1, TLSv1.1, TLSv1.2, TLSv1.3 depending on "
                        + "the Java version used.");
            }
            SSLContext ctx = SSLContext.getInstance(sslContextProtocol);
            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

            SSLEngine engine = ctx.createSSLEngine(hostname, port);
            engine.setUseClientMode(true);

            if (env.sslHostnameVerificationEnabled()) {
                SSLParameters sslParameters = engine.getSSLParameters();
                sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
                engine.setSSLParameters(sslParameters);
            }
            return engine;
        } catch (Exception ex) {
            throw new SSLException("Could not create SSLEngine.", ex);
        }
    }
}
