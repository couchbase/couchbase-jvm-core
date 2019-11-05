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

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslEngine;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

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
     * Create a new engine factory.
     *
     * @param env the config environment.
     */
    public SSLEngineFactory(SecureEnvironment env) {
        this(env, SSL_CONTEXT_PROTOCOL.trim());
    }

    /**
     * Create a new engine factory.
     *
     * @param env the config environment.
     * @param sslContextProtocol the ssl context protocol used.
     */
    SSLEngineFactory(SecureEnvironment env, String sslContextProtocol) {
        this.env = env;
        this.sslContextProtocol = sslContextProtocol;
    }

    /**
     * Unlike the java SDK where we can set the protocol to be TLS to support all versions of TLS (TLSv1, TLSv1.1, TLv1.2)
     * we cannot use this for the OpenSSL Engine.
     * To keep the property com.couchbase.sslProtocol consistent with the JAVA SDK world, if the user specifies TLS we
     * explicitly return all three versions to be set on the OpenSSL Stack.
     * If a explicit version is specified, then we use the one provided by the user.
     * @return a array of TLS enabled protocols to be used for OpenSSL based on the sslContextProtocol variable.
     */
    private String[] getTLSProtocolsBasedOnSSLContextForOpenSSL() {
        if (!sslContextProtocol.startsWith("TLS")) {
            throw new IllegalArgumentException(
                "SSLContext Protocol does not start with TLS, this is to prevent "
                    + "insecure protocols (Like SSL*) to be used. Potential candidates "
                    + "are TLS (default), TLSv1, TLSv1.1, TLSv1.2, TLSv1.3 depending on "
                    + "the Java version used.");
        }

        if(sslContextProtocol.equals("TLS")) {
            return new String[] {"TLSv1", "TLSv1.1", "TLSv1.2"};
        }
        return new String[] { sslContextProtocol };
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

            /**
             * Restrict protocol to only TLS and not use older SSL insecure protocols.
             * Unfortunately, given the netty version we are using it's impossible to disable SSL if we are using
             * the OpenSSL Stack.
             * https://github.com/netty/netty/issues/7935 documents the bug. Until the netty version is updated,
             * this limitation will remain when using openSSL.
             */
            if (!sslContextProtocol.startsWith("TLS")) {
                throw new IllegalArgumentException(
                    "SSLContext Protocol does not start with TLS, this is to prevent "
                        + "insecure protocols (Like SSL*) to be used. Potential candidates "
                        + "are TLS (default), TLSv1, TLSv1.1, TLSv1.2, TLSv1.3 depending on "
                        + "the Java version used.");
            }

            SSLEngine engine = null;
            //Enable OpenSSL if available and configured.
            if(env.openSslEnabled() && OpenSsl.isAvailable()) {
                try {
                    SslContext ctx =
                        SslContextBuilder.forClient().keyManager(kmf).trustManager(tmf)
                            .protocols(getTLSProtocolsBasedOnSSLContextForOpenSSL()).build();
                    engine = ctx.newEngine(ByteBufAllocator.DEFAULT);
                }
                catch (Exception e) {
                    //Opening a OpenSSL based provider encountered an exception. We revert to the JDK implementation
                    LOGGER.warn("Failed to enable Open SSL in the client stack. Encountered Exception {}."
                        + "Reverting to JDK implementation", e);
                    engine = null;
                }
            }

            if(null == engine) {
                SSLContext ctx;
                ctx = SSLContext.getInstance(sslContextProtocol);
                ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
                engine = ctx.createSSLEngine();
            }

            if(engine instanceof OpenSslEngine){
                LOGGER.info("CouchBase Client is now Using OpenSSL");
            }
            else{
                LOGGER.info("CouchBase Client is using the Default JDK implementation");
            }

            engine.setUseClientMode(true);
            return engine;
        } catch (Exception ex) {
            throw new SSLException("Could not create SSLEngine.", ex);
        }
    }
}
