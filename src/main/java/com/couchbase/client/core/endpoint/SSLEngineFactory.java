package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.env.Environment;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * Creates a {@link SSLEngine} which will be passed into the handler if SSL is enabled.
 */
public class SSLEngineFactory {

    private static final String ALGORITHM = "SunX509";

    private final Environment env;

    public SSLEngineFactory(Environment env) {
        this.env = env;
    }

    public SSLEngine get() {
        try {
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            char[] password = env.sslKeystorePassword().isEmpty() ? null : env.sslKeystorePassword().toCharArray();
            ks.load(new FileInputStream(env.sslKeystoreFile()), password);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(ALGORITHM);
            kmf.init(ks, password);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(ALGORITHM);
            tmf.init(ks);

            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            SSLEngine engine = ctx.createSSLEngine();
            engine.setUseClientMode(true);
            return engine;
        } catch(Exception ex) {
            throw new SSLException("Could not create SSLEngine because of:", ex);
        }
    }
}
