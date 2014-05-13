package com.couchbase.client.core.endpoint;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * Creates a {@link SSLEngine} which will be passed into the handler if SSL is enabled.
 */
public class SSLEngineFactory {

    public static SSLEngine get() {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[] { TRUST_MANAGER }, new SecureRandom());
            SSLEngine engine = context.createSSLEngine();
            engine.setUseClientMode(true);
            return engine;
        } catch(Exception ex) {
            throw new SSLException("Could not create SSLEngine because of:", ex);
        }
    }

    private static final TrustManager TRUST_MANAGER = new X509TrustManager() {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // Always trust - it is an example.
            // You should do something in the real world.
            // You will reach here only if you enabled client certificate auth,
            // as described in SecureChatSslContextFactory.
            System.err.println(
                "UNKNOWN CLIENT CERTIFICATE: " + chain[0].getSubjectDN());
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // Always trust - it is an example.
            // You should do something in the real world.
            System.err.println(
                "UNKNOWN SERVER CERTIFICATE: " + chain[0].getSubjectDN());
        }
    };
}
