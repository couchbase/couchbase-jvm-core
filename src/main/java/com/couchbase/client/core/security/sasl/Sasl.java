/**
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.core.security.sasl;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.util.Map;

/**
 * A wrapper for {@link javax.security.sasl.Sasl} which first tries the mechanisms that are supported
 * out of the box and then tries our extended range (provided by {@link ShaSaslClient}.
 *
 * @author Trond Norbye
 * @version 1.2.5
 */
public class Sasl {

    /**
     * Our custom client factory which supports the additional mechanisms.
     */
    private static ShaSaslClientFactory SASL_FACTORY = new ShaSaslClientFactory();

    /**
     * Creates a new {@link SaslClient} and first tries the JVM built in clients before falling back
     * to our custom implementations. The mechanisms are tried in the order they arrive.
     */
    public static SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol,
        String serverName, Map<String, ?> props, CallbackHandler cbh) throws SaslException {

        boolean enableScram = Boolean.parseBoolean(System.getProperty("com.couchbase.scramEnabled", "true"));

        for (String mech : mechanisms) {
            String[] mechs = new String[] { mech };

            SaslClient client = javax.security.sasl.Sasl.createSaslClient(
                mechs, authorizationId, protocol, serverName, props, cbh
            );

            if (client == null && enableScram) {
                client = SASL_FACTORY.createSaslClient(
                    mechs, authorizationId, protocol, serverName, props, cbh
                );
            }

            if (client != null) {
                return client;
            }
        }

        return null;
    }
}
