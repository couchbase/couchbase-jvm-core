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
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * The {@link SaslClientFactory} supporting SCRAM-SHA512, SCRAM-SHA256 and SCRAM-SHA1
 * authentication methods.
 *
 * @author Trond Norbye
 * @version 1.2.5
 */
public class ShaSaslClientFactory implements SaslClientFactory {

    private static final String SCRAM_SHA512 = "SCRAM-SHA512";
    private static final String SCRAM_SHA256 = "SCRAM-SHA256";
    private static final String SCRAM_SHA1 = "SCRAM-SHA1";
    private static final String[] SUPPORTED_MECHS = { SCRAM_SHA512, SCRAM_SHA256, SCRAM_SHA1 };

    @Override
    public SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol,
        String serverName, Map<String, ?> props, CallbackHandler cbh) throws SaslException {

        int sha = 0;

        for (String m : mechanisms) {
            if (m.equals(SCRAM_SHA512)) {
                sha = 512;
                break;
            } else if (m.equals(SCRAM_SHA256)) {
                sha = 256;
                break;
            } else if (m.equals(SCRAM_SHA1)) {
                sha = 1;
                break;
            }
        }

        if (sha == 0) {
            return null;
        }

        if (authorizationId != null) {
            throw new SaslException("authorizationId is not supported");
        }

        if (cbh == null) {
            throw new SaslException("Callback handler to get username/password required");
        }

        try {
            return new ShaSaslClient(cbh, sha);
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
        return SUPPORTED_MECHS;
    }
}