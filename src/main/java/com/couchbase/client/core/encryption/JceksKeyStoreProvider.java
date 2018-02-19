/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.encryption;

import com.couchbase.client.core.annotations.InterfaceStability;
import javax.crypto.SecretKey;
import java.io.InputStream;
import java.security.KeyStore;

/**
 * JCEKS key store is a superset of JKS which supports both symmetric and asymmetric keys.
 *
 * @author Subhashni Balakrishnan
 * @since 1.6.0
 */
@InterfaceStability.Uncommitted
public class JceksKeyStoreProvider implements KeyStoreProvider {

    private final KeyStore ks;
    private String keyPassword;

    public JceksKeyStoreProvider() throws Exception {
        this(null);
    }

    /**
     * Creates an instance of the JCEKS key store provider
     *
     * @param keyPassword password for key protection. Default password is the key name.
     * @throws Exception
     */
    public JceksKeyStoreProvider(String keyPassword) throws Exception {
        this.ks = KeyStore.getInstance("JCEKS");
        this.ks.load(null, null);
        this.keyPassword = keyPassword;
    }

    /**
     * Creates an instance of the JCEKS key store provider
     *
     * @param stream      Input stream to use an existing key store
     * @param password    Password for the key store
     * @param keyPassword password for key protection. Default password is the key name.
     * @throws Exception
     */
    public JceksKeyStoreProvider(InputStream stream, char[] password, String keyPassword) throws Exception {
        this.ks = KeyStore.getInstance("JCEKS");
        this.ks.load(stream, password);
        this.keyPassword = keyPassword;
    }

    private KeyStore.PasswordProtection getProtection(String keyName) {
        KeyStore.PasswordProtection protection;
        if (this.keyPassword == null) {
            protection = new KeyStore.PasswordProtection(keyName.toCharArray());
        } else {
            protection = new KeyStore.PasswordProtection(this.keyPassword.toCharArray());
        }
        return protection;
    }

    @Override
    public byte[] getKey(String keyName) throws Exception {
        KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) this.ks.getEntry(keyName, getProtection(keyName));
        return entry.getSecretKey().getEncoded();
    }

    @Override
    public void storeKey(String keyName, byte[] secretKey) throws Exception {
        SimpleSecretKey secretKeyEntry = new SimpleSecretKey(secretKey);
        this.ks.setEntry(keyName, new KeyStore.SecretKeyEntry(secretKeyEntry), getProtection(keyName));
    }

    public void storeKey(String keyName, byte[] publicKey, byte[] privateKey) throws Exception {
        SimpleSecretKey privateKeyEntry = new SimpleSecretKey(privateKey);
        this.ks.setEntry(keyName + "_private", new KeyStore.SecretKeyEntry(privateKeyEntry), getProtection(keyName + "_private"));
        SimpleSecretKey publicKeyEntry = new SimpleSecretKey(publicKey);
        this.ks.setEntry(keyName + "_public", new KeyStore.SecretKeyEntry(publicKeyEntry), getProtection(keyName + "_public"));
    }

    private static class SimpleSecretKey implements SecretKey {
        private final byte[] secret;

        public SimpleSecretKey(byte[] secret) {
            this.secret = secret;
        }

        @Override
        public String getAlgorithm() {
            return "CUSTOM";
        }

        @Override
        public String getFormat() {
            return "RAW";
        }

        @Override
        public byte[] getEncoded() {
            return secret;
        }
    }
}