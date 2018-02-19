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

import com.couchbase.client.core.utils.Base64;
import org.junit.Assert;
import org.junit.Test;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.SecureRandom;
import java.util.Arrays;

public class JceksKeyStoreProviderTest {

    @Test
    public void testSimpleKeyStore() throws Exception {
        JceksKeyStoreProvider provider = new JceksKeyStoreProvider();
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        SecureRandom random = new SecureRandom();
        keyGen.init(128, random);
        SecretKey secretKey = keyGen.generateKey();
        String keyName = "testkey";
        provider.storeKey(keyName, secretKey.getEncoded());
        byte[] secret = provider.getKey(keyName);
        AES128CryptoProvider cryptoProvider = new AES128CryptoProvider();
        cryptoProvider.setKeyStoreProvider(provider);
        cryptoProvider.setKeyName(keyName);
        String encrypted = Base64.encode(cryptoProvider.encrypt("test".getBytes()));
        String decrypted = new String(cryptoProvider.decrypt(Base64.decode(encrypted)));
        Assert.assertTrue(Arrays.equals(secret, secretKey.getEncoded()));
        Assert.assertEquals(decrypted, "test");
    }
}
