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

import com.couchbase.client.core.annotations.InterfaceAudience;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;

/**
 * Base class for AES crypto provider
 *
 * @since 1.6.0
 */
@InterfaceAudience.Private
public class AESCryptoProviderBase implements CryptoProvider {

    private KeyStoreProvider keyStoreProvider = null;
    private String keyName = null;
    protected int keySize = 32;

    @Override
    public KeyStoreProvider getKeyStoreProvider() {
        return this.keyStoreProvider;
    }

    @Override
    public void setKeyStoreProvider(KeyStoreProvider provider) {
        this.keyStoreProvider = provider;
    }

    @Override
    public String getKeyName() {
        return this.keyName;
    }

    @Override
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public byte[] encrypt(byte[] data) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        SecretKeySpec key = new SecretKeySpec(this.keyStoreProvider.getKey(this.keyName), "AES");
        checkKeySize(key);

        //16 byte IV
        int ivSize = 16;
        byte[] iv = new byte[ivSize];
        SecureRandom random = new SecureRandom();
        random.nextBytes(iv);
        IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);

        cipher.init(Cipher.ENCRYPT_MODE, key, ivParameterSpec);
        byte[] encrypted = cipher.doFinal(data);
        byte[] encryptedWithIv = new byte[encrypted.length + ivSize];
        System.arraycopy(iv, 0, encryptedWithIv, 0, ivSize);
        System.arraycopy(encrypted, 0, encryptedWithIv, ivSize, encrypted.length);
        return encryptedWithIv;
    }

    @Override
    public byte[] decrypt(byte[] encryptedWithIv) throws Exception {
        SecretKeySpec key = new SecretKeySpec(this.keyStoreProvider.getKey(this.keyName), "AES");
        checkKeySize(key);

        int ivSize = 16;
        byte[] iv = new byte[ivSize];
        System.arraycopy(encryptedWithIv, 0, iv, 0, ivSize);
        IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);

        int encryptedSize = encryptedWithIv.length - ivSize;
        byte[] encryptedBytes = new byte[encryptedSize];
        System.arraycopy(encryptedWithIv, ivSize, encryptedBytes, 0, encryptedSize);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

        cipher.init(Cipher.DECRYPT_MODE, key, ivParameterSpec);
        return cipher.doFinal(encryptedBytes);
    }

    @Override
    public String getProviderName() {
        return "AES";
    }

    private void checkKeySize(SecretKeySpec key) throws Exception {
        int keySize = key.getEncoded().length;
        if (keySize != this.keySize) {
            throw new Exception("Invalid key size " + keySize + " for "+ this.getProviderName() +" Algorithm");
        }
    }
}