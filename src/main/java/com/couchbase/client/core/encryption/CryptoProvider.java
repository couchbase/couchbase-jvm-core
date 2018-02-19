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
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * CryptoProvider interface for cryptographic algorithm provider implementations.
 *
 * @author Subhashni Balakrishnan
 * @since 1.6.0
 */
@InterfaceStability.Uncommitted
@InterfaceAudience.Public
public interface CryptoProvider {

    /**
     * Get the key store provider set for the Crypto provider use.
     *
     * @return Key store provider set
     */
    KeyStoreProvider getKeyStoreProvider();

    /**
     * Set the key store provider for the Crypto provider to get keys from.
     *
     * @param provider Key store provider
     */
    void setKeyStoreProvider(KeyStoreProvider provider);

    /**
     * The key name used by the crypto provider.
     *
     * @return Key name
     */
    String getKeyName();

    /**
     * Set key name to be used by the crypto provider.
     *
     * @param keyName Key name
     */
    void setKeyName(String keyName);

    /**
     * Encrypts the given data using the key set. Will throw exceptions
     * if the key store and key name are not set.
     *
     * @param data Data to be encrypted
     * @return Encrypted bytes
     * @throws Exception
     */
    byte[] encrypt(byte[] data) throws Exception;

    /**
     * Decrypts the given data using the key set. Will throw exceptions
     * if the key store and key name are not set.
     *
     * @param encrypted Encrypted data
     * @return Decrypted bytes
     * @throws Exception
     */
    byte[] decrypt(byte[] encrypted) throws Exception;

    /**
     * Get the crypto provider name.
     *
     * @return Name
     */
    String getProviderName();
}