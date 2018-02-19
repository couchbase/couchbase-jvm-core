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
 * Key provider interface for key store implementations.
 *
 * @author Subhashni Balakrishnan
 * @since 1.6.0
 */
@InterfaceStability.Uncommitted
public interface KeyStoreProvider {

    /**
     * Internally used by crypto providers to retrieve the key for encryption/decryption.
     *
     * @param keyName The key to be retrieved for secret keys. Add suffix _public/_private to retrieve
     *                public/private key
     * @return key Key as raw bytes
     * @throws Exception
     */
    @InterfaceAudience.Private
    byte[] getKey(String keyName) throws Exception;

    /**
     * Add a secret key for symmetric key encryption
     *
     * @param keyName   Name of the secret key
     * @param secretKey Secret key as byes
     * @throws Exception
     */
    @InterfaceAudience.Public
    void storeKey(String keyName, byte[] secretKey) throws Exception;

    /**
     * Add public and private keys for asymmetric key encryption/decryption.
     *
     * @param keyName    Common name for the keys
     * @param publicKey  Public key as bytes
     * @param privateKey Private key as bytes
     * @throws Exception
     */
    @InterfaceAudience.Public
    void storeKey(String keyName, byte[] publicKey, byte[] privateKey) throws Exception;
}