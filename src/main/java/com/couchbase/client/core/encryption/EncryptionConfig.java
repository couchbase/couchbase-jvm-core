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

import java.util.HashMap;
import java.util.Map;
import com.couchbase.client.core.annotations.InterfaceStability;

/**
 * Encryption configuration set on the environment for encryption/decryption
 *
 * @author Subhashni Balakrishnan
 * @since 1.6.0
 */
@InterfaceStability.Uncommitted
public class EncryptionConfig {

    private Map<String, CryptoProvider> cryptoProvider;

    public EncryptionConfig() {
        this.cryptoProvider = new HashMap<String, CryptoProvider>();
    }

    /**
     * Add an encryption algorithm provider
     *
     * @param provider Encryption provider implementation
     */
    public void addCryptoProvider(CryptoProvider provider) {
        this.cryptoProvider.put(provider.getProviderName(), provider);
    }

    /**
     * Get an encryption algorithm provider
     * @param providerName encryption provider name
     *
     * @return encryption crypto provider instance
     */
    public CryptoProvider getCryptoProvider(String providerName) {
        return this.cryptoProvider.get(providerName);
    }
}