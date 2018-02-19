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
import java.util.HashMap;

/**
 * Insecure key store which stores keys in memory, useful for development testing.
 *
 * @author Subhashni Balakrishnan
 * @since 1.6.0
 */
@InterfaceStability.Uncommitted
public class InsecureKeyStoreProvider implements KeyStoreProvider {

    private HashMap<String, byte[]> keys = new HashMap<String, byte[]>();

    @Override
    public byte[] getKey(String keyName) {
        return keys.get(keyName);
    }

    @Override
    public void storeKey(String keyName, byte[] secretKey) {
        keys.put(keyName, secretKey);
    }

    @Override
    public void storeKey(String keyName, byte[] publicKey, byte[] privateKey) {
        keys.put(keyName + "_public", publicKey);
        keys.put(keyName + "_private", privateKey);
    }
}