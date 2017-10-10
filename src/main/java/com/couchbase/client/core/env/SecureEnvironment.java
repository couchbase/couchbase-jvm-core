/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.core.env;

import java.security.KeyStore;
import com.couchbase.client.core.endpoint.SSLEngineFactory;

import javax.net.ssl.TrustManagerFactory;

/**
 * A {@link SecureEnvironment} interface defines all methods which environment implementation
 * should have to be accepted by {@link SSLEngineFactory}.
 */
public interface SecureEnvironment {

    /**
     * Identifies if SSL should be enabled.
     *
     * @return true if SSL is enabled, false otherwise.
     */
    boolean sslEnabled();

    /**
     * Identifies the filepath to the ssl keystore.
     *
     * If this method is used without also specifying
     * {@link #sslTruststoreFile()} this keystore will be used to initialize
     * both the key factory as well as the trust factory with java SSL. This
     * needs to be the case for backwards compatibility, but if you do not need
     * X.509 client cert authentication you might as well just use {@link #sslTruststoreFile()}
     * alone.
     *
     * @return the path to the keystore file.
     */
    String sslKeystoreFile();

    /**
     * The password which is used to protect the keystore.
     *
     * Only needed if {@link #sslKeystoreFile()} is used.
     *
     * @return the keystore password.
     */
    String sslKeystorePassword();

    /**
     * Allows to directly configure a {@link KeyStore}.
     *
     * If this method is used without also specifying
     * {@link #sslTruststore()} this keystore will be used to initialize
     * both the key factory as well as the trust factory with java SSL. This
     * needs to be the case for backwards compatibility, but if you do not need
     * X.509 client cert authentication you might as well just use {@link #sslTruststore()}
     * alone.
     *
     * @return the keystore to use.
     */
    KeyStore sslKeystore();

    /**
     * Identifies the filepath to the ssl TrustManager keystore.
     *
     * If this method is used without also specifying
     * {@link #sslKeystoreFile()} this keystore will be used to initialize
     * both the key factory as well as the trust factory with java SSL. Prefer
     * this method over the {@link #sslKeystoreFile()} if you do not need
     * X.509 client auth and just need server side certificate checking.
     *
     * @return the path to the truststore file.
     */
    String sslTruststoreFile();

    /**
     * The password which is used to protect the TrustManager keystore.
     *
     * Only needed if {@link #sslTruststoreFile()} is used.
     *
     * @return the keystore password.
     */
    String sslTruststorePassword();

    /**
     * Allows to directly configure a {@link KeyStore}.
     *
     * If this method is used without also specifying
     * {@link #sslKeystore()} this keystore will be used to initialize
     * both the key factory as well as the trust factory with java SSL. Prefer
     * this method over the {@link #sslKeystore()} if you do not need
     * X.509 client auth and just need server side certificate checking.
     *
     * @return the keystore to use when initializing the factory/factories.
     */
    KeyStore sslTruststore();

}
