/**
 * Copyright (C) 2014 Couchbase, Inc.
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
package com.couchbase.client.core.env;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import rx.Observable;
import rx.functions.Func1;

/**
 * The {@link CouchbaseEnvironment} wraps the underlying environment retrieval mechanisms and provides convenient
 * access methods to commonly used properties.
 *
 * It is intended to be shared throughout the application and should only be instantiated once.
 */
public class CouchbaseEnvironment implements Environment {

    /**
     * The default namespace to use for settings if not configured otherwise.
     */
    private static final String DEFAULT_NAMESPACE = "com.couchbase.client";

    /**
     * The global namespace used.
     */
    private final String namespace;

    /**
     * References the underlying loaded configuration.
     */
    private final Config config;

    /**
     * The IO pool implementation.
     */
    private final EventLoopGroup ioPool;

    public CouchbaseEnvironment() {
        this(ConfigFactory.load());
    }

    public CouchbaseEnvironment(final Config config) {
        this(config, DEFAULT_NAMESPACE);
    }

    public CouchbaseEnvironment(final Config config, String namespace) {
        this.config = config;
        this.namespace = namespace;

        ioPool = new NioEventLoopGroup(ioPoolSize());
    }

    @Override
    public Observable<Boolean> shutdown() {
        return Observable.from(ioPool.shutdownGracefully()).map(new Func1<Object, Boolean>() {
            @Override
            public Boolean call(Object o) {
                return true;
            }
        });
    }

    @Override
    public boolean sslEnabled() {
        return getBoolean("core.bootstrap.sslEnabled");
    }

    @Override
    public String sslKeystoreFile() {
        return getString("core.bootstrap.sslKeystoreFile");
    }

    @Override
    public String sslKeystorePassword() {
        return getString("core.bootstrap.sslKeystorePassword");
    }

    @Override
    public int bootstrapHttpDirectPort() {
        int port = getInt("core.bootstrap.http.directPort");
        if (port <= 0) {
            throw new EnvironmentException("Port must be greater than 0.");
        }
        return port;
    }

    @Override
    public int bootstrapHttpSslPort() {
        int port = getInt("core.bootstrap.http.sslPort");
        if (port <= 0) {
            throw new EnvironmentException("Port must be greater than 0.");
        }
        return port;
    }

    @Override
    public int bootstrapCarrierDirectPort() {
        int port = getInt("core.bootstrap.carrier.directPort");
        if (port <= 0) {
            throw new EnvironmentException("Port must be greater than 0.");
        }
        return port;
    }

    @Override
    public int bootstrapCarrierSslPort() {
        int port = getInt("core.bootstrap.carrier.sslPort");
        if (port <= 0) {
            throw new EnvironmentException("Port must be greater than 0.");
        }
        return port;
    }

    @Override
    public int ioPoolSize() {
        int ioPoolSize = getInt("core.io.poolSize");
        if (ioPoolSize <= 0) {
            return Runtime.getRuntime().availableProcessors();
        }
        return ioPoolSize;
    }

    @Override
    public int requestBufferSize() {
        int reqBufSize = getInt("core.requestBufferSize");
        if (reqBufSize <= 0) {
            throw new EnvironmentException("Request Buffer Size must be > 0 and power of two");
        }
        return reqBufSize;
    }

    @Override
    public int responseBufferSize() {
        int resBufSize = getInt("core.responseBufferSize");
        if (resBufSize <= 0) {
            throw new EnvironmentException("Response Buffer Size must be > 0 and power of two");
        }
        return resBufSize;
    }

    @Override
    public EventLoopGroup ioPool() {
        return ioPool;
    }

    @Override
    public int binaryServiceEndpoints() {
        int endpoints = getInt("core.service.endpoints.binary");
        if (endpoints <= 0) {
            throw new EnvironmentException("At least one Endpoint per Service is required");
        }
        return endpoints;
    }

    @Override
    public int configServiceEndpoints() {
        int endpoints = getInt("core.service.endpoints.config");
        if (endpoints <= 0) {
            throw new EnvironmentException("At least one Endpoint per Service is required");
        }
        return endpoints;
    }

    @Override
    public int streamServiceEndpoints() {
        int endpoints = getInt("core.service.endpoints.stream");
        if (endpoints <= 0) {
            throw new EnvironmentException("At least one Endpoint per Service is required");
        }
        return endpoints;
    }

    @Override
    public int viewServiceEndpoints() {
        int endpoints = getInt("core.service.endpoints.view");
        if (endpoints <= 0) {
            throw new EnvironmentException("At least one Endpoint per Service is required");
        }
        return endpoints;
    }

    protected int getInt(String path) {
        try {
            return config.getInt(namespace + '.' + path);
        } catch (Exception e) {
            throw new EnvironmentException("Could not load environment setting " + path + '.', e);
        }
    }

    protected long getLong(String path) {
        try {
            return config.getLong(namespace + '.' + path);
        } catch (Exception e) {
            throw new EnvironmentException("Could not load environment setting " + path + '.', e);
        }
    }

    protected String getString(String path) {
        try {
            return config.getString(namespace + '.' + path);
        } catch (Exception e) {
            throw new EnvironmentException("Could not load environment setting " + path + '.', e);
        }
    }

    protected boolean getBoolean(String path) {
        try {
            return config.getBoolean(namespace + '.' + path);
        } catch (Exception e) {
            throw new EnvironmentException("Could not load environment setting " + path + '.', e);
        }
    }
}
