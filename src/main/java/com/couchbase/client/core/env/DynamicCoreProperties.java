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

public class DynamicCoreProperties implements CoreProperties {

    private static final String NAMESPACE = "com.couchbase.";

    private final boolean sslEnabled;
    private final String sslKeystoreFile;
    private final String sslKeystorePassword;
    private final boolean queryEnabled;
    private final int queryPort;
    private final boolean bootstrapHttpEnabled;
    private final boolean bootstrapCarrierEnabled;
    private final int bootstrapHttpDirectPort;
    private final int bootstrapHttpSslPort;
    private final int bootstrapCarrierDirectPort;
    private final int bootstrapCarrierSslPort;
    private final int ioPoolSize;
    private final int computationPoolSize;
    private final int responseBufferSize;
    private final int requestBufferSize;
    private final int binaryServiceEndpoints;
    private final int viewServiceEndpoints;
    private final int queryServiceEndpoints;

    DynamicCoreProperties(final Builder builder) {
        sslEnabled = booleanPropertyOr("sslEnabled", builder.sslEnabled());
        sslKeystoreFile = stringPropertyOr("sslKeystoreFile", builder.sslKeystoreFile());
        sslKeystorePassword = stringPropertyOr("sslKeystorePassword", builder.sslKeystorePassword());
        queryEnabled = booleanPropertyOr("queryEnabled", builder.queryEnabled());
        queryPort = intPropertyOr("queryPort", builder.queryPort());
        bootstrapHttpEnabled = booleanPropertyOr("bootstrapHttpEnabled", builder.bootstrapHttpEnabled());
        bootstrapHttpDirectPort = intPropertyOr("bootstrapHttpDirectPort", builder.bootstrapHttpDirectPort());
        bootstrapHttpSslPort = intPropertyOr("bootstrapHttpSslPort", builder.bootstrapHttpSslPort());
        bootstrapCarrierEnabled = booleanPropertyOr("bootstrapCarrierEnabled", builder.bootstrapCarrierEnabled());
        bootstrapCarrierDirectPort = intPropertyOr("bootstrapCarrierDirectPort", builder.bootstrapCarrierDirectPort());
        bootstrapCarrierSslPort = intPropertyOr("bootstrapCarrierSslPort", builder.bootstrapCarrierSslPort());
        ioPoolSize = intPropertyOr("ioPoolSize", builder.ioPoolSize());
        computationPoolSize = intPropertyOr("computationPoolSize", builder.computationPoolSize());
        responseBufferSize = intPropertyOr("responseBufferSize", builder.responseBufferSize());
        requestBufferSize = intPropertyOr("requestBufferSize", builder.requestBufferSize());
        binaryServiceEndpoints = intPropertyOr("binaryServiceEndpoints", builder.binaryServiceEndpoints());
        viewServiceEndpoints = intPropertyOr("viewServiceEndpoints", builder.viewServiceEndpoints());
        queryServiceEndpoints = intPropertyOr("queryServiceEndpoints", builder.queryServiceEndpoints());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DynamicCoreProperties create() {
        return new DynamicCoreProperties(new Builder());
    }

    private boolean booleanPropertyOr(String path, boolean def) {
        String found = System.getProperty(NAMESPACE + path);
        if (found == null) {
            return def;
        }
        return Boolean.parseBoolean(found);
    }

    private String stringPropertyOr(String path, String def) {
        String found = System.getProperty(NAMESPACE + path);
        return found == null ? def : found;
    }

    private int intPropertyOr(String path, int def) {
        String found = System.getProperty(NAMESPACE + path);
        if (found == null) {
            return def;
        }
        return Integer.parseInt(found);
    }

    @Override
    public boolean sslEnabled() {
        return sslEnabled;
    }

    @Override
    public String sslKeystoreFile() {
        return sslKeystoreFile;
    }

    @Override
    public String sslKeystorePassword() {
        return sslKeystorePassword;
    }

    @Override
    public boolean queryEnabled() {
        return queryEnabled;
    }

    @Override
    public int queryPort() {
        return queryPort;
    }

    @Override
    public boolean bootstrapHttpEnabled() {
        return bootstrapHttpEnabled;
    }

    @Override
    public boolean bootstrapCarrierEnabled() {
        return bootstrapCarrierEnabled;
    }

    @Override
    public int bootstrapHttpDirectPort() {
        return bootstrapHttpDirectPort;
    }

    @Override
    public int bootstrapHttpSslPort() {
        return bootstrapHttpSslPort;
    }

    @Override
    public int bootstrapCarrierDirectPort() {
        return bootstrapCarrierDirectPort;
    }

    @Override
    public int bootstrapCarrierSslPort() {
        return bootstrapCarrierSslPort;
    }

    @Override
    public int ioPoolSize() {
        return ioPoolSize;
    }

    @Override
    public int computationPoolSize() {
        return computationPoolSize;
    }

    @Override
    public int requestBufferSize() {
        return requestBufferSize;
    }

    @Override
    public int responseBufferSize() {
        return responseBufferSize;
    }

    @Override
    public int binaryServiceEndpoints() {
        return binaryServiceEndpoints;
    }

    @Override
    public int viewServiceEndpoints() {
        return viewServiceEndpoints;
    }

    @Override
    public int queryServiceEndpoints() {
        return queryServiceEndpoints;
    }

    public static class Builder implements CoreProperties {

        private boolean sslEnabled = DefaultCoreProperties.SSL_ENABLED;
        private String sslKeystoreFile = DefaultCoreProperties.SSL_KEYSTORE_FILE;
        private String sslKeystorePassword = DefaultCoreProperties.SSL_KEYSTORE_PASSWORD;
        private boolean queryEnabled = DefaultCoreProperties.QUERY_ENABLED;
        private int queryPort = DefaultCoreProperties.QUERY_PORT;
        private boolean bootstrapHttpEnabled = DefaultCoreProperties.BOOTSTRAP_HTTP_ENABLED;
        private boolean bootstrapCarrierEnabled = DefaultCoreProperties.BOOTSTRAP_CARRIER_ENABLED;
        private int bootstrapHttpDirectPort = DefaultCoreProperties.BOOTSTRAP_HTTP_DIRECT_PORT;
        private int bootstrapHttpSslPort = DefaultCoreProperties.BOOTSTRAP_HTTP_SSL_PORT;
        private int bootstrapCarrierDirectPort = DefaultCoreProperties.BOOTSTRAP_CARRIER_DIRECT_PORT;
        private int bootstrapCarrierSslPort = DefaultCoreProperties.BOOTSTRAP_CARRIER_SSL_PORT;
        private int ioPoolSize = DefaultCoreProperties.IO_POOL_SIZE;
        private int computationPoolSize = DefaultCoreProperties.COMPUTATION_POOL_SIZE;
        private int responseBufferSize = DefaultCoreProperties.RESPONSE_BUFFER_SIZE;
        private int requestBufferSize = DefaultCoreProperties.REQUEST_BUFFER_SIZE;
        private int binaryServiceEndpoints = DefaultCoreProperties.BINARY_ENDPOINTS;
        private int viewServiceEndpoints = DefaultCoreProperties.VIEW_ENDPOINTS;
        private int queryServiceEndpoints = DefaultCoreProperties.QUERY_ENDPOINTS;

        Builder() {

        }

        @Override
        public boolean sslEnabled() {
            return sslEnabled;
        }

        public Builder sslEnabled(final boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return this;
        }

        @Override
        public String sslKeystoreFile() {
            return sslKeystoreFile;
        }

        public Builder sslKeystoreFile(final String sslKeystoreFile) {
            this.sslKeystoreFile = sslKeystoreFile;
            return this;
        }

        @Override
        public String sslKeystorePassword() {
            return sslKeystorePassword;
        }

        public Builder sslKeystorePassword(final String sslKeystorePassword) {
            this.sslKeystorePassword = sslKeystorePassword;
            return this;
        }

        @Override
        public boolean queryEnabled() {
            return queryEnabled;
        }

        public Builder queryEnabled(final boolean queryEnabled) {
            this.queryEnabled = queryEnabled;
            return this;
        }

        @Override
        public int queryPort() {
            return queryPort;
        }

        public Builder queryPort(final int queryPort) {
            this.queryPort = queryPort;
            return this;
        }

        @Override
        public boolean bootstrapHttpEnabled() {
            return bootstrapHttpEnabled;
        }

        public Builder bootstrapHttpEnabled(final boolean bootstrapHttpEnabled) {
            this.bootstrapHttpEnabled = bootstrapHttpEnabled;
            return this;
        }

        @Override
        public boolean bootstrapCarrierEnabled() {
            return bootstrapCarrierEnabled;
        }

        public Builder bootstrapCarrierEnabled(final boolean bootstrapCarrierEnabled) {
            this.bootstrapCarrierEnabled = bootstrapCarrierEnabled;
            return this;
        }

        @Override
        public int bootstrapHttpDirectPort() {
            return bootstrapHttpDirectPort;
        }

        public Builder bootstrapHttpDirectPort(final int bootstrapHttpDirectPort) {
            this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
            return this;
        }

        @Override
        public int bootstrapHttpSslPort() {
            return bootstrapHttpSslPort;
        }

        public Builder bootstrapHttpSslPort(final int bootstrapHttpSslPort) {
            this.bootstrapHttpSslPort = bootstrapHttpSslPort;
            return this;
        }

        @Override
        public int bootstrapCarrierDirectPort() {
            return bootstrapCarrierDirectPort;
        }

        public Builder bootstrapCarrierDirectPort(final int bootstrapCarrierDirectPort) {
            this.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
            return this;
        }

        @Override
        public int bootstrapCarrierSslPort() {
            return bootstrapCarrierSslPort;
        }

        public Builder bootstrapCarrierSslPort(final int bootstrapCarrierSslPort) {
            this.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
            return this;
        }

        @Override
        public int ioPoolSize() {
            return ioPoolSize;
        }

        public Builder ioPoolSize(final int ioPoolSize) {
            this.ioPoolSize = ioPoolSize;
            return this;
        }

        @Override
        public int computationPoolSize() {
            return computationPoolSize;
        }

        public Builder computationPoolSize(final int computationPoolSize) {
            this.computationPoolSize = computationPoolSize;
            return this;
        }

        @Override
        public int requestBufferSize() {
            return requestBufferSize;
        }

        public Builder requestBufferSize(final int requestBufferSize) {
            this.requestBufferSize = requestBufferSize;
            return this;
        }

        @Override
        public int responseBufferSize() {
            return responseBufferSize;
        }

        public Builder responseBufferSize(final int responseBufferSize) {
            this.responseBufferSize = responseBufferSize;
            return this;
        }

        @Override
        public int binaryServiceEndpoints() {
            return binaryServiceEndpoints;
        }

        public Builder binaryServiceEndpoints(final int binaryServiceEndpoints) {
            this.binaryServiceEndpoints = binaryServiceEndpoints;
            return this;
        }

        @Override
        public int viewServiceEndpoints() {
            return viewServiceEndpoints;
        }

        public Builder viewServiceEndpoints(final int viewServiceEndpoints) {
            this.viewServiceEndpoints = viewServiceEndpoints;
            return this;
        }

        @Override
        public int queryServiceEndpoints() {
            return queryServiceEndpoints;
        }

        public Builder queryServiceEndpoints(final int queryServiceEndpoints) {
            this.queryServiceEndpoints = queryServiceEndpoints;
            return this;
        }

        public DynamicCoreProperties build() {
            return new DynamicCoreProperties(this);
        }
    }
}
