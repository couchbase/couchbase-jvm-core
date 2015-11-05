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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.env.resources.IoPoolShutdownHook;
import com.couchbase.client.core.env.resources.NettyShutdownHook;
import com.couchbase.client.core.env.resources.NoOpShutdownHook;
import com.couchbase.client.core.env.resources.ShutdownHook;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.DefaultEventBus;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.event.consumers.LoggingConsumer;
import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.observe.Observe;
import com.couchbase.client.core.metrics.DefaultLatencyMetricsCollectorConfig;
import com.couchbase.client.core.metrics.DefaultMetricsCollectorConfig;
import com.couchbase.client.core.metrics.LatencyMetricsCollectorConfig;
import com.couchbase.client.core.metrics.MetricsCollector;
import com.couchbase.client.core.metrics.MetricsCollectorConfig;
import com.couchbase.client.core.metrics.NetworkLatencyMetricsCollector;
import com.couchbase.client.core.metrics.RuntimeMetricsCollector;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.time.Delay;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DefaultCoreEnvironment implements CoreEnvironment {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CoreEnvironment.class);

    public static final boolean DCP_ENABLED = false;
    public static final boolean SSL_ENABLED = false;
    public static final String SSL_KEYSTORE_FILE = null;
    public static final String SSL_KEYSTORE_PASSWORD = null;
    public static final boolean QUERY_ENABLED = false;
    public static final int QUERY_PORT = 8093;
    public static final boolean BOOTSTRAP_HTTP_ENABLED = true;
    public static final boolean BOOTSTRAP_CARRIER_ENABLED = true;
    public static final int BOOTSTRAP_HTTP_DIRECT_PORT = 8091;
    public static final int BOOTSTRAP_HTTP_SSL_PORT = 18091;
    public static final int BOOTSTRAP_CARRIER_DIRECT_PORT = 11210;
    public static final int BOOTSTRAP_CARRIER_SSL_PORT = 11207;
    public static final int REQUEST_BUFFER_SIZE = 16384;
    public static final int RESPONSE_BUFFER_SIZE = 16384;
    public static final int DCP_CONNECTION_BUFFER_SIZE = 20971520; // 20MiB
    public static final double DCP_CONNECTION_BUFFER_ACK_THRESHOLD = 0.2; // for 20Mib it is 4MiB
    public static final int IO_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    public static final int COMPUTATION_POOL_SIZE =  Runtime.getRuntime().availableProcessors();
    public static final int KEYVALUE_ENDPOINTS = 1;
    public static final int VIEW_ENDPOINTS = 1;
    public static final int QUERY_ENDPOINTS = 1;
    public static final Delay OBSERVE_INTERVAL_DELAY = Delay.exponential(TimeUnit.MICROSECONDS, 100000, 10);
    public static final Delay RECONNECT_DELAY = Delay.exponential(TimeUnit.MILLISECONDS, 4096, 32);
    public static final Delay RETRY_DELAY = Delay.exponential(TimeUnit.MICROSECONDS, 100000, 100);
    public static final RetryStrategy RETRY_STRATEGY = BestEffortRetryStrategy.INSTANCE;
    public static final long MAX_REQUEST_LIFETIME = TimeUnit.SECONDS.toMillis(75);
    public static final long KEEPALIVEINTERVAL = TimeUnit.SECONDS.toMillis(30);
    public static final long AUTORELEASE_AFTER = TimeUnit.SECONDS.toMillis(2);
    public static final boolean BUFFER_POOLING_ENABLED = true;
    public static final boolean TCP_NODELAY_ENALED = true;
    public static final boolean MUTATION_TOKENS_ENABLED = false;
    public static final int SOCKET_CONNECT_TIMEOUT = 1000;

    public static String PACKAGE_NAME_AND_VERSION = "couchbase-jvm-core";
    public static String USER_AGENT = PACKAGE_NAME_AND_VERSION;

    private static final String NAMESPACE = "com.couchbase.";

    /**
     * The minimum size of the io and computation pools in order to prevent deadlock and resource
     * starvation.
     *
     * Normally this should be higher by default, but if the number of cores are very small or the configuration
     * is wrong it can even go down to 1.
     */
    static final int MIN_POOL_SIZE = 3;

    private static final String VERSION_PROPERTIES = "com.couchbase.client.core.properties";

    /**
     * Sets up the package version and user agent.
     *
     * Note that because the class loader loads classes on demand, one class from the package
     * is loaded upfront.
     */
    static {
        try {
            Class<ClusterFacade> facadeClass = ClusterFacade.class;
            if (facadeClass == null) {
                throw new IllegalStateException("Could not locate ClusterFacade");
            }

            String version = null;
            String gitVersion = null;
            try {
                Properties versionProp = new Properties();
                versionProp.load(DefaultCoreEnvironment.class.getClassLoader().getResourceAsStream(VERSION_PROPERTIES));
                version = versionProp.getProperty("specificationVersion");
                gitVersion = versionProp.getProperty("implementationVersion");
            } catch (Exception e) {
                LOGGER.info("Could not retrieve version properties, defaulting.", e);
            }
            PACKAGE_NAME_AND_VERSION = String.format("couchbase-jvm-core/%s (git: %s)",
                version == null ? "unknown" : version, gitVersion == null ? "unknown" : gitVersion);

            USER_AGENT = String.format("%s (%s/%s %s; %s %s)",
                PACKAGE_NAME_AND_VERSION,
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                System.getProperty("os.arch"),
                System.getProperty("java.vm.name"),
                System.getProperty("java.runtime.version")
            );
        } catch (Exception ex) {
            LOGGER.info("Could not set up user agent and packages, defaulting.", ex);
        }
    }

    private final boolean dcpEnabled;
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
    private final int dcpConnectionBufferSize;
    private final double dcpConnectionBufferAckThreshold;
    private final int kvServiceEndpoints;
    private final int viewServiceEndpoints;
    private final int queryServiceEndpoints;
    private final Delay observeIntervalDelay;
    private final Delay reconnectDelay;
    private final Delay retryDelay;
    private final String userAgent;
    private final String packageNameAndVersion;
    private final RetryStrategy retryStrategy;
    private final long maxRequestLifetime;
    private final long keepAliveInterval;
    private final long autoreleaseAfter;
    private final boolean bufferPoolingEnabled;
    private final boolean tcpNodelayEnabled;
    private final boolean mutationTokensEnabled;
    private final int socketConnectTimeout;

    private static final int MAX_ALLOWED_INSTANCES = 1;
    private static volatile int instanceCounter = 0;

    private final EventLoopGroup ioPool;
    private final Scheduler coreScheduler;
    private final EventBus eventBus;

    private final ShutdownHook ioPoolShutdownHook;
    private final ShutdownHook nettyShutdownHook;
    private final ShutdownHook coreSchedulerShutdownHook;

    private final MetricsCollector runtimeMetricsCollector;
    private final NetworkLatencyMetricsCollector networkLatencyMetricsCollector;
    private final Subscription metricsCollectorSubscription;

    protected DefaultCoreEnvironment(final Builder builder) {
        if (++instanceCounter > MAX_ALLOWED_INSTANCES) {
            LOGGER.warn("More than " + MAX_ALLOWED_INSTANCES + " Couchbase Environments found (" + instanceCounter
                + "), this can have severe impact on performance and stability. Reuse environments!");
        }
        dcpEnabled = booleanPropertyOr("dcpEnabled", builder.dcpEnabled);
        sslEnabled = booleanPropertyOr("sslEnabled", builder.sslEnabled);
        sslKeystoreFile = stringPropertyOr("sslKeystoreFile", builder.sslKeystoreFile);
        sslKeystorePassword = stringPropertyOr("sslKeystorePassword", builder.sslKeystorePassword);
        queryEnabled = booleanPropertyOr("queryEnabled", builder.queryEnabled);
        queryPort = intPropertyOr("queryPort", builder.queryPort);
        bootstrapHttpEnabled = booleanPropertyOr("bootstrapHttpEnabled", builder.bootstrapHttpEnabled);
        bootstrapHttpDirectPort = intPropertyOr("bootstrapHttpDirectPort", builder.bootstrapHttpDirectPort);
        bootstrapHttpSslPort = intPropertyOr("bootstrapHttpSslPort", builder.bootstrapHttpSslPort);
        bootstrapCarrierEnabled = booleanPropertyOr("bootstrapCarrierEnabled", builder.bootstrapCarrierEnabled);
        bootstrapCarrierDirectPort = intPropertyOr("bootstrapCarrierDirectPort", builder.bootstrapCarrierDirectPort);
        bootstrapCarrierSslPort = intPropertyOr("bootstrapCarrierSslPort", builder.bootstrapCarrierSslPort);
        int ioPoolSize = intPropertyOr("ioPoolSize", builder.ioPoolSize);
        int computationPoolSize = intPropertyOr("computationPoolSize", builder.computationPoolSize);
        responseBufferSize = intPropertyOr("responseBufferSize", builder.responseBufferSize);
        requestBufferSize = intPropertyOr("requestBufferSize", builder.requestBufferSize);
        dcpConnectionBufferSize = intPropertyOr("dcpConnectionBufferSize", builder.dcpConnectionBufferSize);
        dcpConnectionBufferAckThreshold = doublePropertyOr("dcpConnectionBufferAckThreshold", builder.dcpConnectionBufferAckThreshold);
        kvServiceEndpoints = intPropertyOr("kvEndpoints", builder.kvEndpoints);
        viewServiceEndpoints = intPropertyOr("viewEndpoints", builder.viewEndpoints);
        queryServiceEndpoints = intPropertyOr("queryEndpoints", builder.queryEndpoints);
        packageNameAndVersion = stringPropertyOr("packageNameAndVersion", builder.packageNameAndVersion);
        userAgent = stringPropertyOr("userAgent", builder.userAgent);
        observeIntervalDelay = builder.observeIntervalDelay;
        reconnectDelay = builder.reconnectDelay;
        retryDelay = builder.retryDelay;
        retryStrategy = builder.retryStrategy;
        maxRequestLifetime = longPropertyOr("maxRequestLifetime", builder.maxRequestLifetime);
        keepAliveInterval = longPropertyOr("keepAliveInterval", builder.keepAliveInterval);
        autoreleaseAfter = longPropertyOr("autoreleaseAfter", builder.autoreleaseAfter);
        bufferPoolingEnabled = booleanPropertyOr("bufferPoolingEnabled", builder.bufferPoolingEnabled);
        tcpNodelayEnabled = booleanPropertyOr("tcpNodelayEnabled", builder.tcpNodelayEnabled);
        mutationTokensEnabled = booleanPropertyOr("mutationTokensEnabled", builder.mutationTokensEnabled);
        socketConnectTimeout = intPropertyOr("socketConnectTimeout", builder.socketConnectTimeout);

        if (ioPoolSize < MIN_POOL_SIZE) {
            LOGGER.info("ioPoolSize is less than {} ({}), setting to: {}", MIN_POOL_SIZE, ioPoolSize, MIN_POOL_SIZE);
            this.ioPoolSize = MIN_POOL_SIZE;
        } else {
            this.ioPoolSize = ioPoolSize;
        }

        if (computationPoolSize < MIN_POOL_SIZE) {
            LOGGER.info("computationPoolSize is less than {} ({}), setting to: {}", MIN_POOL_SIZE, computationPoolSize,
                MIN_POOL_SIZE);
            this.computationPoolSize = MIN_POOL_SIZE;
        } else {
            this.computationPoolSize = computationPoolSize;
        }

        if (builder.ioPool == null) {
            this.ioPool = new NioEventLoopGroup(ioPoolSize(), new DefaultThreadFactory("cb-io", true));
            this.ioPoolShutdownHook = new IoPoolShutdownHook(this.ioPool);
        } else {
            this.ioPool = builder.ioPool;
            this.ioPoolShutdownHook = builder.ioPoolShutdownHook == null
                    ? new NoOpShutdownHook()
                    : builder.ioPoolShutdownHook;
        }

        if (!(this.ioPoolShutdownHook instanceof NoOpShutdownHook)) {
            this.nettyShutdownHook = new NettyShutdownHook();
        } else {
            this.nettyShutdownHook = this.ioPoolShutdownHook;
        }

        if (builder.scheduler == null) {
            CoreScheduler managed = new CoreScheduler(computationPoolSize());
            this.coreScheduler = managed;
            this.coreSchedulerShutdownHook = managed;
        } else {
            this.coreScheduler = builder.scheduler;
            this.coreSchedulerShutdownHook = builder.schedulerShutdownHook == null
                    ? new NoOpShutdownHook()
                    : builder.schedulerShutdownHook;
        }
        this.eventBus = builder.eventBus == null ? new DefaultEventBus(coreScheduler) : builder.eventBus;
        this.runtimeMetricsCollector = new RuntimeMetricsCollector(
            eventBus,
            coreScheduler,
            builder.runtimeMetricsCollectorConfig == null
                ? DefaultMetricsCollectorConfig.create()
                : builder.runtimeMetricsCollectorConfig
        );
        this.networkLatencyMetricsCollector = new NetworkLatencyMetricsCollector(
            eventBus,
            coreScheduler,
            builder.networkLatencyMetricsCollectorConfig == null
                ? DefaultLatencyMetricsCollectorConfig.create()
                : builder.networkLatencyMetricsCollectorConfig
        );

        if (builder.defaultMetricsLoggingConsumer != null) {
            metricsCollectorSubscription = eventBus
                .get()
                .filter(new Func1<CouchbaseEvent, Boolean>() {
                    @Override
                    public Boolean call(CouchbaseEvent evt) {
                        return evt.type().equals(EventType.METRIC);
                    }
                })
                .subscribe(builder.defaultMetricsLoggingConsumer);
        } else {
            metricsCollectorSubscription = null;
        }
    }

    public static DefaultCoreEnvironment create() {
        return new DefaultCoreEnvironment(builder());
    }

    public static Builder builder() {
        return new Builder();
    }

    protected boolean booleanPropertyOr(String path, boolean def) {
        String found = System.getProperty(NAMESPACE + path);
        if (found == null) {
            return def;
        }
        return Boolean.parseBoolean(found);
    }

    protected String stringPropertyOr(String path, String def) {
        String found = System.getProperty(NAMESPACE + path);
        return found == null ? def : found;
    }

    protected int intPropertyOr(String path, int def) {
        String found = System.getProperty(NAMESPACE + path);
        if (found == null) {
            return def;
        }
        return Integer.parseInt(found);
    }

    protected static long longPropertyOr(String path, long def) {
        String found = System.getProperty(NAMESPACE + path);
        if (found == null) {
            return def;
        }
        return Integer.parseInt(found);
    }

    protected static double doublePropertyOr(String path, double def) {
        String found = System.getProperty(NAMESPACE + path);
        if (found == null) {
            return def;
        }
        return Double.parseDouble(found);
    }

    @Override
    public EventLoopGroup ioPool() {
        return ioPool;
    }

    @Override
    @Deprecated
    public Observable<Boolean> shutdown() {
        return shutdownAsync();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Observable<Boolean> shutdownAsync() {
        if (metricsCollectorSubscription != null && !metricsCollectorSubscription.isUnsubscribed()) {
            metricsCollectorSubscription.unsubscribe();
        }

        Observable<Boolean> result = Observable.merge(
                wrapShutdown(ioPoolShutdownHook.shutdown(), "IoPool"),
                wrapBestEffortShutdown(nettyShutdownHook.shutdown(), "Netty"),
                wrapShutdown(coreSchedulerShutdownHook.shutdown(), "Core Scheduler"),
                wrapShutdown(Observable.just(runtimeMetricsCollector.shutdown()), "Runtime Metrics Collector"),
                wrapShutdown(Observable.just(networkLatencyMetricsCollector.shutdown()), "Latency Metrics Collector"))
                .reduce(true,
                        new Func2<Boolean, ShutdownStatus, Boolean>() {
                            @Override
                            public Boolean call(Boolean previousStatus, ShutdownStatus currentStatus) {
                                return previousStatus && currentStatus.success;
                            }
                        });
        return result;
    }

    /**
     * This method wraps an Observable of Boolean (for shutdown hook) into an Observable of ShutdownStatus.
     * It will log each status with a short message indicating which target has been shut down, and the result of
     * the call.
     * Additionally it will ignore signals that shutdown status is false (as long as no exception is detected), logging that the target is "best effort" only.
     */
    private Observable<ShutdownStatus> wrapBestEffortShutdown(Observable<Boolean> source, final String target) {
        return wrapShutdown(source, target)
                .map(new Func1<ShutdownStatus, ShutdownStatus>() {
                    @Override
                    public ShutdownStatus call(ShutdownStatus original) {
                        if (original.cause == null && !original.success) {
                            LOGGER.info(target + " shutdown is best effort, ignoring failure");
                            return new ShutdownStatus(target, true, null);
                        } else {
                            return original;
                        }
                    }
                });
    }

    /**
     * This method wraps an Observable of Boolean (for shutdown hook) into an Observable of ShutdownStatus.
     * It will log each status with a short message indicating which target has been shut down, and the result of
     * the call.
     */
    private Observable<ShutdownStatus> wrapShutdown(Observable<Boolean> source, final String target) {
        return source.
                reduce(true, new Func2<Boolean, Boolean, Boolean>() {
                    @Override
                    public Boolean call(Boolean previousStatus, Boolean currentStatus) {
                        return previousStatus && currentStatus;
                    }
                })
                .map(new Func1<Boolean, ShutdownStatus>() {
                    @Override
                    public ShutdownStatus call(Boolean status) {
                        return new ShutdownStatus(target, status, null);
                    }
                })
                .onErrorReturn(new Func1<Throwable, ShutdownStatus>() {
                    @Override
                    public ShutdownStatus call(Throwable throwable) {
                        return new ShutdownStatus(target, false, throwable);
                    }
                })
                .doOnNext(new Action1<ShutdownStatus>() {
                    @Override
                    public void call(ShutdownStatus shutdownStatus) {
                        LOGGER.info(shutdownStatus.toString());
                    }
                });
    }

    @Override
    public Scheduler scheduler() {
        return coreScheduler;
    }

    @Override
    public boolean sslEnabled() {
        return sslEnabled;
    }

    @Override
    public boolean dcpEnabled() {
        return dcpEnabled;
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
    public int dcpConnectionBufferSize() {
        return dcpConnectionBufferSize;
    }

    @Override
    public double dcpConnectionBufferAckThreshold() {
        return dcpConnectionBufferAckThreshold;
    }

    @Override
    public int kvEndpoints() {
        return kvServiceEndpoints;
    }

    @Override
    public int viewEndpoints() {
        return viewServiceEndpoints;
    }

    @Override
    public int queryEndpoints() {
        return queryServiceEndpoints;
    }

    @Override
    public String userAgent() {
        return userAgent;
    }

    @Override
    public String packageNameAndVersion() {
        return packageNameAndVersion;
    }

    @Override
    public Delay observeIntervalDelay() {
        return observeIntervalDelay;
    }

    @Override
    public Delay reconnectDelay() {
        return reconnectDelay;
    }

    @Override
    public Delay retryDelay() {
        return retryDelay;
    }

    @Override
    public RetryStrategy retryStrategy() {
        return retryStrategy;
    }

    @Override
    public long maxRequestLifetime() {
        return maxRequestLifetime;
    }

    @Override
    public long keepAliveInterval() {
        return this.keepAliveInterval;
    }

    @Override
    public EventBus eventBus() {
        return eventBus;
    }

    @Override
    public long autoreleaseAfter() {
        return autoreleaseAfter;
    }

    @Override
    public boolean bufferPoolingEnabled() {
        return bufferPoolingEnabled;
    }

    @Override
    public boolean tcpNodelayEnabled() {
        return tcpNodelayEnabled;
    }

    @Override
    public boolean mutationTokensEnabled() {
        return mutationTokensEnabled;
    }

    @Override
    public MetricsCollector runtimeMetricsCollector() {
        return runtimeMetricsCollector;
    }

    @Override
    public NetworkLatencyMetricsCollector networkLatencyMetricsCollector() {
        return networkLatencyMetricsCollector;
    }

    @Override
    public int socketConnectTimeout() {
        return socketConnectTimeout;
    }

    public static class Builder {

        private boolean dcpEnabled = DCP_ENABLED;
        private boolean sslEnabled = SSL_ENABLED;
        private String sslKeystoreFile = SSL_KEYSTORE_FILE;
        private String sslKeystorePassword = SSL_KEYSTORE_PASSWORD;
        private String userAgent = USER_AGENT;
        private String packageNameAndVersion = PACKAGE_NAME_AND_VERSION;
        private boolean queryEnabled = QUERY_ENABLED;
        private int queryPort = QUERY_PORT;
        private boolean bootstrapHttpEnabled = BOOTSTRAP_HTTP_ENABLED;
        private boolean bootstrapCarrierEnabled = BOOTSTRAP_CARRIER_ENABLED;
        private int bootstrapHttpDirectPort = BOOTSTRAP_HTTP_DIRECT_PORT;
        private int bootstrapHttpSslPort = BOOTSTRAP_HTTP_SSL_PORT;
        private int bootstrapCarrierDirectPort = BOOTSTRAP_CARRIER_DIRECT_PORT;
        private int bootstrapCarrierSslPort = BOOTSTRAP_CARRIER_SSL_PORT;
        private int ioPoolSize = IO_POOL_SIZE;
        private int computationPoolSize = COMPUTATION_POOL_SIZE;
        private int responseBufferSize = RESPONSE_BUFFER_SIZE;
        private int requestBufferSize = REQUEST_BUFFER_SIZE;
        public int dcpConnectionBufferSize = DCP_CONNECTION_BUFFER_SIZE;
        public double dcpConnectionBufferAckThreshold = DCP_CONNECTION_BUFFER_ACK_THRESHOLD;
        private int kvEndpoints = KEYVALUE_ENDPOINTS;
        private int viewEndpoints = VIEW_ENDPOINTS;
        private int queryEndpoints = QUERY_ENDPOINTS;
        private Delay observeIntervalDelay = OBSERVE_INTERVAL_DELAY;
        private Delay reconnectDelay = RECONNECT_DELAY;
        private Delay retryDelay = RETRY_DELAY;
        private RetryStrategy retryStrategy = RETRY_STRATEGY;
        private EventLoopGroup ioPool;
        private ShutdownHook ioPoolShutdownHook;
        private Scheduler scheduler;
        private ShutdownHook schedulerShutdownHook;
        private EventBus eventBus;
        private long maxRequestLifetime = MAX_REQUEST_LIFETIME;
        private long keepAliveInterval = KEEPALIVEINTERVAL;
        private long autoreleaseAfter = AUTORELEASE_AFTER;
        private boolean bufferPoolingEnabled = BUFFER_POOLING_ENABLED;
        private boolean tcpNodelayEnabled = TCP_NODELAY_ENALED;
        private boolean mutationTokensEnabled = MUTATION_TOKENS_ENABLED;
        private int socketConnectTimeout = SOCKET_CONNECT_TIMEOUT;

        private MetricsCollectorConfig runtimeMetricsCollectorConfig = null;
        private LatencyMetricsCollectorConfig networkLatencyMetricsCollectorConfig = null;
        private LoggingConsumer defaultMetricsLoggingConsumer = LoggingConsumer.create();

        protected Builder() {
        }

        /**
         * Set if DCP should be enabled (only makes sense with server versions >= 3.0.0, default {@value #DCP_ENABLED}).
         */
        public Builder dcpEnabled(final boolean dcpEnabled) {
            this.dcpEnabled = dcpEnabled;
            return this;
        }

        /**
         * Set if SSL should be enabled (default value {@value #SSL_ENABLED}).
         * If true, also set {@link #sslKeystoreFile(String)} and {@link #sslKeystorePassword(String)}.
         */
        public Builder sslEnabled(final boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return this;
        }

        /**
         * Defines the location of the SSL Keystore file (default value null, none).
         */
        public Builder sslKeystoreFile(final String sslKeystoreFile) {
            this.sslKeystoreFile = sslKeystoreFile;
            return this;
        }

        /**
         * Sets the SSL Keystore password to be used with the Keystore file (default value null, none).
         * @see #sslKeystoreFile(String)
         */
        public Builder sslKeystorePassword(final String sslKeystorePassword) {
            this.sslKeystorePassword = sslKeystorePassword;
            return this;
        }

        /**
         * Toggles the N1QL Query feature (default value {@value #QUERY_ENABLED}).
         * This parameter will be deprecated once N1QL is in General Availability and shipped with the server.
         *
         * If not bundled with the server, the N1QL service must run on all the cluster's nodes.
         */
        public Builder queryEnabled(final boolean queryEnabled) {
            this.queryEnabled = queryEnabled;
            return this;
        }

        /**
         * Defines the port for N1QL Query (default value {@value #QUERY_PORT}).
         * This parameter will be deprecated once N1QL is in General Availability and shipped with the server.
         *
         * If not bundled with the server, the N1QL service must run on all the cluster's nodes.
         */
        public Builder queryPort(final int queryPort) {
            this.queryPort = queryPort;
            return this;
        }

        /**
         * Toggles bootstrap via Http (default value {@value #BOOTSTRAP_HTTP_ENABLED}).
         */
        public Builder bootstrapHttpEnabled(final boolean bootstrapHttpEnabled) {
            this.bootstrapHttpEnabled = bootstrapHttpEnabled;
            return this;
        }

        /**
         * Toggles bootstrap via carrier publication (default value {@value #BOOTSTRAP_CARRIER_ENABLED}).
         */
        public Builder bootstrapCarrierEnabled(final boolean bootstrapCarrierEnabled) {
            this.bootstrapCarrierEnabled = bootstrapCarrierEnabled;
            return this;
        }

        /**
         * If Http bootstrap is enabled and not SSL, sets the port to use
         * (default value {@value #BOOTSTRAP_HTTP_DIRECT_PORT}).
         */
        public Builder bootstrapHttpDirectPort(final int bootstrapHttpDirectPort) {
            this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
            return this;
        }

        /**
         * If Http bootstrap and SSL are enabled, sets the port to use
         * (default value {@value #BOOTSTRAP_HTTP_SSL_PORT}).
         */
        public Builder bootstrapHttpSslPort(final int bootstrapHttpSslPort) {
            this.bootstrapHttpSslPort = bootstrapHttpSslPort;
            return this;
        }

        /**
         * If carrier publication bootstrap is enabled and not SSL, sets the port to use
         * (default value {@value #BOOTSTRAP_CARRIER_DIRECT_PORT}).
         */
        public Builder bootstrapCarrierDirectPort(final int bootstrapCarrierDirectPort) {
            this.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
            return this;
        }

        /**
         * If carrier publication bootstrap and SSL are enabled, sets the port to use
         * (default value {@value #BOOTSTRAP_CARRIER_SSL_PORT}).
         */
        public Builder bootstrapCarrierSslPort(final int bootstrapCarrierSslPort) {
            this.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
            return this;
        }

        /**
         * Sets the pool size (number of threads to use) for I/O
         * operations (default value is the number of CPUs).
         *
         * If there is more nodes in the cluster than the defined
         * ioPoolSize, multiplexing will automatically happen.
         */
        public Builder ioPoolSize(final int ioPoolSize) {
            this.ioPoolSize = ioPoolSize;
            return this;
        }

        /**
         * Sets the pool size (number of threads to use) for all non blocking operations in the core and clients
         * (default value is the number of CPUs).
         *
         * Don't size it too small since it would significantly impact performance.
         */
        public Builder computationPoolSize(final int computationPoolSize) {
            this.computationPoolSize = computationPoolSize;
            return this;
        }

        /**
         * Sets the size of the RingBuffer structure that queues requests (default value {@value #REQUEST_BUFFER_SIZE}).
         * This is an advanced parameter that usually shouldn't need to be changed.
         */
        public Builder requestBufferSize(final int requestBufferSize) {
            this.requestBufferSize = requestBufferSize;
            return this;
        }

        /**
         * Sets the size of the RingBuffer structure that queues responses
         * (default value {@value #RESPONSE_BUFFER_SIZE}).
         * This is an advanced parameter that usually shouldn't need to be changed
         */
        public Builder responseBufferSize(final int responseBufferSize) {
            this.responseBufferSize = responseBufferSize;
            return this;
        }

        /**
         * Sets the size of the buffer to control speed of DCP producer. The server will stop emitting data if
         * the current value of the buffer reach this limit. Set it to zero to disable DCP flow control.
         * (default value {@value #DCP_CONNECTION_BUFFER_SIZE}).
         */
        public Builder dcpConnectionBufferSize(final int dcpConnectionBufferSize) {
            this.dcpConnectionBufferSize = dcpConnectionBufferSize;
            return this;
        }

        /**
         * When a DCP connection read bytes reaches this percentage of the {@link #dcpConnectionBufferSize},
         * a DCP Buffer Acknowledge message is sent to the server to signal producer how much data has been processed.
         * (default value {@value #DCP_CONNECTION_BUFFER_ACK_THRESHOLD}).
         */
        public Builder dcpConnectionBufferAckThreshold(final int dcpConnectionBufferAckThreshold) {
            this.dcpConnectionBufferAckThreshold = dcpConnectionBufferAckThreshold;
            return this;
        }

        /**
         * Sets the number of Key/Value endpoints to open per nodes in the cluster
         * (default value {@value #KEYVALUE_ENDPOINTS}).
         *
         * Only tune to more if IO has been identified as the most probable bottleneck,
         * since it can reduce batching on the tcp/network level.
         */
        public Builder kvEndpoints(final int kvEndpoints) {
            this.kvEndpoints = kvEndpoints;
            return this;
        }

        /**
         * Sets the number of View endpoints to open per node in the cluster (default value {@value #VIEW_ENDPOINTS}).
         *
         * Setting this to a higher number is advised in heavy view workloads.
         */
        public Builder viewEndpoints(final int viewEndpoints) {
            this.viewEndpoints = viewEndpoints;
            return this;
        }

        /**
         * Sets the number of Query (N1QL) endpoints to open per node in the cluster
         * (default value {@value #QUERY_ENDPOINTS}).
         *
         * Setting this to a higher number is advised in heavy query workloads.
         */
        public Builder queryEndpoints(final int queryEndpoints) {
            this.queryEndpoints = queryEndpoints;
            return this;
        }

        /**
         * Sets the USER-AGENT String to be sent in HTTP requests headers (should usually not be tweaked,
         * default value is computed from the SDK {@link #packageNameAndVersion()}).
         */
        public Builder userAgent(final String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        /**
         * Sets the String to be used as identifier for the library namespace and version.
         * (should usually not be tweaked, default value is computed at build time from VCS tags/commits).
         *
         * This is used in {@link #userAgent()} notably.
         */
        public Builder packageNameAndVersion(final String packageNameAndVersion) {
            this.packageNameAndVersion = packageNameAndVersion;
            return this;
        }

        /**
         * Sets the {@link Delay} for {@link Observe} poll operations (default value
         * is a delay growing exponentially between 10us and 100ms).
         */
        public Builder observeIntervalDelay(final Delay observeIntervalDelay) {
            this.observeIntervalDelay = observeIntervalDelay;
            return this;
        }

        /**
         * Sets the {@link Delay} for node reconnects (default value is a delay growing exponentially
         * between 32ms and 4096ms).
         */
        public Builder reconnectDelay(final Delay reconnectDelay) {
            this.reconnectDelay = reconnectDelay;
            return this;
        }

        /**
         * Sets the {@link Delay} for retries of requests (default value is a delay growing exponentially
         * between 100us and 100ms).
         */
        public Builder retryDelay(final Delay retryDelay) {
            this.retryDelay = retryDelay;
            return this;
        }

        /**
         * Sets the I/O Pool implementation for the underlying IO framework.
         * This is an advanced configuration that should only be used if you know what you are doing.
         *
         * @deprecated use {@link #ioPool(EventLoopGroup, ShutdownHook)} to also provide a shutdown hook.
         */
        @Deprecated
        public Builder ioPool(final EventLoopGroup group) {
            return ioPool(group, new NoOpShutdownHook());
        }

        /**
         * Sets the I/O Pool implementation for the underlying IO framework, along with the action
         * to execute when this environment is shut down.
         * This is an advanced configuration that should only be used if you know what you are doing.
         */
        public Builder ioPool(final EventLoopGroup group, final ShutdownHook shutdownHook) {
            this.ioPool = group;
            this.ioPoolShutdownHook = shutdownHook;
            return this;
        }

        /**
         * Sets the Scheduler implementation for the underlying computation framework.
         * This is an advanced configuration that should only be used if you know what you are doing.
         *
         * @deprecated use {@link #ioPool(EventLoopGroup, ShutdownHook)} to also provide a shutdown hook.
         */
        @Deprecated
        public Builder scheduler(final Scheduler scheduler) {
            return scheduler(scheduler, new NoOpShutdownHook());
        }

        /**
         * Sets the Scheduler implementation for the underlying computation framework, along with the action
         * to execute when this environment is shut down.
         * This is an advanced configuration that should only be used if you know what you are doing.
         */
        public Builder scheduler(final Scheduler scheduler, final ShutdownHook shutdownHook) {
            this.scheduler = scheduler;
            this.schedulerShutdownHook = shutdownHook;
            return this;
        }

        /**
         * Sets the {@link RetryStrategy} to be used during request retries
         * (default value is a {@link BestEffortRetryStrategy}).
         */
        public Builder retryStrategy(final RetryStrategy retryStrategy) {
            this.retryStrategy = retryStrategy;
            return this;
        }

        /**
         * Sets the maximum time in milliseconds a request is allowed to live.
         *
         * If the best effort retry strategy is used, the request will still be cancelled after this
         * period to make sure that requests are not sticking around forever. Make sure it is longer than any
         * timeout you potentially have configured.
         *
         * Default is 75s.
         */
        public Builder maxRequestLifetime(final long maxRequestLifetime) {
            this.maxRequestLifetime = maxRequestLifetime;
            return this;
        }

        /**
         * Sets the time of inactivity, in milliseconds, after which some services
         * will issue a form of keep-alive request to their corresponding server/nodes
         * (default is 30s, values <= 0 deactivate the idle check).
         */
        public Builder keepAliveInterval(long keepAliveIntervalMilliseconds) {
            this.keepAliveInterval = keepAliveIntervalMilliseconds;
            return this;
        }

        /**
         * Sets the time after which any non-consumed buffers will be automatically released.
         * Setting this to a higher value than a few seconds is not recommended since this
         * may lead to increased garbage collection.
         */
        public Builder autoreleaseAfter(long autoreleaseAfter) {
            this.autoreleaseAfter = autoreleaseAfter;
            return this;
        }

        /**
         * Sets the event bus to an alternative implementation.
         *
         * This setting should only be tweaked in advanced cases.
         */
        public Builder eventBus(final EventBus eventBus) {
            this.eventBus = eventBus;
            return this;
        }

        /**
         * Forcefully disable buffer pooling by setting the value to false.
         *
         * This should not be used in general because buffer pooling is in place to reduce GC
         * pressure during workloads. It is implemented to be used as a "last resort" if the
         * client is suspect to a buffer leak which can terminate the application. Until a
         * solution is found to the leak buffer pooling can be disabled at the cost of higher
         * GC.
         */
        public Builder bufferPoolingEnabled(boolean bufferPoolingEnabled) {
            this.bufferPoolingEnabled = bufferPoolingEnabled;
            return this;
        }

        /**
         * If TCP_NODELAY is manually disabled, Nagle'ing will take effect on both the client
         * and (if supported) the server side.
         */
        public Builder tcpNodelayEnabled(boolean tcpNodelayEnabled) {
            this.tcpNodelayEnabled = tcpNodelayEnabled;
            return this;
        }

        /**
         * If mutation tokens are enabled, they can be used for advanced durability requirements,
         * as well as optimized RYOW consistency.
         *
         * Note that just enabling it here won't help if the server does not support it as well. Use at
         * least Couchbase Server 4.0. Also, consider the additional overhead of 16 bytes per mutation response
         * (8 byte for the vbucket uuid and 8 byte for the sequence number).
         */
        public Builder mutationTokensEnabled(boolean mutationTokensEnabled) {
            this.mutationTokensEnabled = mutationTokensEnabled;
            return this;
        }

        /**
         * Sets a custom configuration for the {@link RuntimeMetricsCollector}.
         *
         * @param metricsCollectorConfig the custom configuration
         */
        public Builder runtimeMetricsCollectorConfig(MetricsCollectorConfig metricsCollectorConfig) {
            this.runtimeMetricsCollectorConfig = metricsCollectorConfig;
            return this;
        }

        /**
         * Sets a custom configuration for the {@link NetworkLatencyMetricsCollector}.
         *
         * @param metricsCollectorConfig the custom configuration for the collector.
         */
        public Builder networkLatencyMetricsCollectorConfig(LatencyMetricsCollectorConfig metricsCollectorConfig) {
            this.networkLatencyMetricsCollectorConfig = metricsCollectorConfig;
            return this;
        }

        public Builder defaultMetricsLoggingConsumer(boolean enabled, CouchbaseLogLevel level, LoggingConsumer.OutputFormat format) {
            if (enabled) {
                defaultMetricsLoggingConsumer = LoggingConsumer.create(level, format);
            } else {
                defaultMetricsLoggingConsumer = null;
            }
            return this;
        }

        public Builder defaultMetricsLoggingConsumer(boolean enabled, CouchbaseLogLevel level) {
            return defaultMetricsLoggingConsumer(enabled, level, LoggingConsumer.DEFAULT_FORMAT);
        }

        /**
         * Sets a custom socket connect timeout.
         *
         * @param socketConnectTimeout the socket connect timeout in milliseconds.
         */
        public Builder socketConnectTimeout(int socketConnectTimeout) {
            this.socketConnectTimeout = socketConnectTimeout;
            return this;
        }

        public DefaultCoreEnvironment build() {
            return new DefaultCoreEnvironment(this);
        }
    }

    /**
     * Dumps the environment parameters known to this implementation into a {@link StringBuilder},
     * which is returned for method chaining.
     *
     * @param sb the StringBuilder in which to dump parameters.
     * @return the same StringBuilder for method chaining.
     */
    protected StringBuilder dumpParameters(StringBuilder sb) {
        sb.append("sslEnabled=").append(sslEnabled);
        sb.append(", sslKeystoreFile='").append(sslKeystoreFile).append('\'');
        sb.append(", sslKeystorePassword='").append(sslKeystorePassword).append('\'');
        sb.append(", queryEnabled=").append(queryEnabled);
        sb.append(", queryPort=").append(queryPort);
        sb.append(", bootstrapHttpEnabled=").append(bootstrapHttpEnabled);
        sb.append(", bootstrapCarrierEnabled=").append(bootstrapCarrierEnabled);
        sb.append(", bootstrapHttpDirectPort=").append(bootstrapHttpDirectPort);
        sb.append(", bootstrapHttpSslPort=").append(bootstrapHttpSslPort);
        sb.append(", bootstrapCarrierDirectPort=").append(bootstrapCarrierDirectPort);
        sb.append(", bootstrapCarrierSslPort=").append(bootstrapCarrierSslPort);
        sb.append(", ioPoolSize=").append(ioPoolSize);
        sb.append(", computationPoolSize=").append(computationPoolSize);
        sb.append(", responseBufferSize=").append(responseBufferSize);
        sb.append(", requestBufferSize=").append(requestBufferSize);
        sb.append(", kvServiceEndpoints=").append(kvServiceEndpoints);
        sb.append(", viewServiceEndpoints=").append(viewServiceEndpoints);
        sb.append(", queryServiceEndpoints=").append(queryServiceEndpoints);
        sb.append(", ioPool=").append(ioPool.getClass().getSimpleName());
        if (ioPoolShutdownHook == null || ioPoolShutdownHook instanceof  NoOpShutdownHook) {
            sb.append("!unmanaged");
        }
        sb.append(", coreScheduler=").append(coreScheduler.getClass().getSimpleName());
        if (coreSchedulerShutdownHook == null || coreSchedulerShutdownHook instanceof NoOpShutdownHook) {
            sb.append("!unmanaged");
        }
        sb.append(", eventBus=").append(eventBus.getClass().getSimpleName());
        sb.append(", packageNameAndVersion=").append(packageNameAndVersion);
        sb.append(", dcpEnabled=").append(dcpEnabled);
        sb.append(", retryStrategy=").append(retryStrategy);
        sb.append(", maxRequestLifetime=").append(maxRequestLifetime);
        sb.append(", retryDelay=").append(retryDelay);
        sb.append(", reconnectDelay=").append(reconnectDelay);
        sb.append(", observeIntervalDelay=").append(observeIntervalDelay);
        sb.append(", keepAliveInterval=").append(keepAliveInterval);
        sb.append(", autoreleaseAfter=").append(autoreleaseAfter);
        sb.append(", bufferPoolingEnabled=").append(bufferPoolingEnabled);
        sb.append(", tcpNodelayEnabled=").append(tcpNodelayEnabled);
        sb.append(", mutationTokensEnabled=").append(mutationTokensEnabled);
        sb.append(", socketConnectTimeout=").append(socketConnectTimeout);
        sb.append(", dcpConnectionBufferSize=").append(dcpConnectionBufferSize);
        sb.append(", dcpConnectionBufferAckThreshold=").append(dcpConnectionBufferAckThreshold);

        return sb;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CoreEnvironment: {");
        dumpParameters(sb).append('}');
        return sb.toString();
    }

    /**
     * An internal class used to keep track of components being shut down and
     * the result of their shutdown call / cause for failures.
     */
    private static final class ShutdownStatus {
        public final String target;
        public final boolean success;
        public final Throwable cause;

        public ShutdownStatus(String target, boolean success, Throwable cause) {
            this.target = target;
            this.success = success;
            this.cause = cause;
        }

        @Override
        public String toString() {
            return "Shutdown " + target + ": " + (success ? "success " : "failure ") + (cause == null ? "" : " due to "
                    + cause.toString());
        }
    }
}
