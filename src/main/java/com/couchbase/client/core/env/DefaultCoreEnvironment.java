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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.env.resources.IoPoolShutdownHook;
import com.couchbase.client.core.env.resources.NettyShutdownHook;
import com.couchbase.client.core.env.resources.NoOpShutdownHook;
import com.couchbase.client.core.env.resources.ShutdownHook;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.DefaultEventBus;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.event.consumers.LoggingConsumer;
import com.couchbase.client.core.event.system.TooManyEnvironmentsEvent;
import com.couchbase.client.core.hooks.CouchbaseCoreSendHook;
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
import com.couchbase.client.core.node.DefaultMemcachedHashingStrategy;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.core.tracing.DefaultOrphanResponseReporter;
import com.couchbase.client.core.tracing.ThresholdLogTracer;
import com.couchbase.client.core.tracing.OrphanResponseReporter;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import com.couchbase.client.core.utils.Blocking;

import java.security.KeyStore;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultCoreEnvironment implements CoreEnvironment {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CoreEnvironment.class);

    public static final boolean SSL_ENABLED = false;
    public static final String SSL_KEYSTORE_FILE = null;
    public static final String SSL_TRUSTSTORE_FILE = null;
    public static final String SSL_KEYSTORE_PASSWORD = null;
    public static final String SSL_TRUSTSTORE_PASSWORD = null;
    public static final KeyStore SSL_KEYSTORE = null;
    public static final KeyStore SSL_TRUSTSTORE = null;
    public static final boolean BOOTSTRAP_HTTP_ENABLED = true;
    public static final boolean BOOTSTRAP_CARRIER_ENABLED = true;
    public static final int BOOTSTRAP_HTTP_DIRECT_PORT = 8091;
    public static final int BOOTSTRAP_HTTP_SSL_PORT = 18091;
    public static final int BOOTSTRAP_CARRIER_DIRECT_PORT = 11210;
    public static final int BOOTSTRAP_CARRIER_SSL_PORT = 11207;
    public static final int REQUEST_BUFFER_SIZE = 16384;
    public static final int RESPONSE_BUFFER_SIZE = 16384;
    public static final int IO_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    public static final int COMPUTATION_POOL_SIZE =  Runtime.getRuntime().availableProcessors();
    public static final int KEYVALUE_ENDPOINTS = 1;
    public static final int VIEW_ENDPOINTS = 12;
    public static final int QUERY_ENDPOINTS = 12;
    public static final int SEARCH_ENDPOINTS = 12;
    public static final int ANALYTICS_ENDPOINTS = 12;
    public static final Delay OBSERVE_INTERVAL_DELAY = Delay.exponential(TimeUnit.MICROSECONDS, 100000, 10);
    public static final Delay RECONNECT_DELAY = Delay.exponential(TimeUnit.MILLISECONDS, 4096, 32);
    public static final Delay RETRY_DELAY = Delay.exponential(TimeUnit.MICROSECONDS, 100000, 100);
    public static final RetryStrategy RETRY_STRATEGY = BestEffortRetryStrategy.INSTANCE;
    public static final long MAX_REQUEST_LIFETIME = TimeUnit.SECONDS.toMillis(75);
    public static final long KEEPALIVEINTERVAL = TimeUnit.SECONDS.toMillis(30);
    public static final boolean CONTINUOUS_KEEPALIVE_ENABLED = true;
    public static final long KEEPALIVE_ERROR_THRESHOLD = 4;
    public static final long KEEPALIVE_TIMEOUT = 2500;
    public static final long AUTORELEASE_AFTER = TimeUnit.SECONDS.toMillis(2);
    public static final boolean BUFFER_POOLING_ENABLED = true;
    public static final boolean TCP_NODELAY_ENALED = true;
    public static final boolean MUTATION_TOKENS_ENABLED = false;
    public static final int SOCKET_CONNECT_TIMEOUT = 1000;
    public static final boolean CALLBACKS_ON_IO_POOL = false;
    public static final long DISCONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(25);
    public static final MemcachedHashingStrategy MEMCACHED_HASHING_STRATEGY =
        DefaultMemcachedHashingStrategy.INSTANCE;
    public static final long CONFIG_POLL_INTERVAL = 2500;
    public static final long CONFIG_POLL_FLOOR_INTERVAL = 50;
    public static final boolean CERT_AUTH_ENABLED = false;
    public static final boolean FORCE_SASL_PLAIN = false;
    public static final boolean OPERATION_TRACING_ENABLED = true;
    public static final boolean OPERATION_TRACING_SERVER_DUR_ENABLED = true;
    public static final int MIN_COMPRESSION_SIZE = 32;
    public static final double MIN_COMPRESSION_RATIO = 0.83;
    public static final boolean COMPRESSION_ENABLED = true;
    public static final boolean ORPHAN_REPORTING_ENABLED = true;
    public static final NetworkResolution NETWORK_RESOLUTION = NetworkResolution.AUTO;

    public static String CORE_VERSION;
    public static String CORE_GIT_VERSION;
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
                LOGGER.info("Could not retrieve core version properties, defaulting.", e);
            }

            CORE_VERSION = version == null ? "unknown" : version;
            CORE_GIT_VERSION = gitVersion == null ? "unknown" : gitVersion;
            PACKAGE_NAME_AND_VERSION = String.format("couchbase-jvm-core/%s (git: %s)",
                CORE_VERSION, CORE_GIT_VERSION);

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

        try {
            if (System.getProperty("com.couchbase.client.deps.io.netty.packagePrefix") == null) {
                System.setProperty("com.couchbase.client.deps.io.netty.packagePrefix", "com.couchbase.client.deps.");
            }
        } catch (Exception ex) {
            LOGGER.warn("Could not configure bundled netty's package prefix", ex);
        }
    }

    private final boolean sslEnabled;
    private final String sslKeystoreFile;
    private final String sslTruststoreFile;
    private final String sslKeystorePassword;
    private final String sslTruststorePassword;
    private final KeyStore sslKeystore;
    private final KeyStore sslTruststore;
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
    private final int kvServiceEndpoints;
    private final int viewServiceEndpoints;
    private final int queryServiceEndpoints;
    private final int searchServiceEndpoints;
    private final Delay observeIntervalDelay;
    private final Delay reconnectDelay;
    private final Delay retryDelay;
    private final String userAgent;
    private final String packageNameAndVersion;
    private final RetryStrategy retryStrategy;
    private final long maxRequestLifetime;
    private final long keepAliveInterval;
    private final boolean continuousKeepAliveEnabled;
    private final long keepAliveErrorThreshold;
    private final long keepAliveTimeout;
    private final long autoreleaseAfter;
    private final boolean bufferPoolingEnabled;
    private final boolean tcpNodelayEnabled;
    private final boolean mutationTokensEnabled;
    private final int socketConnectTimeout;
    private final boolean callbacksOnIoPool;
    private final long disconnectTimeout;
    private final WaitStrategyFactory requestBufferWaitStrategy;
    private final MemcachedHashingStrategy memcachedHashingStrategy;
    private final long configPollInterval;
    private final long configPollFloorInterval;
    private final boolean certAuthEnabled;
    private final boolean operationTracingEnabled;
    private final boolean operationTracingServerDurationEnabled;
    private final Tracer tracer;
    private final int minCompressionSize;
    private final double minCompressionRatio;
    private final boolean compressionEnabled;
    private final NetworkResolution networkResolution;

    private static volatile int MAX_ALLOWED_INSTANCES = 1;
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();

    private final EventLoopGroup ioPool;
    private final EventLoopGroup kvIoPool;
    private final EventLoopGroup queryIoPool;
    private final EventLoopGroup viewIoPool;
    private final EventLoopGroup searchIoPool;
    private final EventLoopGroup analyticsIoPool;
    private final Scheduler coreScheduler;
    private final EventBus eventBus;

    private final ShutdownHook ioPoolShutdownHook;
    private final ShutdownHook kvIoPoolShutdownHook;
    private final ShutdownHook queryIoPoolShutdownHook;
    private final ShutdownHook viewIoPoolShutdownHook;
    private final ShutdownHook searchIoPoolShutdownHook;
    private final ShutdownHook analyticsIoPoolShutdownHook;
    private final ShutdownHook tracerShutdownHook;
    private final ShutdownHook orphanReporterShutdownHook;

    private final KeyValueServiceConfig keyValueServiceConfig;
    private final QueryServiceConfig queryServiceConfig;
    private final ViewServiceConfig viewServiceConfig;
    private final SearchServiceConfig searchServiceConfig;
    private final AnalyticsServiceConfig analyticsServiceConfig;

    private final ShutdownHook nettyShutdownHook;
    private final ShutdownHook coreSchedulerShutdownHook;

    private final MetricsCollector runtimeMetricsCollector;
    private final NetworkLatencyMetricsCollector networkLatencyMetricsCollector;
    private final Subscription metricsCollectorSubscription;

    private final boolean forceSaslPlain;

    private final CouchbaseCoreSendHook couchbaseCoreSendHook;

    private final boolean orphanResponseReportingEnabled;
    private final OrphanResponseReporter orphanResponseReporter;

    protected DefaultCoreEnvironment(final Builder builder) {
        boolean emitEnvWarnMessage = false;
        if (INSTANCE_COUNT.incrementAndGet() > MAX_ALLOWED_INSTANCES) {
            LOGGER.warn("More than " + MAX_ALLOWED_INSTANCES + " Couchbase Environments found (" + INSTANCE_COUNT.get()
                + "), this can have severe impact on performance and stability. Reuse environments!");
            emitEnvWarnMessage = true;
        }

        sslEnabled = booleanPropertyOr("sslEnabled", builder.sslEnabled);
        sslKeystoreFile = stringPropertyOr("sslKeystoreFile", builder.sslKeystoreFile);
        sslTruststoreFile = stringPropertyOr("sslTruststoreFile", builder.sslTruststoreFile);
        sslKeystorePassword = stringPropertyOr("sslKeystorePassword", builder.sslKeystorePassword);
        sslTruststorePassword = stringPropertyOr("sslTruststorePassword", builder.sslTruststorePassword);
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
        kvServiceEndpoints = intPropertyOr("kvEndpoints", builder.kvEndpoints);
        viewServiceEndpoints = intPropertyOr("viewEndpoints", builder.viewEndpoints);
        queryServiceEndpoints = intPropertyOr("queryEndpoints", builder.queryEndpoints);
        searchServiceEndpoints = intPropertyOr("searchEndpoints", builder.searchEndpoints);
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
        callbacksOnIoPool = booleanPropertyOr("callbacksOnIoPool", builder.callbacksOnIoPool);
        disconnectTimeout = longPropertyOr("disconnectTimeout", builder.disconnectTimeout);
        sslKeystore = builder.sslKeystore;
        sslTruststore = builder.sslTruststore;
        memcachedHashingStrategy = builder.memcachedHashingStrategy;
        configPollInterval = longPropertyOr("configPollInterval", builder.configPollInterval);
        configPollFloorInterval = longPropertyOr("configPollFloorInterval", builder.configPollFloorInterval);
        certAuthEnabled = booleanPropertyOr("certAuthEnabled", builder.certAuthEnabled);
        forceSaslPlain = booleanPropertyOr("forceSaslPlain", builder.forceSaslPlain);
        continuousKeepAliveEnabled = booleanPropertyOr(
            "continuousKeepAliveEnabled",
                builder.continuousKeepAliveEnabled
        );
        keepAliveErrorThreshold = longPropertyOr("keepAliveErrorThreshold", builder.keepAliveErrorThreshold);
        keepAliveTimeout = longPropertyOr("keepAliveTimeout", builder.keepAliveTimeout);
        operationTracingEnabled = booleanPropertyOr("operationTracingEnabled", builder.operationTracingEnabled);
        orphanResponseReportingEnabled = booleanPropertyOr("orphanResponseReportingEnabled", builder.orphanResponseReportingEnabled);
        operationTracingServerDurationEnabled = booleanPropertyOr("operationTracingServerDurationEnabled", builder.operationTracingServerDurationEnabled);
        minCompressionRatio = doublePropertyOr("compressionMinRatio", builder.minCompressionRatio);
        minCompressionSize = intPropertyOr("compressionMinSize", builder.minCompressionSize);
        compressionEnabled = booleanPropertyOr("compressionEnabled", builder.compressionEnabled);

        if (!operationTracingEnabled) {
            tracer = NoopTracerFactory.create();
            tracerShutdownHook = new NoOpShutdownHook();
        } else if (builder.tracer == null) {
            tracer = ThresholdLogTracer.create();
            tracerShutdownHook = new ShutdownHook() {
                private volatile boolean shutdown = false;
                @Override
                public Observable<Boolean> shutdown() {
                    return Observable.fromCallable(new Callable<Boolean>() {
                        @Override
                        public Boolean call() {
                            ((ThresholdLogTracer) tracer).shutdown();
                            shutdown = true;
                            return true;
                        }
                    });
                }

                @Override
                public boolean isShutdown() {
                    return shutdown;
                }
            };
        } else {
            tracer = builder.tracer;
            tracerShutdownHook = new NoOpShutdownHook();
        }

        if (!orphanResponseReportingEnabled) {
            orphanResponseReporter = null;
            orphanReporterShutdownHook = new NoOpShutdownHook();
        } else if (builder.orphanResponseReporter == null){
            orphanResponseReporter = DefaultOrphanResponseReporter.create();
            orphanReporterShutdownHook = new ShutdownHook() {
                private volatile boolean shutdown = false;

                @Override
                public Observable<Boolean> shutdown() {
                    return Observable.fromCallable(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            orphanResponseReporter.shutdown();
                            shutdown = true;
                            return true;
                        }
                    });
                }

                @Override
                public boolean isShutdown() {
                    return shutdown;
                }
            };
        } else {
            orphanResponseReporter = builder.orphanResponseReporter;
            orphanReporterShutdownHook = new NoOpShutdownHook();
        }

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

        if (certAuthEnabled && !sslEnabled) {
            throw new IllegalStateException("Client Certificate Authentication enabled, but SSL is not - " +
                "please configure encryption properly.");
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

        if (builder.kvIoPool != null) {
            this.kvIoPool = builder.kvIoPool;
            this.kvIoPoolShutdownHook = builder.kvIoPoolShutdownHook == null
                    ? new NoOpShutdownHook()
                    : builder.kvIoPoolShutdownHook;
        } else {
            this.kvIoPool = null;
            this.kvIoPoolShutdownHook = new NoOpShutdownHook();
        }
        if (builder.queryIoPool != null) {
            this.queryIoPool = builder.queryIoPool;
            this.queryIoPoolShutdownHook = builder.queryIoPoolShutdownHook == null
                ? new NoOpShutdownHook()
                : builder.queryIoPoolShutdownHook;
        } else {
            this.queryIoPool = null;
            this.queryIoPoolShutdownHook = new NoOpShutdownHook();
        }
        if (builder.viewIoPool != null) {
            this.viewIoPool = builder.viewIoPool;
            this.viewIoPoolShutdownHook = builder.viewIoPoolShutdownHook == null
                ? new NoOpShutdownHook()
                : builder.viewIoPoolShutdownHook;
        } else {
            this.viewIoPool = null;
            this.viewIoPoolShutdownHook = new NoOpShutdownHook();
        }
        if (builder.searchIoPool != null) {
            this.searchIoPool = builder.searchIoPool;
            this.searchIoPoolShutdownHook = builder.searchIoPoolShutdownHook == null
                ? new NoOpShutdownHook()
                : builder.searchIoPoolShutdownHook;
        } else {
            this.searchIoPool = null;
            this.searchIoPoolShutdownHook = new NoOpShutdownHook();
        }
        if (builder.analyticsIoPool != null) {
            this.analyticsIoPool = builder.analyticsIoPool;
            this.analyticsIoPoolShutdownHook = builder.analyticsIoPoolShutdownHook == null
                ? new NoOpShutdownHook()
                : builder.analyticsIoPoolShutdownHook;
        } else {
            this.analyticsIoPool = null;
            this.analyticsIoPoolShutdownHook = new NoOpShutdownHook();
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
                ? DefaultLatencyMetricsCollectorConfig.disabled()
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

        if (builder.requestBufferWaitStrategy == null) {
            requestBufferWaitStrategy = new WaitStrategyFactory() {
                @Override
                public WaitStrategy newWaitStrategy() {
                    return new BlockingWaitStrategy();
                }
            };
        } else {
            requestBufferWaitStrategy = builder.requestBufferWaitStrategy;
        }

        if (builder.keyValueServiceConfig != null) {
            this.keyValueServiceConfig = builder.keyValueServiceConfig;
        } else {
            this.keyValueServiceConfig = KeyValueServiceConfig.create(kvEndpoints());
        }

        if (builder.viewServiceConfig != null) {
            this.viewServiceConfig = builder.viewServiceConfig;
        } else {
            int minEndpoints = viewEndpoints() == VIEW_ENDPOINTS ? 0 : viewEndpoints();
            this.viewServiceConfig = ViewServiceConfig.create(minEndpoints, viewEndpoints());
        }

        if (builder.queryServiceConfig != null) {
            this.queryServiceConfig = builder.queryServiceConfig;
        } else {
            int minEndpoints = queryEndpoints() == QUERY_ENDPOINTS ? 0 : queryEndpoints();
            this.queryServiceConfig = QueryServiceConfig.create(minEndpoints, queryEndpoints());
        }

        if (builder.searchServiceConfig != null) {
            this.searchServiceConfig = builder.searchServiceConfig;
        } else {
            int minEndpoints = searchEndpoints() == SEARCH_ENDPOINTS ? 0 : searchEndpoints();
            this.searchServiceConfig = SearchServiceConfig.create(minEndpoints, searchEndpoints());
        }

        if (builder.analyticsServiceConfig != null) {
            this.analyticsServiceConfig = builder.analyticsServiceConfig;
        } else {
            this.analyticsServiceConfig = AnalyticsServiceConfig.create(0, ANALYTICS_ENDPOINTS);
        }

        if (emitEnvWarnMessage) {
            eventBus.publish(new TooManyEnvironmentsEvent(INSTANCE_COUNT.get()));
        }

        this.couchbaseCoreSendHook = builder.couchbaseCoreSendHook;

        if (configPollInterval < configPollFloorInterval) {
            throw new IllegalArgumentException("The config poll interval (" +
                configPollInterval + ") is lower than the config " +
                "poll floor interval (" + configPollFloorInterval +
                "), which means that some config polling will " +
                "be skipped. Please adjust both settings in a logical fashion.");
        }

        this.networkResolution = builder.networkResolution;
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
    public boolean shutdown() {
        return shutdown(disconnectTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean shutdown(long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(shutdownAsync(), timeout, timeUnit);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Observable<Boolean> shutdownAsync() {
        if (metricsCollectorSubscription != null && !metricsCollectorSubscription.isUnsubscribed()) {
            metricsCollectorSubscription.unsubscribe();
        }

        Observable<Boolean> result = Observable.merge(Observable.merge(
                wrapShutdown(ioPoolShutdownHook.shutdown(), "IoPool"),
                wrapBestEffortShutdown(nettyShutdownHook.shutdown(), "Netty"),
                wrapShutdown(kvIoPoolShutdownHook.shutdown(), "kvIoPool"),
                wrapShutdown(viewIoPoolShutdownHook.shutdown(), "viewIoPool"),
                wrapShutdown(queryIoPoolShutdownHook.shutdown(), "queryIoPool"),
                wrapShutdown(searchIoPoolShutdownHook.shutdown(), "searchIoPool"),
                wrapShutdown(coreSchedulerShutdownHook.shutdown(), "Core Scheduler"),
                wrapShutdown(Observable.just(runtimeMetricsCollector.shutdown()), "Runtime Metrics Collector"),
                wrapShutdown(Observable.just(networkLatencyMetricsCollector.shutdown()), "Latency Metrics Collector")),
                wrapShutdown(analyticsIoPoolShutdownHook.shutdown(), "analyticsIoPool"),
                wrapShutdown(tracerShutdownHook.shutdown(), "Tracer"),
                wrapShutdown(orphanReporterShutdownHook.shutdown(), "OrphanReporter"))
                .reduce(true,
                        new Func2<Boolean, ShutdownStatus, Boolean>() {
                            @Override
                            public Boolean call(Boolean previousStatus, ShutdownStatus currentStatus) {
                                return previousStatus && currentStatus.success;
                            }
                        })
                .doOnTerminate(new Action0() {
                    @Override
                    public void call() {
                        INSTANCE_COUNT.decrementAndGet();
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
    public String sslKeystoreFile() {
        return sslKeystoreFile;
    }

    @Override
    public String sslKeystorePassword() {
        return sslKeystorePassword;
    }

    @Override
    public KeyStore sslKeystore() {
        return sslKeystore;
    }

    @Override
    public String sslTruststoreFile() {
        return sslTruststoreFile;
    }

    @Override
    public KeyStore sslTruststore() {
        return sslTruststore;
    }

    @Override
    public String sslTruststorePassword() {
        return sslTruststorePassword;
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
    public int searchEndpoints() {
        return searchServiceEndpoints;
    }

    @Override
    public String coreVersion() {
        return CORE_VERSION;
    }

    @Override
    public String coreBuild() {
        return CORE_GIT_VERSION;
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

    @Override
    public boolean callbacksOnIoPool() {
        return callbacksOnIoPool;
    }

    @Override
    public long disconnectTimeout() {
        return disconnectTimeout;
    }

    @Override
    public WaitStrategyFactory requestBufferWaitStrategy() {
        return requestBufferWaitStrategy;
    }

    @Override
    public MemcachedHashingStrategy memcachedHashingStrategy() {
        return memcachedHashingStrategy;
    }

    @Override
    public EventLoopGroup kvIoPool() {
        return kvIoPool;
    }

    @Override
    public EventLoopGroup viewIoPool() {
        return viewIoPool;
    }

    @Override
    public EventLoopGroup queryIoPool() {
        return queryIoPool;
    }

    @Override
    public EventLoopGroup searchIoPool() {
        return searchIoPool;
    }

    @Override
    public EventLoopGroup analyticsIoPool() {
        return analyticsIoPool;
    }

    public static int instanceCounter() {
        return INSTANCE_COUNT.get();
    }

    @Override
    public KeyValueServiceConfig kvServiceConfig() {
        return keyValueServiceConfig;
    }

    @Override
    public QueryServiceConfig queryServiceConfig() {
        return queryServiceConfig;
    }

    @Override
    public ViewServiceConfig viewServiceConfig() {
        return viewServiceConfig;
    }

    @Override
    public SearchServiceConfig searchServiceConfig() {
        return searchServiceConfig;
    }

    @Override
    public AnalyticsServiceConfig analyticsServiceConfig() {
        return analyticsServiceConfig;
    }

    @InterfaceStability.Committed
    @InterfaceAudience.Public
    @Override
    public long configPollInterval() {
        return configPollInterval;
    }

    @InterfaceStability.Committed
    @InterfaceAudience.Public
    @Override
    public long configPollFloorInterval() {
        return configPollFloorInterval;
    }

    @Override
    public boolean certAuthEnabled() {
        return certAuthEnabled;
    }

    @Override
    public boolean continuousKeepAliveEnabled() {
        return continuousKeepAliveEnabled;
    }

    @Override
    public long keepAliveErrorThreshold() {
        return keepAliveErrorThreshold;
    }

    @Override
    public long keepAliveTimeout() {
        return keepAliveTimeout;
    }

    @Override
    public CouchbaseCoreSendHook couchbaseCoreSendHook() {
        return couchbaseCoreSendHook;
    }

    @Override
    public boolean forceSaslPlain() {
        return forceSaslPlain;
    }

    @Override
    public boolean operationTracingEnabled() {
        return operationTracingEnabled;
    }

    @Override
    public boolean operationTracingServerDurationEnabled() {
        return operationTracingServerDurationEnabled;
    }

    @Override
    public Tracer tracer() {
        return tracer;
    }

    @Override
    public int compressionMinSize() {
        return minCompressionSize;
    }

    @Override
    public double compressionMinRatio() {
        return minCompressionRatio;
    }

    @Override
    public boolean compressionEnabled() {
        return compressionEnabled;
    }

    @Override
    public boolean orphanResponseReportingEnabled() { return orphanResponseReportingEnabled; }

    @Override
    public OrphanResponseReporter orphanResponseReporter() { return orphanResponseReporter; }

    @Override
    public NetworkResolution networkResolution() {
        return networkResolution;
    }

    /**
     * Returns the number of maximal allowed instances before warning log will be
     * emitted.
     */
    public static int maxAllowedInstances() {
        return MAX_ALLOWED_INSTANCES;
    }

    /**
     * Allows to set the maximum allowed instances to higher than 1 to silence
     * warning logs in environments where it can be disregarded.
     *
     * @param max the number of envs to allow.
     */
    public static void maxAllowedInstances(int max) {
        MAX_ALLOWED_INSTANCES = max;
    }

    public static class Builder<SELF extends Builder<SELF>> {

        private boolean sslEnabled = SSL_ENABLED;
        private String sslKeystoreFile = SSL_KEYSTORE_FILE;
        private String sslTruststoreFile = SSL_TRUSTSTORE_FILE;
        private String sslKeystorePassword = SSL_KEYSTORE_PASSWORD;
        private String sslTruststorePassword = SSL_TRUSTSTORE_PASSWORD;
        private KeyStore sslKeystore = SSL_KEYSTORE;
        private KeyStore sslTruststore = SSL_TRUSTSTORE;
        private String userAgent = USER_AGENT;
        private String packageNameAndVersion = PACKAGE_NAME_AND_VERSION;
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
        private int kvEndpoints = KEYVALUE_ENDPOINTS;
        private int viewEndpoints = VIEW_ENDPOINTS;
        private int queryEndpoints = QUERY_ENDPOINTS;
        private int searchEndpoints = SEARCH_ENDPOINTS;
        private Delay observeIntervalDelay = OBSERVE_INTERVAL_DELAY;
        private Delay reconnectDelay = RECONNECT_DELAY;
        private Delay retryDelay = RETRY_DELAY;
        private RetryStrategy retryStrategy = RETRY_STRATEGY;
        private EventLoopGroup ioPool;
        private EventLoopGroup kvIoPool;
        private EventLoopGroup viewIoPool;
        private EventLoopGroup queryIoPool;
        private EventLoopGroup searchIoPool;
        private EventLoopGroup analyticsIoPool;
        private ShutdownHook ioPoolShutdownHook;
        private ShutdownHook kvIoPoolShutdownHook;
        private ShutdownHook viewIoPoolShutdownHook;
        private ShutdownHook queryIoPoolShutdownHook;
        private ShutdownHook searchIoPoolShutdownHook;
        private ShutdownHook analyticsIoPoolShutdownHook;
        private Scheduler scheduler;
        private ShutdownHook schedulerShutdownHook;
        private EventBus eventBus;
        private long maxRequestLifetime = MAX_REQUEST_LIFETIME;
        private long keepAliveInterval = KEEPALIVEINTERVAL;
        private boolean continuousKeepAliveEnabled = CONTINUOUS_KEEPALIVE_ENABLED;
        private long keepAliveErrorThreshold = KEEPALIVE_ERROR_THRESHOLD;
        private long keepAliveTimeout = KEEPALIVE_TIMEOUT;
        private long autoreleaseAfter = AUTORELEASE_AFTER;
        private boolean bufferPoolingEnabled = BUFFER_POOLING_ENABLED;
        private boolean tcpNodelayEnabled = TCP_NODELAY_ENALED;
        private boolean mutationTokensEnabled = MUTATION_TOKENS_ENABLED;
        private int socketConnectTimeout = SOCKET_CONNECT_TIMEOUT;
        private boolean callbacksOnIoPool = CALLBACKS_ON_IO_POOL;
        private long disconnectTimeout = DISCONNECT_TIMEOUT;
        private WaitStrategyFactory requestBufferWaitStrategy;
        private MemcachedHashingStrategy memcachedHashingStrategy = MEMCACHED_HASHING_STRATEGY;
        private long configPollInterval = CONFIG_POLL_INTERVAL;
        private long configPollFloorInterval = CONFIG_POLL_FLOOR_INTERVAL;
        private boolean certAuthEnabled = CERT_AUTH_ENABLED;
        private CouchbaseCoreSendHook couchbaseCoreSendHook;
        private boolean forceSaslPlain = FORCE_SASL_PLAIN;
        private boolean operationTracingEnabled = OPERATION_TRACING_ENABLED;
        private boolean operationTracingServerDurationEnabled = OPERATION_TRACING_SERVER_DUR_ENABLED;
        private Tracer tracer;
        private double minCompressionRatio = MIN_COMPRESSION_RATIO;
        private int minCompressionSize = MIN_COMPRESSION_SIZE;
        private boolean compressionEnabled = COMPRESSION_ENABLED;
        private boolean orphanResponseReportingEnabled = ORPHAN_REPORTING_ENABLED;
        private OrphanResponseReporter orphanResponseReporter;
        private NetworkResolution networkResolution = NETWORK_RESOLUTION;

        private MetricsCollectorConfig runtimeMetricsCollectorConfig;
        private LatencyMetricsCollectorConfig networkLatencyMetricsCollectorConfig;
        private LoggingConsumer defaultMetricsLoggingConsumer = LoggingConsumer.create();

        private KeyValueServiceConfig keyValueServiceConfig;
        private QueryServiceConfig queryServiceConfig;
        private ViewServiceConfig viewServiceConfig;
        private SearchServiceConfig searchServiceConfig;
        private AnalyticsServiceConfig analyticsServiceConfig;


        protected Builder() {
        }

        @SuppressWarnings({ "unchecked" })
        protected SELF self() {
            return (SELF) this;
        }

        /**
         * Set if SSL should be enabled (default value {@value #SSL_ENABLED}).
         * If true, also set {@link #sslKeystoreFile(String)} and {@link #sslKeystorePassword(String)}.
         */
        public SELF sslEnabled(final boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return self();
        }

        /**
         * Defines the location of the SSL Keystore file (default value null, none).
         *
         * If this method is used without also specifying
         * {@link #sslTruststoreFile(String)} this keystore will be used to initialize
         * both the key factory as well as the trust factory with java SSL. This
         * needs to be the case for backwards compatibility, but if you do not need
         * X.509 client cert authentication you might as well just use {@link #sslTruststoreFile(String)}
         * alone.
         */
        public SELF sslKeystoreFile(final String sslKeystoreFile) {
            this.sslKeystoreFile = sslKeystoreFile;
            return self();
        }

        /**
         * Defines the location of the SSL TrustStore keystore file (default value null, none).
         *
         * If this method is used without also specifying
         * {@link #sslKeystoreFile(String)} this keystore will be used to initialize
         * both the key factory as well as the trust factory with java SSL. Prefer
         * this method over the {@link #sslKeystoreFile(String)} if you do not need
         * X.509 client auth and just need server side certificate checking.
         */
        public SELF sslTruststoreFile(final String sslTruststoreFile) {
            this.sslTruststoreFile = sslTruststoreFile;
            return self();
        }

        /**
         * Sets the SSL Keystore password to be used with the Keystore file (default value null, none).
         *
         * @see #sslKeystoreFile(String)
         */
        public SELF sslKeystorePassword(final String sslKeystorePassword) {
            this.sslKeystorePassword = sslKeystorePassword;
            return self();
        }

        /**
         * Sets the SSL TrustStore password to be used with the Keystore file (default value null, none).
         *
         * @see #sslKeystoreFile(String)
         */
        public SELF sslTruststorePassword(final String sslTruststorePassword) {
            this.sslTruststorePassword = sslTruststorePassword;
            return self();
        }

        /**
         * Sets the SSL Keystore directly and not indirectly via filepath.
         *
         * If this method is used without also specifying
         * {@link #sslTruststore(KeyStore)} this keystore will be used to initialize
         * both the key factory as well as the trust factory with java SSL. This
         * needs to be the case for backwards compatibility, but if you do not need
         * X.509 client cert authentication you might as well just use {@link #sslTruststore(KeyStore)}
         * alone.
         *
         * @param sslKeystore the keystore to use.
         */
        public SELF sslKeystore(final KeyStore sslKeystore) {
            this.sslKeystore = sslKeystore;
            return self();
        }

        /**
         * Sets the SSL Keystore for the TrustStore directly and not indirectly via filepath.
         *
         * If this method is used without also specifying
         * {@link #sslKeystore(KeyStore)} this keystore will be used to initialize
         * both the key factory as well as the trust factory with java SSL. Prefer
         * this method over the {@link #sslKeystore(KeyStore)} if you do not need
         * X.509 client auth and just need server side certificate checking.
         *
         * @param sslTruststore the keystore to use.
         */
        public SELF sslTruststore(final KeyStore sslTruststore) {
            this.sslTruststore = sslTruststore;
            return self();
        }

        /**
         * Toggles bootstrap via Http (default value {@value #BOOTSTRAP_HTTP_ENABLED}).
         */
        public SELF bootstrapHttpEnabled(final boolean bootstrapHttpEnabled) {
            this.bootstrapHttpEnabled = bootstrapHttpEnabled;
            return self();
        }

        /**
         * Toggles bootstrap via carrier publication (default value {@value #BOOTSTRAP_CARRIER_ENABLED}).
         */
        public SELF bootstrapCarrierEnabled(final boolean bootstrapCarrierEnabled) {
            this.bootstrapCarrierEnabled = bootstrapCarrierEnabled;
            return self();
        }

        /**
         * If Http bootstrap is enabled and not SSL, sets the port to use
         * (default value {@value #BOOTSTRAP_HTTP_DIRECT_PORT}).
         */
        public SELF bootstrapHttpDirectPort(final int bootstrapHttpDirectPort) {
            this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
            return self();
        }

        /**
         * If Http bootstrap and SSL are enabled, sets the port to use
         * (default value {@value #BOOTSTRAP_HTTP_SSL_PORT}).
         */
        public SELF bootstrapHttpSslPort(final int bootstrapHttpSslPort) {
            this.bootstrapHttpSslPort = bootstrapHttpSslPort;
            return self();
        }

        /**
         * If carrier publication bootstrap is enabled and not SSL, sets the port to use
         * (default value {@value #BOOTSTRAP_CARRIER_DIRECT_PORT}).
         */
        public SELF bootstrapCarrierDirectPort(final int bootstrapCarrierDirectPort) {
            this.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
            return self();
        }

        /**
         * If carrier publication bootstrap and SSL are enabled, sets the port to use
         * (default value {@value #BOOTSTRAP_CARRIER_SSL_PORT}).
         */
        public SELF bootstrapCarrierSslPort(final int bootstrapCarrierSslPort) {
            this.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
            return self();
        }

        /**
         * Sets the pool size (number of threads to use) for I/O
         * operations (default value is the number of CPUs).
         *
         * If there is more nodes in the cluster than the defined
         * ioPoolSize, multiplexing will automatically happen.
         */
        public SELF ioPoolSize(final int ioPoolSize) {
            this.ioPoolSize = ioPoolSize;
            return self();
        }

        /**
         * Sets the pool size (number of threads to use) for all non blocking operations in the core and clients
         * (default value is the number of CPUs).
         *
         * Don't size it too small since it would significantly impact performance.
         */
        public SELF computationPoolSize(final int computationPoolSize) {
            this.computationPoolSize = computationPoolSize;
            return self();
        }

        /**
         * Sets the size of the RingBuffer structure that queues requests (default value {@value #REQUEST_BUFFER_SIZE}).
         * This is an advanced parameter that usually shouldn't need to be changed.
         */
        public SELF requestBufferSize(final int requestBufferSize) {
            this.requestBufferSize = requestBufferSize;
            return self();
        }

        /**
         * Sets the size of the RingBuffer structure that queues responses
         * (default value {@value #RESPONSE_BUFFER_SIZE}).
         * This is an advanced parameter that usually shouldn't need to be changed
         */
        public SELF responseBufferSize(final int responseBufferSize) {
            this.responseBufferSize = responseBufferSize;
            return self();
        }

        /**
         * Sets the number of Key/Value endpoints to open per nodes in the cluster
         * (default value {@value #KEYVALUE_ENDPOINTS}).
         *
         * Only tune to more if IO has been identified as the most probable bottleneck,
         * since it can reduce batching on the tcp/network level.
         *
         * @deprecated Please use {@link Builder#keyValueServiceConfig(KeyValueServiceConfig)} going forward.
         */
        public SELF kvEndpoints(final int kvEndpoints) {
            this.kvEndpoints = kvEndpoints;
            return self();
        }

        /**
         * Sets the number of View endpoints to open per node in the cluster (default value {@value #VIEW_ENDPOINTS}).
         *
         * Setting this to a higher number is advised in heavy view workloads.
         *
         * @deprecated Please use {@link Builder#viewServiceConfig(ViewServiceConfig)} going forward.
         */
        public SELF viewEndpoints(final int viewEndpoints) {
            this.viewEndpoints = viewEndpoints;
            return self();
        }

        /**
         * Sets the number of Query (N1QL) endpoints to open per node in the cluster
         * (default value {@value #QUERY_ENDPOINTS}).
         *
         * Setting this to a higher number is advised in heavy query workloads.
         *
         * @deprecated Please use {@link Builder#queryServiceConfig(QueryServiceConfig)} going forward.
         */
        public SELF queryEndpoints(final int queryEndpoints) {
            this.queryEndpoints = queryEndpoints;
            return self();
        }

        /**
         * Sets the number of Search (CBFT) endpoints to open per node in the cluster
         * (default value {@value #SEARCH_ENDPOINTS}).
         *
         * Setting this to a higher number is advised in heavy query workloads.
         *
         * @deprecated Please use {@link Builder#searchServiceConfig(SearchServiceConfig)} going forward.
         */
        public SELF searchEndpoints(final int searchEndpoints) {
            this.searchEndpoints = searchEndpoints;
            return self();
        }

        /**
         * Sets the USER-AGENT String to be sent in HTTP requests headers (should usually not be tweaked,
         * default value is computed from the SDK {@link #packageNameAndVersion()}).
         */
        public SELF userAgent(final String userAgent) {
            this.userAgent = userAgent;
            return self();
        }

        /**
         * Sets the String to be used as identifier for the library namespace and version.
         * (should usually not be tweaked, default value is computed at build time from VCS tags/commits).
         *
         * This is used in {@link #userAgent()} notably.
         */
        public SELF packageNameAndVersion(final String packageNameAndVersion) {
            this.packageNameAndVersion = packageNameAndVersion;
            return self();
        }

        /**
         * Sets the {@link Delay} for {@link Observe} poll operations (default value
         * is a delay growing exponentially between 10us and 100ms).
         */
        public SELF observeIntervalDelay(final Delay observeIntervalDelay) {
            this.observeIntervalDelay = observeIntervalDelay;
            return self();
        }

        /**
         * Sets the {@link Delay} for node reconnects (default value is a delay growing exponentially
         * between 32ms and 4096ms).
         */
        public SELF reconnectDelay(final Delay reconnectDelay) {
            this.reconnectDelay = reconnectDelay;
            return self();
        }

        /**
         * Sets the {@link Delay} for retries of requests (default value is a delay growing exponentially
         * between 100us and 100ms).
         */
        public SELF retryDelay(final Delay retryDelay) {
            this.retryDelay = retryDelay;
            return self();
        }

        /**
         * Sets the I/O Pool implementation for the underlying IO framework.
         * This is an advanced configuration that should only be used if you know what you are doing.
         *
         * @deprecated use {@link #ioPool(EventLoopGroup, ShutdownHook)} to also provide a shutdown hook.
         */
        @Deprecated
        public SELF ioPool(final EventLoopGroup group) {
            return ioPool(group, new NoOpShutdownHook());
        }

        /**
         * Sets the I/O Pool implementation for the underlying IO framework, along with the action
         * to execute when this environment is shut down.
         * This is an advanced configuration that should only be used if you know what you are doing.
         */
        public SELF ioPool(final EventLoopGroup group, final ShutdownHook shutdownHook) {
            this.ioPool = group;
            this.ioPoolShutdownHook = shutdownHook;
            return self();
        }

        /**
         * Sets the KV I/O Pool implementation for the underlying IO framework, along with the action
         * to execute when this environment is shut down.
         * This is an advanced configuration that should only be used if you know what you are doing.
         */
        public SELF kvIoPool(final EventLoopGroup group, final ShutdownHook shutdownHook) {
            this.kvIoPool = group;
            this.kvIoPoolShutdownHook = shutdownHook;
            return self();
        }

        /**
         * Sets the View I/O Pool implementation for the underlying IO framework, along with the action
         * to execute when this environment is shut down.
         * This is an advanced configuration that should only be used if you know what you are doing.
         */
        public SELF viewIoPool(final EventLoopGroup group, final ShutdownHook shutdownHook) {
            this.viewIoPool = group;
            this.viewIoPoolShutdownHook = shutdownHook;
            return self();
        }

        /**
         * Sets the Query I/O Pool implementation for the underlying IO framework, along with the action
         * to execute when this environment is shut down.
         * This is an advanced configuration that should only be used if you know what you are doing.
         */
        public SELF queryIoPool(final EventLoopGroup group, final ShutdownHook shutdownHook) {
            this.queryIoPool = group;
            this.queryIoPoolShutdownHook = shutdownHook;
            return self();
        }

        /**
         * Sets the Search I/O Pool implementation for the underlying IO framework, along with the action
         * to execute when this environment is shut down.
         * This is an advanced configuration that should only be used if you know what you are doing.
         */
        public SELF searchIoPool(final EventLoopGroup group, final ShutdownHook shutdownHook) {
            this.searchIoPool = group;
            this.searchIoPoolShutdownHook = shutdownHook;
            return self();
        }

        /**
         * Sets the Analytics I/O Pool implementation for the underlying IO framework, along with the action
         * to execute when this environment is shut down.
         * This is an advanced configuration that should only be used if you know what you are doing.
         */
        public SELF analyticsIoPool(final EventLoopGroup group, final ShutdownHook shutdownHook) {
            this.analyticsIoPool = group;
            this.analyticsIoPoolShutdownHook = shutdownHook;
            return self();
        }

        /**
         * Sets the Scheduler implementation for the underlying computation framework.
         * This is an advanced configuration that should only be used if you know what you are doing.
         *
         * @deprecated use {@link #ioPool(EventLoopGroup, ShutdownHook)} to also provide a shutdown hook.
         */
        @Deprecated
        public SELF scheduler(final Scheduler scheduler) {
            return scheduler(scheduler, new NoOpShutdownHook());
        }

        /**
         * Sets the Scheduler implementation for the underlying computation framework, along with the action
         * to execute when this environment is shut down.
         * This is an advanced configuration that should only be used if you know what you are doing.
         */
        public SELF scheduler(final Scheduler scheduler, final ShutdownHook shutdownHook) {
            this.scheduler = scheduler;
            this.schedulerShutdownHook = shutdownHook;
            return self();
        }

        /**
         * Sets the {@link RetryStrategy} to be used during request retries
         * (default value is a {@link BestEffortRetryStrategy}).
         */
        public SELF retryStrategy(final RetryStrategy retryStrategy) {
            this.retryStrategy = retryStrategy;
            return self();
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
        public SELF maxRequestLifetime(final long maxRequestLifetime) {
            this.maxRequestLifetime = maxRequestLifetime;
            return self();
        }

        /**
         * Sets the time of inactivity, in milliseconds, after which some services
         * will issue a form of keep-alive request to their corresponding server/nodes
         * (default is 30s, values <= 0 deactivate the idle check).
         */
        public SELF keepAliveInterval(long keepAliveIntervalMilliseconds) {
            this.keepAliveInterval = keepAliveIntervalMilliseconds;
            return self();
        }

        /**
         * Sets the time after which any non-consumed buffers will be automatically released.
         * Setting this to a higher value than a few seconds is not recommended since this
         * may lead to increased garbage collection.
         */
        public SELF autoreleaseAfter(long autoreleaseAfter) {
            this.autoreleaseAfter = autoreleaseAfter;
            return self();
        }

        /**
         * Sets the event bus to an alternative implementation.
         *
         * This setting should only be tweaked in advanced cases.
         */
        public SELF eventBus(final EventBus eventBus) {
            this.eventBus = eventBus;
            return self();
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
        public SELF bufferPoolingEnabled(boolean bufferPoolingEnabled) {
            this.bufferPoolingEnabled = bufferPoolingEnabled;
            return self();
        }

        /**
         * If TCP_NODELAY is manually disabled, Nagle'ing will take effect on both the client
         * and (if supported) the server side.
         */
        public SELF tcpNodelayEnabled(boolean tcpNodelayEnabled) {
            this.tcpNodelayEnabled = tcpNodelayEnabled;
            return self();
        }

        /**
         * If mutation tokens are enabled, they can be used for advanced durability requirements,
         * as well as optimized RYOW consistency.
         *
         * Note that just enabling it here won't help if the server does not support it as well. Use at
         * least Couchbase Server 4.0. Also, consider the additional overhead of 16 bytes per mutation response
         * (8 byte for the vbucket uuid and 8 byte for the sequence number).
         */
        public SELF mutationTokensEnabled(boolean mutationTokensEnabled) {
            this.mutationTokensEnabled = mutationTokensEnabled;
            return self();
        }

        /**
         * Sets a custom configuration for the {@link RuntimeMetricsCollector}.
         *
         * @param metricsCollectorConfig the custom configuration
         */
        public SELF runtimeMetricsCollectorConfig(MetricsCollectorConfig metricsCollectorConfig) {
            this.runtimeMetricsCollectorConfig = metricsCollectorConfig;
            return self();
        }

        /**
         * Sets a custom configuration for the {@link NetworkLatencyMetricsCollector}.
         *
         * @param metricsCollectorConfig the custom configuration for the collector.
         */
        public SELF networkLatencyMetricsCollectorConfig(LatencyMetricsCollectorConfig metricsCollectorConfig) {
            this.networkLatencyMetricsCollectorConfig = metricsCollectorConfig;
            return self();
        }

        public SELF defaultMetricsLoggingConsumer(boolean enabled, CouchbaseLogLevel level, LoggingConsumer.OutputFormat format) {
            if (enabled) {
                defaultMetricsLoggingConsumer = LoggingConsumer.create(level, format);
            } else {
                defaultMetricsLoggingConsumer = null;
            }
            return self();
        }

        public SELF defaultMetricsLoggingConsumer(boolean enabled, CouchbaseLogLevel level) {
            return defaultMetricsLoggingConsumer(enabled, level, LoggingConsumer.DEFAULT_FORMAT);
        }

        /**
         * Sets a custom socket connect timeout.
         *
         * @param socketConnectTimeout the socket connect timeout in milliseconds.
         */
        public SELF socketConnectTimeout(int socketConnectTimeout) {
            this.socketConnectTimeout = socketConnectTimeout;
            return self();
        }

        /**
         * Set to true if the {@link Observable} callbacks should be completed on the IO event loops.
         *
         * Note: this is an advanced option and must be used with care. It can be used to improve performance since it
         * removes additional scheduling overhead on the response path, but any blocking calls in the callbacks will
         * lead to more work on the event loops itself and eventually stall them.
         *
         * USE WITH CARE!
         */
        public SELF callbacksOnIoPool(boolean callbacksOnIoPool) {
            this.callbacksOnIoPool = callbacksOnIoPool;
            return self();
        }

        /**
         * Sets a custom disconnect timeout.
         *
         * @param disconnectTimeout the disconnect timeout in milliseconds.
         */
        public SELF disconnectTimeout(long disconnectTimeout) {
            this.disconnectTimeout = disconnectTimeout;
            return self();
        }

        /**
         * Sets a custom waiting strategy for requests. Default is {@link BlockingWaitStrategy}.
         *
         * @param waitStrategy waiting strategy
         */
        @InterfaceStability.Experimental
        @InterfaceAudience.Public
        public SELF requestBufferWaitStrategy(WaitStrategyFactory waitStrategy) {
            this.requestBufferWaitStrategy = waitStrategy;
            return self();
        }

        /**
         * Sets a custom memcached node hashing strategy, mainly used for compatibility with other clients.
         *
         * @param memcachedHashingStrategy the strategy to use.
         */
        public SELF memcachedHashingStrategy(MemcachedHashingStrategy memcachedHashingStrategy) {
            this.memcachedHashingStrategy = memcachedHashingStrategy;
            return self();
        }

        /**
         * Allows to set a custom configuration for the KV service.
         *
         * See {@link KeyValueServiceConfig} for more information on the possible options.
         *
         * @param keyValueServiceConfig the config to apply.
         */
        public SELF keyValueServiceConfig(KeyValueServiceConfig keyValueServiceConfig) {
            this.keyValueServiceConfig = keyValueServiceConfig;
            return self();
        }

        /**
         * Allows to set a custom configuration for the View service.
         *
         * See {@link ViewServiceConfig} for more information on the possible options.
         *
         * @param viewServiceConfig the config to apply.
         */
        public SELF viewServiceConfig(ViewServiceConfig viewServiceConfig) {
            this.viewServiceConfig = viewServiceConfig;
            return self();
        }

        /**
         * Allows to set a custom configuration for the Query service.
         *
         * See {@link QueryServiceConfig} for more information on the possible options.
         *
         * @param queryServiceConfig the config to apply.
         */
        public SELF queryServiceConfig(QueryServiceConfig queryServiceConfig) {
            this.queryServiceConfig = queryServiceConfig;
            return self();
        }

        /**
         * Allows to set a custom configuration for the Search service.
         *
         * See {@link SearchServiceConfig} for more information on the possible options.
         *
         * @param searchServiceConfig the config to apply.
         */
        public SELF searchServiceConfig(SearchServiceConfig searchServiceConfig) {
            this.searchServiceConfig = searchServiceConfig;
            return self();
        }

        /**
         * Allows to set a custom configuration for the Analytics service.
         *
         * See {@link AnalyticsServiceConfig} for more information on the possible options.
         *
         * @param analyticsServiceConfig the config to apply.
         */
        public SELF analyticsServiceConfig(AnalyticsServiceConfig analyticsServiceConfig) {
            this.analyticsServiceConfig = analyticsServiceConfig;
            return self();
        }

        /**
         * Allows to set the configuration poll interval which polls the server cluster
         * configuration proactively.
         *
         * Note that the interval cannot be set lower than the floor interval defined in
         * {@link #configPollFloorInterval(long)}.
         *
         * @param configPollInterval the interval in milliseconds, 0 deactivates the polling.
         */
        @InterfaceStability.Committed
        @InterfaceAudience.Public
        public SELF configPollInterval(long configPollInterval) {
            this.configPollInterval = configPollInterval;
            return self();
        }

        /**
         * Allows to set the minimum config polling interval.
         *
         * Note that the {@link #configPollInterval(long)} obviously needs to be equal
         * or larger than this setting, otherwise intervals will be skipped.
         *
         * @param configPollFloorInterval the interval in milliseconds.
         */
        @InterfaceStability.Committed
        @InterfaceAudience.Public
        public SELF configPollFloorInterval(long configPollFloorInterval) {
            this.configPollFloorInterval = configPollFloorInterval;
            return self();
        }

        /**
         * Allows to enable X.509 client certificate authentication. Needs to be used in
         * combination with {@link #sslEnabled(boolean)} and related methods of course.
         */
        @InterfaceStability.Uncommitted
        @InterfaceAudience.Public
        public SELF certAuthEnabled(boolean certAuthEnabled) {
            this.certAuthEnabled = certAuthEnabled;
            return self();
        }

        /**
         * Allows to enable or disable the continous emitting of keepalive messages.
         */
        @InterfaceAudience.Public
        @InterfaceStability.Uncommitted
        public SELF continuousKeepAliveEnabled(final boolean continuousKeepAliveEnabled) {
            this.continuousKeepAliveEnabled = continuousKeepAliveEnabled;
            return self();
        }

        /**
         * Allows to customize the errors on keepalive messages threshold after which the
         * connection will be recycled.
         */
        @InterfaceAudience.Public
        @InterfaceStability.Uncommitted
        public SELF keepAliveErrorThreshold(final long keepAliveErrorThreshold) {
            this.keepAliveErrorThreshold = keepAliveErrorThreshold;
            return self();
        }

        /**
         * Allows to customize the timeout used for keepalive operations.
         */
        @InterfaceAudience.Public
        @InterfaceStability.Uncommitted
        public SELF keepAliveTimeout(final long keepAliveTimeout) {
            this.keepAliveTimeout = keepAliveTimeout;
            return self();
        }

        /**
         * Allows to configure a custom core send hook, see the javadocs for it for more details.
         */
        @InterfaceAudience.Public
        @InterfaceStability.Experimental
        public SELF couchbaseCoreSendHook(final CouchbaseCoreSendHook hook) {
            this.couchbaseCoreSendHook = hook;
            return self();
        }

        /**
         * Allows to forcre the KeyValue SASL authentication method to PLAIN which is used to
         * allow authentication against LDAP-based users.
         *
         * @param forceSaslPlain true if plain should be forced, false otherwise.
         */
        @InterfaceAudience.Public
        @InterfaceStability.Committed
        public SELF forceSaslPlain(final boolean forceSaslPlain) {
            this.forceSaslPlain = forceSaslPlain;
            return self();
        }

        /**
         * Allows to enable/disable the tracing support.
         *
         * Note that it is enabled by default, but only activated once opentracing is
         * on the classpath!
         *
         * @param operationTracingEnabled enable or disable the tracing.
         * @return this builder for chaining purposes.
         */
        @InterfaceStability.Committed
        public SELF operationTracingEnabled(final boolean operationTracingEnabled) {
            this.operationTracingEnabled = operationTracingEnabled;
            return self();
        }

        /**
         * Allows to enable/disable the negotiation of server duration-enabled tracing.
         *
         * Note that this is enabled by default, but depending on the server version
         * may or may not be available (it is negotiated at runtime)!
         *
         * @param operationTracingServerDurationEnabled enable or disable the server duration.
         * @return this builder for chaining purposes.
         */
        @InterfaceStability.Committed
        public SELF operationTracingServerDurationEnabled(final boolean operationTracingServerDurationEnabled) {
            this.operationTracingServerDurationEnabled = operationTracingServerDurationEnabled;
            return self();
        }

        /**
         * Allows to specify a custom tracer.
         *
         * @param tracer the tracer to use.
         * @return this builder for chaining purposes.
         */
        @InterfaceStability.Committed
        public SELF tracer(final Tracer tracer) {
            this.tracer = tracer;
            return self();
        }

        /**
         * Allows to adjust the minimum size of a document to be considered
         * for compression - in bytes.
         *
         * @param compressionMinSize min compression size in bytes.
         * @return the builder for chaining purposes.
         */
        @InterfaceAudience.Public
        @InterfaceStability.Uncommitted
        public SELF compressionMinSize(final int compressionMinSize) {
            this.minCompressionSize = compressionMinSize;
            return self();
        }

        /**
         * Allows to adjust the ratio over which the document is sent
         * compressed over the wire - in percent.
         *
         * @param compressionMinRatio the compression ratio in percent.
         * @return the builder for chaining purposes.
         */
        @InterfaceAudience.Public
        @InterfaceStability.Uncommitted
        public SELF compressionMinRatio(final double compressionMinRatio) {
            this.minCompressionRatio = compressionMinRatio;
            return self();
        }

        /**
         * Allows to enable/disable compression completely, enabled by default.
         *
         * @param compressionEnabled if compression should be enabled or disabled.
         * @return the builder for chaining purposes.
         */
        @InterfaceAudience.Public
        @InterfaceStability.Uncommitted
        public SELF compressionEnabled(final boolean compressionEnabled) {
            this.compressionEnabled = compressionEnabled;
            return self();
        }

        @InterfaceAudience.Public
        @InterfaceStability.Committed
        public SELF orphanResponseReportingEnabled(final boolean orphanResponseReportingEnabled) {
            this.orphanResponseReportingEnabled = orphanResponseReportingEnabled;
            return self();
        }

        @InterfaceAudience.Public
        @InterfaceStability.Committed
        public SELF orphanResponseReporter(final OrphanResponseReporter orphanResponseReporter) {
            this.orphanResponseReporter = orphanResponseReporter;
            return self();
        }

        /**
         * Allows to tune the network resolution setting, pinning it to either
         * internal or external instead of relying on the automatic mechanism.
         *
         * @param networkResolution the network resolution to use.
         * @return the builder for chaining purposes.
         */
        @InterfaceAudience.Public
        @InterfaceStability.Uncommitted
        public SELF networkResolution(final NetworkResolution networkResolution) {
            if (networkResolution == null) {
                throw new IllegalArgumentException("NetworkResolution must not be null!");
            }
            this.networkResolution = networkResolution;
            return self();
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
        sb.append(", sslTruststoreFile='").append(sslTruststoreFile).append('\'');
        sb.append(", sslKeystorePassword=").append(sslKeystorePassword != null && !sslKeystorePassword.isEmpty());
        sb.append(", sslTruststorePassword=").append(sslTruststorePassword != null && !sslTruststorePassword.isEmpty());
        sb.append(", sslKeystore=").append(sslKeystore);
        sb.append(", sslTruststore=").append(sslTruststore);
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
        sb.append(", searchServiceEndpoints=").append(searchServiceEndpoints);
        sb.append(", configPollInterval=").append(configPollInterval);
        sb.append(", configPollFloorInterval=").append(configPollFloorInterval);
        sb.append(", networkResolution=").append(networkResolution);
        sb.append(", ioPool=").append(ioPool.getClass().getSimpleName());
        if (ioPoolShutdownHook == null || ioPoolShutdownHook instanceof  NoOpShutdownHook) {
            sb.append("!unmanaged");
        }
        if (kvIoPool != null) {
            sb.append(", kvIoPool=").append(kvIoPool.getClass().getSimpleName());
            if (kvIoPoolShutdownHook == null || kvIoPoolShutdownHook instanceof  NoOpShutdownHook) {
                sb.append("!unmanaged");
            }
        } else {
            sb.append(", kvIoPool=").append("null");
        }
        if (viewIoPool != null) {
            sb.append(", viewIoPool=").append(viewIoPool.getClass().getSimpleName());
            if (viewIoPoolShutdownHook == null || viewIoPoolShutdownHook instanceof  NoOpShutdownHook) {
                sb.append("!unmanaged");
            }
        } else {
            sb.append(", viewIoPool=").append("null");
        }
        if (searchIoPool != null) {
            sb.append(", searchIoPool=").append(searchIoPool.getClass().getSimpleName());
            if (searchIoPoolShutdownHook == null || searchIoPoolShutdownHook instanceof  NoOpShutdownHook) {
                sb.append("!unmanaged");
            }
        } else {
            sb.append(", searchIoPool=").append("null");
        }
        if (queryIoPool != null) {
            sb.append(", queryIoPool=").append(queryIoPool.getClass().getSimpleName());
            if (queryIoPoolShutdownHook == null || queryIoPoolShutdownHook instanceof  NoOpShutdownHook) {
                sb.append("!unmanaged");
            }
        } else {
            sb.append(", queryIoPool=").append("null");
        }
        if (analyticsIoPool != null) {
            sb.append(", analyticsIoPool=").append(analyticsIoPool.getClass().getSimpleName());
            if (analyticsIoPoolShutdownHook == null || analyticsIoPoolShutdownHook instanceof  NoOpShutdownHook) {
                sb.append("!unmanaged");
            }
        } else {
            sb.append(", analyticsIoPool=").append("null");
        }

        sb.append(", coreScheduler=").append(coreScheduler.getClass().getSimpleName());
        if (coreSchedulerShutdownHook == null || coreSchedulerShutdownHook instanceof NoOpShutdownHook) {
            sb.append("!unmanaged");
        }
        sb.append(", memcachedHashingStrategy=").append(memcachedHashingStrategy.getClass().getSimpleName());
        sb.append(", eventBus=").append(eventBus.getClass().getSimpleName());
        sb.append(", packageNameAndVersion=").append(packageNameAndVersion);
        sb.append(", retryStrategy=").append(retryStrategy);
        sb.append(", maxRequestLifetime=").append(maxRequestLifetime);
        sb.append(", retryDelay=").append(retryDelay);
        sb.append(", reconnectDelay=").append(reconnectDelay);
        sb.append(", observeIntervalDelay=").append(observeIntervalDelay);
        sb.append(", keepAliveInterval=").append(keepAliveInterval);
        sb.append(", continuousKeepAliveEnabled=").append(continuousKeepAliveEnabled);
        sb.append(", keepAliveErrorThreshold=").append(keepAliveErrorThreshold);
        sb.append(", keepAliveTimeout=").append(keepAliveTimeout);
        sb.append(", autoreleaseAfter=").append(autoreleaseAfter);
        sb.append(", bufferPoolingEnabled=").append(bufferPoolingEnabled);
        sb.append(", tcpNodelayEnabled=").append(tcpNodelayEnabled);
        sb.append(", mutationTokensEnabled=").append(mutationTokensEnabled);
        sb.append(", socketConnectTimeout=").append(socketConnectTimeout);
        sb.append(", callbacksOnIoPool=").append(callbacksOnIoPool);
        sb.append(", disconnectTimeout=").append(disconnectTimeout);
        sb.append(", requestBufferWaitStrategy=").append(requestBufferWaitStrategy);
        sb.append(", certAuthEnabled=").append(certAuthEnabled);
        sb.append(", coreSendHook=").append(couchbaseCoreSendHook == null ? "null" :
            couchbaseCoreSendHook.getClass().getSimpleName());
        sb.append(", forceSaslPlain=").append(forceSaslPlain);
        sb.append(", compressionMinRatio=").append(minCompressionRatio);
        sb.append(", compressionMinSize=").append(minCompressionSize);
        sb.append(", compressionEnabled=").append(compressionEnabled);
        sb.append(", operationTracingEnabled=").append(operationTracingEnabled);
        sb.append(", operationTracingServerDurationEnabled=").append(operationTracingServerDurationEnabled);
        sb.append(", tracer=").append(tracer != null ? tracer.getClass().getSimpleName() : "null");
        sb.append(", orphanResponseReportingEnabled=").append(orphanResponseReportingEnabled);
        sb.append(", orphanResponseReporter=").append(orphanResponseReporter != null ? orphanResponseReporter.getClass().getSimpleName() : "null");
        sb.append(", keyValueServiceConfig=").append(keyValueServiceConfig);
        sb.append(", queryServiceConfig=").append(queryServiceConfig);
        sb.append(", searchServiceConfig=").append(searchServiceConfig);
        sb.append(", viewServiceConfig=").append(viewServiceConfig);
        sb.append(", analyticsServiceConfig=").append(analyticsServiceConfig);
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
