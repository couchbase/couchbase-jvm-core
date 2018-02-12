/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.hooks.CouchbaseCoreSendHook;
import com.couchbase.client.core.message.observe.Observe;
import com.couchbase.client.core.metrics.MetricsCollector;
import com.couchbase.client.core.metrics.NetworkLatencyMetricsCollector;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.time.Delay;
import io.netty.channel.EventLoopGroup;
import rx.Observable;
import rx.Scheduler;

import java.util.concurrent.TimeUnit;

/**
 * A {@link CoreEnvironment} provides all the core building blocks like environment settings and thread pools so
 * that the application can work with it properly.
 *
 * This interface defines the contract. How properties are loaded is chosen by the implementation. See the
 * {@link DefaultCoreEnvironment} class for the default implementation.
 *
 * Note that the {@link CoreEnvironment} is stateful, so be sure to call {@link #shutdown()} or
 * {@link #shutdownAsync()} properly.
 */
public interface CoreEnvironment extends SecureEnvironment, ConfigParserEnvironment {

    /**
     * Shutdown the {@link CoreEnvironment} with the default timeout.
     *
     * Note: If this Environment has been created by the user, it is very important to properly close it, but
     * only after all the related cluster and bucket resources need to be closed! Once an environment
     * is shut down, all its contained resources cannot be used any longer!
     *
     * This method has been converted (after a deprecation phase) from an async method into a synchronous one.
     * The async version can still be found at {@link #shutdownAsync()}.
     *
     * @return returning a boolean, indicating the success of the shutdown.
     */
    boolean shutdown();

    /**
     * Shutdown the {@link CoreEnvironment} with a custom timeout.
     *
     * Note: If this Environment has been created by the user, it is very important to properly close it, but
     * only after all the related cluster and bucket resources need to be closed! Once an environment
     * is shut down, all its contained resources cannot be used any longer!
     *
     * This method has been converted (after a deprecation phase) from an async method into a synchronous one.
     * The async version can still be found at {@link #shutdownAsync()}.
     *
     * @return returning a boolean, indicating the success of the shutdown.
     */
    boolean shutdown(long timeout, TimeUnit timeUnit);

    /**
     * The default timeout for disconnect operations, set to {@link DefaultCoreEnvironment#DISCONNECT_TIMEOUT}.
     *
     * @return the default disconnect timeout.
     */
    long disconnectTimeout();

    /**
     * Shutdown the {@link CoreEnvironment} in an asynchronous fashion.
     *
     * Note: If this Environment has been created by the user, it is very important to properly close it, but
     * only after all the related cluster and bucket resources need to be closed! Once an environment
     * is shut down, all its contained resources cannot be used any longer!
     *
     * Since this method is asynchronous and cold, it is important to subscribe to the observable to actually
     * initiate the shutdown process.
     *
     * @return an {@link Observable} eventually returning a boolean, indicating the success of the shutdown.
     */
    Observable<Boolean> shutdownAsync();

    /**
     * Returns the IO pool for the underlying IO framework, used as the default pool if not overridden
     * for the individual services.
     *
     * @return the default IO pool, shared across resources.
     */
    EventLoopGroup ioPool();

    /**
     * If set returns the IO Pool for the KV service, if not returns null.
     *
     * @return the KV IO pool if set, null otherwise.
     */
    EventLoopGroup kvIoPool();

    /**
     * If set returns the IO Pool for the View service, if not returns null.
     *
     * @return the view IO pool if set, null otherwise.
     */
    EventLoopGroup viewIoPool();

    /**
     * If set returns the IO Pool for the query service, if not returns null.
     *
     * @return the query IO pool if set, null otherwise.
     */
    EventLoopGroup queryIoPool();

    /**
     * If set returns the IO Pool for the search service, if not returns null.
     *
     * @return the search IO pool if set, null otherwise.
     */
    EventLoopGroup searchIoPool();

    /**
     * Returns the current {@link com.couchbase.client.core.service.KeyValueService} configuration.
     *
     * @return the current config for the kv service.
     */
    KeyValueServiceConfig kvServiceConfig();

    /**
     * Returns the current {@link com.couchbase.client.core.service.QueryService} configuration.
     *
     * @return the current config for the query service.
     */
    QueryServiceConfig queryServiceConfig();

    /**
     * Returns the current {@link com.couchbase.client.core.service.ViewService} configuration.
     *
     * @return the current config for the view service.
     */
    ViewServiceConfig viewServiceConfig();

    /**
     * Returns the current {@link com.couchbase.client.core.service.SearchService} configuration.
     *
     * @return the current config for the search service.
     */
    SearchServiceConfig searchServiceConfig();

    /**
     * Returns the scheduler which should be used for all core actions that need to happen
     * asynchronously.
     *
     * @return the scheduler used for internal operations.
     */
    Scheduler scheduler();

    /**
     * If bootstrapping through HTTP is enabled.
     *
     * @return true if enabled.
     */
    boolean bootstrapHttpEnabled();

    /**
     * If bootstrapping through the advanced carrier publication is enabled.
     *
     * @return true if enabled.
     */
    boolean bootstrapCarrierEnabled();

    /**
     * The port to use when bootstrapping through HTTP without SSL.
     *
     * @return the direct http port.
     */
    int bootstrapHttpDirectPort();

    /**
     * The port to use when bootstrapping through HTTP with SSL.
     *
     * @return the https port.
     */
    int bootstrapHttpSslPort();

    /**
     * The port to use when bootstrapping through carrier publication without SSL.
     *
     * @return the direct carrier publication port.
     */
    int bootstrapCarrierDirectPort();

    /**
     * The port to use when bootstrapping through carrier publication with SSL.
     *
     * @return the ssl carrier publication port.
     */
    int bootstrapCarrierSslPort();

    /**
     * Returns the configured IO pool size.
     *
     * @return the pool size (number of threads to use).
     */
    int ioPoolSize();

    /**
     * Returns the pool size (number of threads) for all computation tasks.
     *
     * @return the pool size (number of threads to use).
     */
    int computationPoolSize();

    /**
     * Returns the {@link Delay} for {@link Observe} poll operations.
     *
     * @return the observe interval delay.
     */
    Delay observeIntervalDelay();

    /**
     * Returns the {@link Delay} for node reconnects.
     *
     * @return the node reconnect delay.
     */
    Delay reconnectDelay();

    /**
     * Returns the {@link Delay} for request retries.
     *
     * @return the request retry delay.
     */
    Delay retryDelay();

    /**
     * Returns the size of the request ringbuffer.
     *
     * @return the size of the ringbuffer.
     */
    int requestBufferSize();

    /**
     * Returns the size of the response ringbuffer.
     *
     * @return the size of the ringbuffer.
     */
    int responseBufferSize();

    /**
     * The number of key/value service endpoints.
     *
     * @return amount of endpoints per service.
     */
    int kvEndpoints();

    /**
     * The number of view service endpoints.
     *
     * @return amount of endpoints per service.
     */
    int viewEndpoints();

    /**
     * The number of query service endpoints.
     *
     * @return amount of endpoints per service.
     */
    int queryEndpoints();

    /**
     * The number of search service endpoints.
     *
     * @return amount of endpoints per service.
     */
    int searchEndpoints();

    /**
     * Returns version information on the core. Version number is in the form
     * MAJOR.MINOR.PATCH, and is the one for the core-io layer.
     *
     * @return the version string for the core.
     * @see #coreBuild() for a more specific build information (relevant for tracking
     * the exact version of the code the core was built from)
     */
    String coreVersion();

    /**
     * Returns build information on the Couchbase Java SDK core. This has a better
     * granularity than {@link #coreVersion()} and thus is more relevant to track
     * the exact version of the code the core was built from.
     *
     * Build information can contain VCS information like commit numbers, tags, etc...
     *
     * @return the build string for the core.
     * @see #coreVersion() for more generic version information.
     */
    String coreBuild();

    /**
     * Library identification string, which can be used as User-Agent header in HTTP requests.
     *
     * @return identification string
     */
    String userAgent();

    /**
     * Returns name and the version of the package. This method used to by @{link userAgent()}.
     *
     * @return string containing package name and version
     */
    String packageNameAndVersion();

    /**
     * The retry strategy on how to dispatch requests in the failure case.
     *
     * @return the retry strategy.
     */
    RetryStrategy retryStrategy();

    /**
     * Returns the maximum time in milliseconds a request is allowed to life.
     *
     * If the best effort retry strategy is used, the request will still be cancelled after this
     * period to make sure that requests are not sticking around forever. Make sure it is longer than any
     * timeout you potentially have configured.
     *
     * @return the maximum request lifetime.
     */
    long maxRequestLifetime();

    /**
     * The time in milliseconds after which a non-subscribed observable is going to be automatically released.
     *
     * This prevents accidentally leaking buffers when requested but not consumed by the user.
     *
     * @return the time after which the buffers are released if not subscribed.
     */
    long autoreleaseAfter();

    /**
     * The time in milliseconds after which some service will issue a form of keep-alive request.
     *
     * @return the interval of idle time in milliseconds after which a keep-alive is triggered.
     */
    long keepAliveInterval();

    /**
     * If set to true, KeepAlives will be sent on a regular basis in the interval also if there
     * is traffic on the socket,  not only if its idle.
     *
     * @return true if enabled, false otherwise.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Uncommitted
    boolean continuousKeepAliveEnabled();

    /**
     * Specifies the number of times a KeepAlive message on a socket can fail before the connection
     * is recycled.
     *
     * @return the number of keepalive errors allowed to happen before the socket is reopened.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Uncommitted
    long keepAliveErrorThreshold();

    /**
     * Specifies the timeout of a keepalive operation on the socket in milliseconds.
     *
     * @return
     */
    @InterfaceAudience.Public
    @InterfaceStability.Uncommitted
    long keepAliveTimeout();

    /**
     * Returns the event bus where events are broadcasted on and can be published to.
     *
     * @return the configured event bus.
     */
    EventBus eventBus();

    /**
     * Returns if buffer pooling is enabled for greater GC efficiency.
     *
     * In general this is always set to true and should only be set to false if there are leaks reported
     * that are not fixable by correcting user level code.
     *
     * @return true if enabled, false otherwise.
     */
    boolean bufferPoolingEnabled();

    /**
     * Returns true if TCP_NODELAY is enabled (therefore Nagle'ing is disabled).
     *
     * @return true if enabled.
     */
    boolean tcpNodelayEnabled();

    /**
     * Returns true if extended mutation tokens are enabled.
     *
     * Note that while this may return true, the server also needs to support it (Couchbase Server
     * 4.0 and above). It will be negotiated during connection setup, but needs to be explicitly
     * enabled on the environment as well to take effect (since it has a 16 bytes overhead on
     * every mutation performed).
     *
     * @return true if enabled on the client side.
     */
    boolean mutationTokensEnabled();

    /**
     * Returns the collector responsible for aggregating and publishing runtime information like gc and memory.
     *
     * @return the collector.
     */
    MetricsCollector runtimeMetricsCollector();

    /**
     * Returns the collector responsible for aggregating and publishing network latency information.
     *
     * @return the collector.
     */
    NetworkLatencyMetricsCollector networkLatencyMetricsCollector();

    /**
     * Returns the amount of time the SDK will wait on the socket connect until an error is raised and handled.
     *
     * @return the socket connect timeout in milliseconds.
     */
    int socketConnectTimeout();

    /**
     * Returns true if the {@link Observable} callbacks are completed on the IO event loops.
     *
     * Note: this is an advanced option and must be used with care. It can be used to improve performance since it
     * removes additional scheduling overhead on the response path, but any blocking calls in the callbacks will
     * lead to more work on the event loops itself and eventually stall them.
     *
     * @return true if callbacks are scheduled on the IO event loops.
     */
    boolean callbacksOnIoPool();

    /**
     * Waiting strategy used by request {@link com.lmax.disruptor.EventProcessor}s to wait for data from
     * {@link com.lmax.disruptor.RingBuffer}
     *
     * @return waiting strategy
     */
    @InterfaceStability.Experimental
    @InterfaceAudience.Public
    WaitStrategyFactory requestBufferWaitStrategy();

    /**
     * Returns the proactive polling interval for cluster configurations.
     *
     * @return the polling interval in milliseconds.
     */
    @InterfaceStability.Committed
    @InterfaceAudience.Public
    long configPollInterval();


    /**
     * Returns the minimum polling interval allowed.
     *
     * @return the minimum polling interval.
     */
    @InterfaceStability.Committed
    @InterfaceAudience.Public
    long configPollFloorInterval();

    /**
     * True if X.509 client certificate authentication is enabled.
     */
    @InterfaceStability.Uncommitted
    @InterfaceAudience.Public
    boolean certAuthEnabled();

    /**
     * Returns the {@link CouchbaseCoreSendHook} if set, null otherwise.
     */
    @InterfaceStability.Experimental
    @InterfaceAudience.Public
    CouchbaseCoreSendHook couchbaseCoreSendHook();

    /**
     * Forces the KeyValue SASL Authentication method to PLAIN if set to true,
     * specifically allowing LDAP-based users to authenticate.
     *
     * @return true if plain should be forced, false otherwise (default).
     */
    boolean forceSaslPlain();

    /**
     * True if tracing is enabled on the environment.
     */
    boolean tracingEnabled();

}
