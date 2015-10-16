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

import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.message.observe.Observe;
import com.couchbase.client.core.metrics.MetricsCollector;
import com.couchbase.client.core.metrics.NetworkLatencyMetricsCollector;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.time.Delay;
import io.netty.channel.EventLoopGroup;
import rx.Observable;
import rx.Scheduler;

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
public interface CoreEnvironment {

    /**
     * Shutdown the {@link CoreEnvironment}.
     *
     * @deprecated This method will be changed in 2.3.0 into a synchronous version. Please migrate
     * to {@link #shutdownAsync()} right now to avoid breaking your code.
     *
     * @return an {@link Observable} eventually returning a boolean, indicating the success of the shutdown.
     */
    @Deprecated
    Observable<Boolean> shutdown();

    /**
     * Shutdown the {@link CoreEnvironment} in an asynchronous fashion.
     *
     * Since this method is asynchronous and cold, it is important to subscribe to the observable to actually
     * initiate the shutdown process.
     *
     * @return an {@link Observable} eventually returning a boolean, indicating the success of the shutdown.
     */
    Observable<Boolean> shutdownAsync();

    /**
     * Returns the IO pool for the underlying IO framework.
     *
     * @return the IO pool, shared across resources.
     */
    EventLoopGroup ioPool();

    /**
     * Returns the scheduler which should be used for all core actions that need to happen
     * asynchronously.
     *
     * @return the scheduler used for internal operations.
     */
    Scheduler scheduler();

    /**
     * Identifies if DCP should be enabled.
     *
     * @return true if DCP is enabled, false otherwise.
     */
    boolean dcpEnabled();

    /**
     * Identifies if SSL should be enabled.
     *
     * @return true if SSL is enabled, false otherwise.
     */
    boolean sslEnabled();

    /**
     * Identifies the filepath to the ssl keystore.
     *
     * @return the path to the keystore file.
     */
    String sslKeystoreFile();

    /**
     * The password which is used to protect the keystore.
     *
     * @return the keystore password.
     */
    String sslKeystorePassword();

    /**
     * True if N1QL querying should be enabled manually, deprecated.
     *
     * With Couchbase Server 4.0 and onward, it will be automatically detected.
     *
     * @return true if manual N1QL querying is enabled.
     * @deprecated
     */
    @Deprecated
    boolean queryEnabled();

    /**
     * If manual querying enabled, this defines the N1QL port to use, deprecated.
     *
     * With Couchbase Server 4.0 and onward, it will be automatically detected.
     *
     * @return the query port.
     * @deprecated
     */
    @Deprecated
    int queryPort();

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

}
