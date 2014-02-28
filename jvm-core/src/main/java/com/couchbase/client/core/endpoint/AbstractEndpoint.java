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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.NotConnectedException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.event.Event;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link AbstractEndpoint} implements common functionality needed across all {@link Endpoint}s.
 *
 * This common functionality revolves mostly around connecting, disconnecting, state management, error handling and
 * logging. The actual endpoints should only implement the specific pipeline, keeping code duplication at a minimum.
 */
public abstract class AbstractEndpoint extends AbstractStateMachine<LifecycleState> implements Endpoint {

    /**
     * The logger to use for all endpoints.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Endpoint.class);

    /**
     * A shared logging handler for all endpoints.
     */
    private static final ChannelHandler LOGGING_HANDLER_INSTANCE = new DebugLoggingHandler(LogLevel.TRACE);

    /**
     * Precreated not connected exception for performance reasons.
     */
    private static final NotConnectedException NOT_CONNECTED_EXCEPTION = new NotConnectedException();

    /**
     * The couchbase environment.
     */
    private final Environment env;

    /**
     * The IO (netty) bootstrapper.
     */
    private final BootstrapAdapter bootstrap;

    /**
     * The underlying IO (netty) channel.
     */
    private volatile Channel channel;

    /**
     * The reconnect delay that increases with backoff.
     */
    private final AtomicLong reconnectDelay = new AtomicLong(0);

    /**
     * Preset the stack trace for the static exceptions.
     */
    static {
        NOT_CONNECTED_EXCEPTION.setStackTrace(new StackTraceElement[0]);
    }

    /**
     * Add custom endpoint handlers to the {@link ChannelPipeline}.
     *
     * @param pipeline the pipeline where to add handlers.
     */
    protected abstract void customEndpointHandlers(ChannelPipeline pipeline);

    /**
     * Returns the flush interval for this endpoint.
     *
     * @param env the environment to use.
     * @return the flush interval for this endpoint.
     */
    protected abstract long flushInterval(Environment env);

    /**
     * Create a new {@link Endpoint} with a custom bootstrap.
     *
     * This constructor normally only needs to be used during unit-testing scenarios where a
     * mocked bootstrap can be passed in. For regular extensions, use
     * {@link #AbstractEndpoint(InetSocketAddress, Environment)}.
     *
     * @param bootstrap the netty bootstrap.
     * @param env the environment to use.
     */
    protected AbstractEndpoint(final BootstrapAdapter bootstrap, final Environment env) {
        super(LifecycleState.DISCONNECTED, env);
        this.bootstrap = bootstrap;
        this.env = env;
    }

    /**
     * Create a new {@link Endpoint}.
     *
     * This also automatically sets up the IO bootstrap, but does not do the connection yet.
     *
     * @param address the address to connect.
     * @param env the environment to use.
     */
    protected AbstractEndpoint(final InetSocketAddress address, final Environment env) {
        super(LifecycleState.DISCONNECTED, env);
        this.env = env;

        final ChannelHandler endpointHandler = new GenericEndpointHandler(this, flushInterval(env));
        bootstrap = new BootstrapAdapter(new Bootstrap()
            .group(env.ioPool())
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();

                    if (LOGGER.isTraceEnabled()) {
                        pipeline.addLast(LOGGING_HANDLER_INSTANCE);
                    }
                    customEndpointHandlers(pipeline);
                    pipeline.addLast(endpointHandler);
                }
            })
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.TCP_NODELAY, false)
            .remoteAddress(address));
    }

    @Override
    public Promise<LifecycleState> connect() {
        if (state() != LifecycleState.DISCONNECTED) {
            return Promises.success(state()).get();
        }

        final Deferred<LifecycleState, Promise<LifecycleState>> deferred = Promises.defer(env.reactorEnv());
        transitionState(LifecycleState.CONNECTING);

        bootstrap.connect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (state() == LifecycleState.DISCONNECTING || state() == LifecycleState.DISCONNECTED) {
                    LOGGER.debug("Endpoint connect completed, but got instructed to disconnect in the meantime.");
                    transitionState(LifecycleState.DISCONNECTED);
                    channel = null;
                } else {
                    if (future.isSuccess()) {
                        channel = future.channel();
                        transitionState(LifecycleState.CONNECTED);
                        LOGGER.debug("Successfully connected to Endpoint " + channel.remoteAddress());
                    } else {
                        long delay = reconnectDelay();
                        LOGGER.warn("Could not connect to endpoint, retrying with delay " + delay + "ms: "
                            + future.channel().remoteAddress());
                        transitionState(LifecycleState.CONNECTING);
                        future.channel().eventLoop().schedule(new Runnable() {
                            @Override
                            public void run() {
                                connect();
                            }
                        }, delay, TimeUnit.MILLISECONDS);
                    }
                }
                deferred.accept(state());
            }
        });

        return deferred.compose();
    }

    @Override
    public Promise<LifecycleState> disconnect() {
        if (state() == LifecycleState.DISCONNECTED || state() == LifecycleState.DISCONNECTING) {
            return Promises.success(state()).get();
        }

        if (state() == LifecycleState.CONNECTING) {
            transitionState(LifecycleState.DISCONNECTED);
            return Promises.success(state()).get();
        }

        transitionState(LifecycleState.DISCONNECTING);
        final Deferred<LifecycleState, Promise<LifecycleState>> deferred = Promises.defer(env.reactorEnv());
        channel.disconnect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    LOGGER.debug("Successfully disconnected from Endpoint " + channel.remoteAddress());
                } else {
                    LOGGER.warn("Received an error during disconnect.", future.cause());
                }
                transitionState(LifecycleState.DISCONNECTED);
                deferred.accept(state());
                channel = null;
            }
        });
        return deferred.compose();
    }

    @Override
    public <R extends CouchbaseResponse> Promise<R> send(final CouchbaseRequest request) {
        final Deferred<R,Promise<R>> deferred = Promises.defer(env.reactorEnv());

        if (state() == LifecycleState.CONNECTED) {
            Event<CouchbaseRequest> event = Event.wrap(request);
            event.setReplyTo(deferred);
            channel.write(event).addListener(new GenericFutureListener<Future<Void>>() {
                @Override
                public void operationComplete(final Future<Void> future) throws Exception {
                    if (!future.isSuccess()) {
                        // TODO: use better exceptions (also precreated ones for perf)
                        deferred.accept(new Exception("Writing the event into the endpoint failed", future.cause()));
                    }
                }
            });
        } else {
            deferred.accept(NOT_CONNECTED_EXCEPTION);
        }

        return deferred.compose();
    }

    /**
     * Helper method that is called inside from the event loop to notify the upper endpoint of a disconnect.
     *
     * Subsequent reconnect attempts are triggered from here.
     */
    void notifyChannelInactive() {
        transitionState(LifecycleState.DISCONNECTED);
        connect();
    }

    /**
     * Returns the reconnect retry delay in Miliseconds.
     *
     * Currently, it uses linear backoff.
     *
     * @return the retry delay.
     */
    private long reconnectDelay() {
        return reconnectDelay.getAndIncrement();
    }

}
