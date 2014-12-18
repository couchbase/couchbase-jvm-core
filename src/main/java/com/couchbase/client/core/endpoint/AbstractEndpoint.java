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

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.ResponseHandler;
import com.couchbase.client.core.endpoint.kv.AuthenticationException;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.internal.SignalConfigReload;
import com.couchbase.client.core.message.internal.SignalFlush;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.NotConnectedException;
import com.lmax.disruptor.RingBuffer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import rx.Observable;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;
import javax.net.ssl.SSLEngine;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;

/**
 * The common parent implementation for all {@link Endpoint}s.
 *
 * This parent implementation provides common functionality that all {@link Endpoint}s need, most notably
 * bootstrapping, connecting and reconnecting.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public abstract class AbstractEndpoint extends AbstractStateMachine<LifecycleState> implements Endpoint {

    /**
     * The maximum reconnect delay in milliseconds, so it does not grow out of bounds.
     */
    public static final int MAX_RECONNECT_DELAY = 4096;

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Endpoint.class);

    /**
     * A shared logging handler for all endpoints.
     */
    private static final ChannelHandler LOGGING_HANDLER_INSTANCE = new LoggingHandler(LogLevel.TRACE);

    /**
     * Pre-created not connected exception for performance reasons.
     */
    private static final NotConnectedException NOT_CONNECTED_EXCEPTION = new NotConnectedException();

    /**
     * A static listener which logs failed writes.
     */
    private static final WriteLogListener WRITE_LOG_LISTENER = new WriteLogListener();

    /**
     * The netty bootstrap adapter.
     */
    private final BootstrapAdapter bootstrap;

    /**
     * The name of the couchbase bucket (needed for bucket-level endpoints).
     */
    private final String bucket;

    /**
     * The password of the couchbase bucket (needed for bucket-level endpoints).
     */
    private final String password;

    /**
     * The reference to the response buffer to publish response events.
     */
    private final RingBuffer<ResponseEvent> responseBuffer;

    /**
     * Reference to the overall {@link CoreEnvironment}.
     */
    private final CoreEnvironment env;

    /**
     * Factory which handles {@link SSLEngine} creation.
     */
    private SSLEngineFactory sslEngineFactory;

    /**
     * The underlying IO (netty) channel.
     */
    private volatile Channel channel;

    /**
     * True if there have been operations written, pending flush.
     */
    private volatile boolean hasWritten;

    /**
     * Number of reconnects already done.
     */
    private volatile long reconnectAttempt = 1;

    /**
     * Preset the stack trace for the static exceptions.
     */
    static {
        NOT_CONNECTED_EXCEPTION.setStackTrace(new StackTraceElement[0]);
    }

    /**
     * Constructor to which allows to pass in an artificial bootstrap adapter.
     *
     * This method should not be used outside of tests. Please use the
     * {@link #AbstractEndpoint(String, String, String, int, CoreEnvironment, RingBuffer)} constructor instead.
     *
     * @param bucket the name of the bucket.
     * @param password the password of the bucket.
     * @param adapter the bootstrap adapter.
     */
    protected AbstractEndpoint(final String bucket, final String password, final BootstrapAdapter adapter) {
        super(LifecycleState.DISCONNECTED);
        bootstrap = adapter;
        this.bucket = bucket;
        this.password = password;
        this.responseBuffer = null;
        this.env = null;
    }

    /**
     * Create a new {@link AbstractEndpoint}.
     *
     * @param hostname the hostname/ipaddr of the remote channel.
     * @param bucket the name of the bucket.
     * @param password the password of the bucket.
     * @param port the port of the remote channel.
     * @param environment the environment of the core.
     * @param responseBuffer the response buffer for passing responses up the stack.
     */
    protected AbstractEndpoint(final String hostname, final String bucket, final String password, final int port,
        final CoreEnvironment environment, final RingBuffer<ResponseEvent> responseBuffer) {
        super(LifecycleState.DISCONNECTED);
        this.bucket = bucket;
        this.password = password;
        this.responseBuffer = responseBuffer;
        this.env = environment;
        if (environment.sslEnabled()) {
            this.sslEngineFactory = new SSLEngineFactory(environment);
        }

        Class<? extends Channel> channelClass = NioSocketChannel.class;
        if (environment.ioPool() instanceof EpollEventLoopGroup) {
            channelClass = EpollSocketChannel.class;
        } else if (environment.ioPool() instanceof OioEventLoopGroup) {
            channelClass = OioSocketChannel.class;
        }
        bootstrap = new BootstrapAdapter(new Bootstrap()
            .remoteAddress(hostname, port)
            .group(environment.ioPool())
            .channel(channelClass)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.TCP_NODELAY, false)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel channel) throws Exception {
                    ChannelPipeline pipeline = channel.pipeline();
                    if (environment.sslEnabled()) {
                        pipeline.addLast(new SslHandler(sslEngineFactory.get()));
                    }
                    if (LOGGER.isTraceEnabled()) {
                        pipeline.addLast(LOGGING_HANDLER_INSTANCE);
                    }
                    customEndpointHandlers(pipeline);
                }
            }));
    }

    /**
     * Add custom endpoint handlers to the {@link ChannelPipeline}.
     *
     * This method needs to be implemented by the actual endpoint implementations to add specific handlers to
     * the pipeline depending on the endpoint type and intended behavior.
     *
     * @param pipeline the pipeline where to add handlers.
     */
    protected abstract void customEndpointHandlers(ChannelPipeline pipeline);

    @Override
    public Observable<LifecycleState> connect() {
        if (state() != LifecycleState.DISCONNECTED) {
            return Observable.just(state());
        }

        final AsyncSubject<LifecycleState> observable = AsyncSubject.create();
        transitionState(LifecycleState.CONNECTING);
        doConnect(observable);
        return observable;
    }

    /**
     * Helper method to perform the actual connect and reconnect.
     *
     * @param observable the {@link Subject} which is eventually notified if the connect process
     *                   succeeded or failed.
     */
    protected void doConnect(final Subject<LifecycleState, LifecycleState> observable) {
        bootstrap.connect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (state() == LifecycleState.DISCONNECTING || state() == LifecycleState.DISCONNECTED) {
                    LOGGER.debug(logIdent(channel, AbstractEndpoint.this) + "Endpoint connect completed, "
                        + "but got instructed to disconnect in the meantime.");
                    transitionState(LifecycleState.DISCONNECTED);
                    channel = null;
                } else {
                    if (future.isSuccess()) {
                        channel = future.channel();
                        LOGGER.debug(logIdent(channel, AbstractEndpoint.this) + "Connected Endpoint.");
                        transitionState(LifecycleState.CONNECTED);
                    } else {
                        if (future.cause() instanceof AuthenticationException) {
                            LOGGER.warn(logIdent(channel, AbstractEndpoint.this)
                                + "Authentication Failure.");
                            transitionState(LifecycleState.DISCONNECTED);
                            observable.onError(future.cause());
                        } else if (future.cause() instanceof ClosedChannelException) {
                            LOGGER.warn(logIdent(channel, AbstractEndpoint.this)
                                + "Generic Failure.");
                            transitionState(LifecycleState.DISCONNECTED);
                            LOGGER.warn(future.cause().getMessage());
                            observable.onError(future.cause());
                        } else {
                            long delay = reconnectDelay();
                            LOGGER.warn(logIdent(channel, AbstractEndpoint.this)
                                    + "Could not connect to endpoint, retrying with delay " + delay + "ms: ",
                                future.cause());
                            transitionState(LifecycleState.CONNECTING);
                            future.channel().eventLoop().schedule(new Runnable() {
                                @Override
                                public void run() {
                                    doConnect(observable);
                                }
                            }, delay, TimeUnit.MILLISECONDS);
                        }
                    }
                }
                observable.onNext(state());
                observable.onCompleted();
            }
        });
    }


    @Override
    public Observable<LifecycleState> disconnect() {
        if (state() == LifecycleState.DISCONNECTED || state() == LifecycleState.DISCONNECTING) {
            return Observable.just(state());
        }

        if (state() == LifecycleState.CONNECTING) {
            transitionState(LifecycleState.DISCONNECTED);
            return Observable.just(state());
        }

        transitionState(LifecycleState.DISCONNECTING);
        final AsyncSubject<LifecycleState> observable = AsyncSubject.create();
        channel.disconnect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    LOGGER.debug(logIdent(channel, AbstractEndpoint.this) + "Disconnected Endpoint.");
                } else {
                    LOGGER.warn(logIdent(channel, AbstractEndpoint.this) + "Received an error "
                        + "during disconnect.", future.cause());
                }
                transitionState(LifecycleState.DISCONNECTED);
                observable.onNext(state());
                observable.onCompleted();
                channel = null;
            }
        });
        return observable;
    }

    @Override
    public void send(final CouchbaseRequest request) {
        if (state() == LifecycleState.CONNECTED) {
            if (request instanceof SignalFlush) {
                if (hasWritten) {
                    channel.flush();
                    hasWritten = false;
                }
            } else {
                if (channel.isWritable()) {
                    channel.write(request).addListener(WRITE_LOG_LISTENER);
                    hasWritten = true;
                } else {
                    responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, request, request.observable());
                }
            }
        } else {
            if (request instanceof SignalFlush) {
                return;
            }
            request.observable().onError(NOT_CONNECTED_EXCEPTION);
        }
    }

    /**
     * Helper method that is called from inside the event loop to notify the upper {@link Endpoint} of a disconnect.
     *
     * Note that the connect method is only called if the endpoint is currently connected, since otherwise this would
     * try to connect to a socket which has already been removed on a failover/rebalance out.
     *
     * Subsequent reconnect attempts are triggered from here.
     */
    public void notifyChannelInactive() {
        LOGGER.debug(logIdent(channel, this) + "Got notified from Channel as inactive.");

        responseBuffer.publishEvent(ResponseHandler.RESPONSE_TRANSLATOR, SignalConfigReload.INSTANCE, null);
        if (state() == LifecycleState.CONNECTED || state() == LifecycleState.CONNECTING) {
            transitionState(LifecycleState.DISCONNECTED);
            connect();
        }
    }

    /**
     * Returns the reconnect retry delay in  milliseconds.
     *
     * It uses an exponential back-off algorithm (2^attempt) until a fixed
     * ceiling is reached ({@link #MAX_RECONNECT_DELAY}).
     *
     * @return the retry delay.
     */
    private long reconnectDelay() {
        int delay = 1 << (reconnectAttempt++);
        return delay >= MAX_RECONNECT_DELAY ? MAX_RECONNECT_DELAY : delay;
    }

    /**
     * The name of the bucket.
     *
     * @return the bucket name.
     */
    protected String bucket() {
        return bucket;
    }

    /**
     * The password of the bucket.
     *
     * @return the bucket password.
     */
    protected String password() {
        return password;
    }

    /**
     * The {@link CoreEnvironment} reference.
     *
     * @return the environment.
     */
    public CoreEnvironment environment() {
        return env;
    }

    /**
     * The {@link RingBuffer} response buffer reference.
     *
     * @return the response buffer.
     */
    public RingBuffer<ResponseEvent> responseBuffer() {
        return responseBuffer;
    }

    /**
     * Simple log helper to give logs a common prefix.
     *
     * @param chan the address.
     * @param endpoint the endpoint.
     * @return a prefix string for logs.
     */
    protected static String logIdent(final Channel chan, final Endpoint endpoint) {
        SocketAddress addr = chan != null ? chan.remoteAddress() : null;
        return "[" + addr + "][" + endpoint.getClass().getSimpleName() + "]: ";
    }

    /**
     * A generic future listener which logs unsuccessful writes.
     *
     * Note that {@link ClosedChannelException}s are ignored because they are handled
     * gracefully by the {@link AbstractGenericHandler}.
     */
    static class WriteLogListener implements GenericFutureListener<Future<Void>> {

        @Override
        public void operationComplete(Future<Void> future) throws Exception {
            if (!future.isSuccess() && !(future.cause() instanceof ClosedChannelException)) {
                LOGGER.warn("Error during IO write phase.", future.cause());
            }
        }

    }

}
