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
import com.couchbase.client.core.env.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
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
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.AsyncSubject;

import javax.net.ssl.SSLEngine;
import java.util.concurrent.TimeUnit;

/**
 * The default implementation of a {@link Endpoint}.
 *
 * This implementation provides all the common functionality across endpoints, including connection management and
 * writing data into the channel.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public abstract class AbstractEndpoint extends AbstractStateMachine<LifecycleState> implements Endpoint {

    /**
     * The logger to use for all endpoints.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Endpoint.class);

    /**
     * A shared logging handler for all endpoints.
     */
    private static final ChannelHandler LOGGING_HANDLER_INSTANCE = new LoggingHandler(LogLevel.TRACE);

    /**
     * Pre-created not connected exception for performance reasons.
     */
    private static final NotConnectedException NOT_CONNECTED_EXCEPTION = new NotConnectedException();

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

    private final RingBuffer<ResponseEvent> responseBuffer;

    private final Environment env;

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
    private volatile long reconnectAttempt;

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
     * {@link #AbstractEndpoint(String, String, String, int, Environment, RingBuffer)} constructor instead.
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
        final Environment environment, final RingBuffer<ResponseEvent> responseBuffer) {
        super(LifecycleState.DISCONNECTED);
        this.bucket = bucket;
        this.password = password;
        this.responseBuffer = responseBuffer;
        this.env = environment;
        if (environment.sslEnabled()) {
            this.sslEngineFactory = new SSLEngineFactory(environment);
        }

        bootstrap = new BootstrapAdapter(new Bootstrap()
            .remoteAddress(hostname, port)
            .group(environment.ioPool())
            .channel(NioSocketChannel.class)
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
                    pipeline.addLast(new GenericEndpointHandler(AbstractEndpoint.this, responseBuffer));
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
            return Observable.from(state());
        }

        final AsyncSubject<LifecycleState> observable = AsyncSubject.create();
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
                        LOGGER.debug("Connected to " + AbstractEndpoint.this.getClass().getSimpleName()
                            + " " + channel.remoteAddress());
                        transitionState(LifecycleState.CONNECTED);
                    } else {
                        long delay = reconnectDelay();
                        LOGGER.warn("Could not connect to endpoint, retrying with delay " + delay + "ms: "
                            + future.channel().remoteAddress(), future.cause());
                        transitionState(LifecycleState.CONNECTING);
                        future.channel().eventLoop().schedule(new Runnable() {
                            @Override
                            public void run() {
                                connect();
                            }
                        }, delay, TimeUnit.MILLISECONDS);
                    }
                }
                observable.onNext(state());
                observable.onCompleted();
            }
        });
        return observable;
    }

    @Override
    public Observable<LifecycleState> disconnect() {
        if (state() == LifecycleState.DISCONNECTED || state() == LifecycleState.DISCONNECTING) {
            return Observable.from(state());
        }

        if (state() == LifecycleState.CONNECTING) {
            transitionState(LifecycleState.DISCONNECTED);
            return Observable.from(state());
        }

        transitionState(LifecycleState.DISCONNECTING);
        final AsyncSubject<LifecycleState> observable = AsyncSubject.create();
        channel.disconnect().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    LOGGER.debug("Disconnected from " + AbstractEndpoint.this.getClass().getSimpleName() + " "
                        + channel.remoteAddress());
                } else {
                    LOGGER.warn("Received an error during disconnect.", future.cause());
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
                    channel.write(request, channel.voidPromise());
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
        if (state() == LifecycleState.CONNECTED || state() == LifecycleState.CONNECTING) {
            transitionState(LifecycleState.DISCONNECTED);
            connect();
        }
    }

    /**
     * Returns the reconnect retry delay in  milliseconds.
     *
     * For now just use linear backoff.
     *
     * @return the retry delay.
     */
    private long reconnectDelay() {
        return reconnectAttempt++;
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

    protected Environment environment() {
        return env;
    }

}
